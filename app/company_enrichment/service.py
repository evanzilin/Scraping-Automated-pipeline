"""Batch orchestration: DB + cache + Scrapingdog + compare (fail per record)."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from app.company_enrichment.cache_service import (
    CacheProtocol,
    build_cache_payload,
    cache_key_for_domain,
    decode_cache_entry,
)
from app.company_enrichment.comparator import (
    build_final_resolved,
    compare_claimed_to_reference,
    infer_source_used,
    infer_status,
)
from app.company_enrichment.models import (
    CompanyEnrichmentResult,
    EnrichedFields,
    HQLocation,
    SchemaMapping,
    StoredCompanyMetadata,
    TargetRecord,
    stored_from_company_row,
    target_record_from_row,
)
from app.company_enrichment.normalizers import extract_domain, normalize_domain
from app.company_enrichment.repository import CompanyEnrichmentRepository
from app.company_enrichment.scrapingdog_client import ScrapingdogEnrichmentClient, enrich_domain
from app.company_enrichment.types import EnrichmentStatus

logger = logging.getLogger(__name__)


def _empty_enriched_dict() -> Dict[str, Any]:
    return EnrichedFields().to_dict()


class CompanyEnrichmentService:
    __slots__ = ("repo", "cache", "schema", "sd_client", "cache_ttl", "try_dynamic_scrape")

    def __init__(
        self,
        repo: CompanyEnrichmentRepository,
        cache: CacheProtocol,
        sd_client: ScrapingdogEnrichmentClient,
        *,
        schema: Optional[SchemaMapping] = None,
        cache_ttl_seconds: int = 86_400,
        try_dynamic_scrape: bool = False,
    ) -> None:
        self.repo = repo
        self.cache = cache
        self.schema = schema or SchemaMapping()
        self.sd_client = sd_client
        self.cache_ttl = cache_ttl_seconds
        self.try_dynamic_scrape = try_dynamic_scrape

    async def process_batch(self, limit: int, offset: int = 0) -> Tuple[Any, List[CompanyEnrichmentResult]]:
        from app.company_enrichment.models import BatchSummary

        summary = BatchSummary()
        results: List[CompanyEnrichmentResult] = []

        try:
            rows = await self.repo.fetch_target_records(limit, offset)
        except Exception as e:
            logger.exception("fetch_target_records failed: %s", e)
            return summary, results

        summary.total_records = len(rows)
        if not rows:
            return summary, results

        targets: List[TargetRecord] = [target_record_from_row(r, self.schema) for r in rows]

        company_ids = [t.company_id for t in targets if t.company_id]
        domains_raw = []
        for t in targets:
            d = normalize_domain(t.email_domain) or normalize_domain(extract_domain(t.email or ""))
            if d:
                domains_raw.append(d)

        by_company: Dict[str, Dict[str, Any]] = {}
        by_domain: Dict[str, Dict[str, Any]] = {}
        try:
            if company_ids:
                by_company = await self.repo.fetch_existing_company_metadata_by_ids(company_ids)
            if domains_raw:
                by_domain = await self.repo.fetch_existing_company_metadata_by_domains(domains_raw)
        except Exception as e:
            logger.exception("batch metadata fetch failed: %s", e)

        unique_domains = list(dict.fromkeys(domains_raw))
        cache_keys = [cache_key_for_domain(d) for d in unique_domains]
        cached_raw: List[Optional[bytes]] = []
        try:
            cached_raw = await self.cache.mget(cache_keys)
        except Exception as e:
            logger.warning("cache mget failed: %s", e)
            cached_raw = [None] * len(cache_keys)

        cache_by_domain: Dict[str, Dict[str, Any]] = {}
        for d, key, raw in zip(unique_domains, cache_keys, cached_raw):
            dec = decode_cache_entry(raw)
            if dec:
                cache_by_domain[d] = dec

        summary.cache_hits = len(cache_by_domain)
        summary.db_hits = len(by_company) + len(by_domain)

        # Domains that need Scrapingdog: no website in stored/cache final, seed from email domain
        need_scrape: Set[str] = set()
        domain_to_enriched: Dict[str, EnrichedFields] = {}

        def stored_for_target(t: TargetRecord) -> Optional[StoredCompanyMetadata]:
            if t.company_id and t.company_id in by_company:
                return stored_from_company_row(by_company[t.company_id], self.schema)
            d = normalize_domain(t.email_domain) or normalize_domain(extract_domain(t.email or ""))
            if d and d in by_domain:
                return stored_from_company_row(by_domain[d], self.schema)
            return None

        for t in targets:
            d = normalize_domain(t.email_domain) or normalize_domain(extract_domain(t.email or ""))
            if not d:
                continue
            st = stored_for_target(t)
            cached = cache_by_domain.get(d)
            website_ok = bool(
                (t.claimed.company_website_url or "").strip()
                or (st and (st.company_website_url or "").strip())
                or (
                    cached
                    and (cached.get("final_resolved") or {}).get("company_website_url")
                )
            )
            if website_ok:
                continue
            if d in cache_by_domain:
                # cache hit but no website — still try scrape once per batch if not deduped
                ent = cache_by_domain[d].get("enriched") or {}
                if not (ent.get("company_website_url") or "").strip():
                    need_scrape.add(d)
            else:
                need_scrape.add(d)

        scrape_list = [d for d in unique_domains if d in need_scrape]

        async def scrape_one(dom: str) -> Tuple[str, EnrichedFields]:
            try:
                ef = await enrich_domain(self.sd_client, dom, try_dynamic=self.try_dynamic_scrape)
            except Exception as e:
                logger.warning("scrape_one failed domain=%s err=%s", dom, e)
                ef = EnrichedFields(raw_error=str(e))
            return dom, ef

        scrape_tasks = [scrape_one(d) for d in scrape_list]
        if scrape_tasks:
            summary.scrapingdog_calls = len(scrape_tasks)
            scraped = await asyncio.gather(*scrape_tasks, return_exceptions=False)
            for dom, ef in scraped:
                domain_to_enriched[dom] = ef

        to_cache: Dict[str, Dict[str, Any]] = {}
        for dom, ef in domain_to_enriched.items():
            st_meta: Optional[StoredCompanyMetadata] = None
            cl = None
            for t in targets:
                td = normalize_domain(t.email_domain) or normalize_domain(extract_domain(t.email or ""))
                if td == dom:
                    st_meta = stored_for_target(t)
                    cl = t.claimed
                    break
            if cl is None:
                continue
            fr, _ = build_final_resolved(cl, st_meta, ef)
            to_cache[cache_key_for_domain(dom)] = build_cache_payload(
                ef.to_dict(),
                fr,
                EnrichmentStatus.ENRICHED.value if ef.any_value() else EnrichmentStatus.INSUFFICIENT_DATA.value,
            )
        if to_cache:
            try:
                await self.cache.mset_json(to_cache, self.cache_ttl)
            except Exception as e:
                logger.warning("cache mset failed: %s", e)
            for dom in domain_to_enriched:
                k = cache_key_for_domain(dom)
                if k in to_cache:
                    cache_by_domain[dom] = to_cache[k]

        # Per record
        for t in targets:
            t0 = time.perf_counter()
            try:
                res = self._process_one_record(
                    t,
                    stored_for_target(t),
                    cache_by_domain,
                    domain_to_enriched,
                )
            except Exception as e:
                logger.exception("record failed lead=%s: %s", t.lead_id, e)
                res = CompanyEnrichmentResult(
                    company_id=t.company_id,
                    lead_id=t.lead_id,
                    email_domain=normalize_domain(t.email_domain) or "",
                    claimed=t.claimed.to_dict(),
                    stored={},
                    enriched=_empty_enriched_dict(),
                    final_resolved_fields={},
                    matched_fields=[],
                    mismatched_fields=[],
                    missing_fields=[],
                    status=EnrichmentStatus.FAILED.value,
                    confidence_notes=[str(e)],
                    processing_time_ms=(time.perf_counter() - t0) * 1000,
                    source_used="none",
                    error_detail=str(e),
                )
            res.processing_time_ms = (time.perf_counter() - t0) * 1000
            results.append(res)
            self._accumulate_summary(summary, res)

        return summary, results

    def _process_one_record(
        self,
        t: TargetRecord,
        stored: Optional[StoredCompanyMetadata],
        cache_by_domain: Dict[str, Dict[str, Any]],
        domain_to_enriched: Dict[str, EnrichedFields],
    ) -> CompanyEnrichmentResult:
        d = normalize_domain(t.email_domain) or normalize_domain(extract_domain(t.email or ""))
        from_cache = bool(d and d in cache_by_domain)
        enriched_model: Optional[EnrichedFields] = None
        if d and d in domain_to_enriched:
            enriched_model = domain_to_enriched[d]
        elif d and d in cache_by_domain:
            ed = cache_by_domain[d].get("enriched")
            if isinstance(ed, dict):
                enriched_model = EnrichedFields(
                    company_website_url=ed.get("company_website_url"),
                    company_linkedin_url=ed.get("company_linkedin_url"),
                    employee_count=ed.get("employee_count"),
                    industry=ed.get("industry"),
                    sub_industry=ed.get("sub_industry"),
                    hq=HQLocation.from_dict(ed.get("hq_location")),
                    raw_error=ed.get("raw_error"),
                )

        if enriched_model is None:
            enriched_model = EnrichedFields()

        matched, mismatched, missing, notes = compare_claimed_to_reference(
            t.claimed, stored, enriched_model if enriched_model.any_value() else None
        )
        final_resolved, from_enr = build_final_resolved(t.claimed, stored, enriched_model if enriched_model.any_value() else None)  # noqa: E501

        website_resolved = bool((final_resolved.get("company_website_url") or "").strip())
        used_scrape = d in domain_to_enriched and bool(domain_to_enriched[d].any_value())
        used_db = stored is not None
        source = infer_source_used(from_cache, used_db, used_scrape)

        status = infer_status(
            matched,
            mismatched,
            missing,
            used_enrichment=used_scrape or bool(from_enr),
            had_error=bool(enriched_model.raw_error) and not enriched_model.any_value() and not website_resolved,
            website_resolved=website_resolved,
        )

        return CompanyEnrichmentResult(
            company_id=t.company_id,
            lead_id=t.lead_id,
            email_domain=d,
            claimed=t.claimed.to_dict(),
            stored=stored.to_dict() if stored else {},
            enriched=enriched_model.to_dict(),
            final_resolved_fields=final_resolved,
            matched_fields=matched,
            mismatched_fields=mismatched,
            missing_fields=missing,
            status=status,
            confidence_notes=notes,
            processing_time_ms=0.0,
            source_used=source,
            error_detail=enriched_model.raw_error,
        )

    def _accumulate_summary(self, summary: Any, res: CompanyEnrichmentResult) -> None:
        s = res.status
        if s == EnrichmentStatus.MATCHED.value:
            summary.matched_count += 1
        elif s == EnrichmentStatus.PARTIALLY_MATCHED.value:
            summary.partially_matched_count += 1
        elif s == EnrichmentStatus.ENRICHED.value:
            summary.enriched_count += 1
        elif s == EnrichmentStatus.INSUFFICIENT_DATA.value:
            summary.insufficient_data_count += 1
        else:
            summary.failed_count += 1
        summary.total_processing_time_ms += res.processing_time_ms
