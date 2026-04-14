"""
Orchestrates ES-first search → Postgres source of truth → optional Scrapingdog → persist → index.
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Dict, Optional

from company_profile.company_contract import CompanyProfile
from company_profile.elasticsearch_repository import ElasticsearchCompanyRepository
from company_profile.normalizer import apply_us_state_rule, normalize_domain, normalize_profile
from company_profile.postgres_repository import PostgresCompanyRepository
from company_profile.scrapingdog_client import scrape_domain
from company_profile.sufficiency import is_sufficient
from scrape_api_key import get_scrape_api_key

logger = logging.getLogger(__name__)


def _merge_pg_and_scrape(pg_row: Optional[Dict[str, Any]], scraped: Dict[str, Any], domain: str) -> CompanyProfile:
    """Prefer existing Postgres values; fill gaps from scrape."""
    base = CompanyProfile.from_dict(pg_row) if pg_row else CompanyProfile(company_domain=domain)
    scr = CompanyProfile.from_dict({**{"company_domain": domain}, **scraped})

    def pick(a: Optional[str], b: Optional[str]) -> Optional[str]:
        if a and str(a).strip():
            return a
        if b and str(b).strip():
            return b
        return a or b

    merged = CompanyProfile(
        id=base.id or scr.id or domain,
        company_domain=domain,
        company_website_url=pick(base.company_website_url, scr.company_website_url),
        employee_count=pick(base.employee_count, scr.employee_count),
        industry=pick(base.industry, scr.industry),
        sub_industry=pick(base.sub_industry, scr.sub_industry),
        company_linkedin_url=pick(base.company_linkedin_url, scr.company_linkedin_url),
        hq_location_country=pick(base.hq_location_country, scr.hq_location_country),
        hq_location_state=pick(base.hq_location_state, scr.hq_location_state),
        hq_location_city=pick(base.hq_location_city, scr.hq_location_city),
        source_url=scr.source_url or base.source_url,
        source_type=scr.source_type or base.source_type,
    )
    return merged


class CompanyEnrichmentService:
    """
    Wire Elasticsearch → Postgres → Scrapingdog.

    If you do not pass repositories, they are built from environment variables.
    """

    def __init__(
        self,
        *,
        es: Optional[ElasticsearchCompanyRepository] = None,
        pg: Optional[PostgresCompanyRepository] = None,
        scrapingdog_api_key: Optional[str] = None,
    ) -> None:
        self._es = es or ElasticsearchCompanyRepository.from_env()
        self._pg = pg or PostgresCompanyRepository.from_env()
        self._api_key = (scrapingdog_api_key or get_scrape_api_key() or "").strip()

    def get_company_profile(self, company_domain: str) -> Dict[str, Any]:
        """
        Main entry:

        1. Normalize domain.
        2. Search Elasticsearch by domain → get linked ``id``.
        3. If hit, load Postgres row by ``id``; if sufficient, return normalized profile.
        4. Else scrape with Scrapingdog (also when ES misses or row missing / insufficient).
        5. Merge PG + scrape, normalize, upsert Postgres, index Elasticsearch, return dict.
        """
        domain = normalize_domain(company_domain)
        if not domain:
            logger.warning("Empty company_domain")
            return apply_us_state_rule(
                CompanyProfile(
                    company_domain="",
                    source_url="unknown",
                    source_type="unknown",
                ).to_dict_full()
            )

        pg_row: Optional[Dict[str, Any]] = None
        company_id: Optional[str] = None

        try:
            company_id = self._es.find_company_id_by_domain(domain)
        except Exception as e:
            logger.exception("Elasticsearch step failed (continuing to scrape path): %s", e)
            company_id = None

        if company_id:
            try:
                pg_row = self._pg.fetch_by_id(company_id)
            except Exception as e:
                logger.exception("Postgres fetch failed id=%s: %s", company_id, e)
                pg_row = None

            if pg_row and is_sufficient(pg_row):
                prof = normalize_profile(CompanyProfile.from_dict(pg_row))
                out = apply_us_state_rule(prof.to_dict_full())
                logger.info("Served from Postgres (ES hit) domain=%s", domain)
                return out

        if not self._api_key:
            logger.error("SCRAPE_API_KEY missing; cannot enrich domain=%s", domain)

        scraped = scrape_domain(domain, self._api_key) if self._api_key else {"company_domain": domain, "source_url": "unknown", "source_type": "unknown"}  # noqa: E501

        merged = _merge_pg_and_scrape(pg_row, scraped, domain)
        if not merged.id:
            merged.id = pg_row.get("id") if pg_row and pg_row.get("id") else str(uuid.uuid4())

        normalized = normalize_profile(merged)
        try:
            self._pg.upsert_company(normalized)
        except Exception as e:
            logger.exception("Postgres upsert failed domain=%s: %s", domain, e)
            # still return profile to caller
        try:
            doc = apply_us_state_rule(normalized.to_dict_full())
            self._es.index_company(doc, doc_id=str(normalized.id))
        except Exception as e:
            logger.exception("Elasticsearch index failed domain=%s: %s", domain, e)

        return apply_us_state_rule(normalized.to_dict_full())


def get_company_profile(company_domain: str) -> Dict[str, Any]:
    """
    Convenience one-liner using env-configured clients.

    Environment:
    - ``ELASTICSEARCH_URL``, ``ELASTICSEARCH_INDEX_COMPANY``
    - ``DATABASE_URL`` / ``POSTGRES_DSN``
    - ``SCRAPE_API_KEY`` (or legacy ``SCRAPINGDOG_API_KEY``)
    """
    return CompanyEnrichmentService().get_company_profile(company_domain)
