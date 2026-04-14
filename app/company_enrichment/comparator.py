"""Compare claimed vs stored vs enriched profiles (normalized)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from app.company_enrichment.models import ClaimedFields, EnrichedFields, StoredCompanyMetadata
from app.company_enrichment.normalizers import (
    normalize_city,
    normalize_country,
    normalize_employee_count,
    normalize_linkedin_company_url,
    normalize_state,
    normalize_text,
    website_domains_equal,
)

# Fields we compare when claimed has a value
_FIELD_KEYS = (
    "company_website_url",
    "company_linkedin_url",
    "employee_count",
    "industry",
    "sub_industry",
    "hq_country",
    "hq_state",
    "hq_city",
)


def _stored_as_dict(s: Optional[StoredCompanyMetadata]) -> Dict[str, Any]:
    if not s:
        return {}
    cn = normalize_country(s.hq.country)
    return {
        "company_website_url": s.company_website_url,
        "company_linkedin_url": s.company_linkedin_url,
        "employee_count": s.employee_count,
        "industry": s.industry,
        "sub_industry": s.sub_industry,
        "hq_country": s.hq.country,
        "hq_state": s.hq.state if cn == "us" else None,
        "hq_city": s.hq.city,
    }


def _enriched_as_dict(e: Optional[EnrichedFields]) -> Dict[str, Any]:
    if not e:
        return {}
    cn = normalize_country(e.hq.country)
    return {
        "company_website_url": e.company_website_url,
        "company_linkedin_url": e.company_linkedin_url,
        "employee_count": e.employee_count,
        "industry": e.industry,
        "sub_industry": e.sub_industry,
        "hq_country": e.hq.country,
        "hq_state": e.hq.state if cn == "us" else None,
        "hq_city": e.hq.city,
    }


def _claimed_as_dict(c: ClaimedFields) -> Dict[str, Any]:
    cn = normalize_country(c.hq.country)
    return {
        "company_website_url": c.company_website_url,
        "company_linkedin_url": c.company_linkedin_url,
        "employee_count": c.employee_count,
        "industry": c.industry,
        "sub_industry": c.sub_industry,
        "hq_country": c.hq.country,
        "hq_state": c.hq.state if cn == "us" else None,
        "hq_city": c.hq.city,
    }


def _values_equal(field: str, a: Any, b: Any) -> bool:
    if a is None and b is None:
        return True
    if field == "company_website_url":
        return website_domains_equal(
            str(a) if a else None,
            str(b) if b else None,
        )
    if field == "company_linkedin_url":
        x = normalize_linkedin_company_url(str(a) if a else "")
        y = normalize_linkedin_company_url(str(b) if b else "")
        if not x and not y:
            return True
        return x == y and bool(x)
    if field == "employee_count":
        return normalize_employee_count(str(a) if a else "") == normalize_employee_count(
            str(b) if b else ""
        )
    if field in ("industry", "sub_industry"):
        return normalize_text(str(a) if a else "") == normalize_text(str(b) if b else "")
    if field == "hq_country":
        return normalize_country(str(a) if a else "") == normalize_country(str(b) if b else "")
    if field == "hq_state":
        ca = normalize_country("")  # need country context — handled by caller passing resolved country
        return normalize_state(str(a) if a else "", ca or "us") == normalize_state(
            str(b) if b else "", ca or "us"
        )
    if field == "hq_city":
        return normalize_city(str(a) if a else "") == normalize_city(str(b) if b else "")
    return str(a).strip().lower() == str(b).strip().lower()


def values_equal_with_country(
    field: str,
    a: Any,
    b: Any,
    *,
    country_for_state: str,
) -> bool:
    if field != "hq_state":
        if field == "hq_country":
            return normalize_country(str(a) if a else "") == normalize_country(str(b) if b else "")
        return _values_equal(field, a, b)
    ca = normalize_country(country_for_state)
    sa = normalize_state(str(a) if a else "", ca)
    sb = normalize_state(str(b) if b else "", ca)
    if ca != "us":
        return sa is None and sb is None
    return sa == sb


def compare_claimed_to_reference(
    claimed: ClaimedFields,
    stored: Optional[StoredCompanyMetadata],
    enriched: Optional[EnrichedFields],
) -> Tuple[List[str], List[str], List[str], List[str]]:
    """
    Returns (matched_fields, mismatched_fields, missing_fields, confidence_notes).
    """
    cd = _claimed_as_dict(claimed)
    sd = _stored_as_dict(stored)
    ed = _enriched_as_dict(enriched)

    matched: List[str] = []
    mismatched: List[str] = []
    missing: List[str] = []
    notes: List[str] = []

    ref_country = normalize_country(
        cd.get("hq_country") or sd.get("hq_country") or ed.get("hq_country") or ""
    )

    for field in _FIELD_KEYS:
        cval = cd.get(field)
        if cval is None or str(cval).strip() == "":
            continue
        sval = sd.get(field)
        eval_ = ed.get(field)
        ref_val: Any = None
        if sval is not None and str(sval).strip() != "":
            ref_val = sval
        elif eval_ is not None and str(eval_).strip() != "":
            ref_val = eval_

        if ref_val is None:
            missing.append(field)
            notes.append(f"{field}: no stored or enriched value to verify")
            continue
        if values_equal_with_country(field, cval, ref_val, country_for_state=ref_country or "us"):
            matched.append(field)
        else:
            mismatched.append(field)
            notes.append(f"{field}: claimed differs from reference")

    return matched, mismatched, missing, notes


def build_final_resolved(
    claimed: ClaimedFields,
    stored: Optional[StoredCompanyMetadata],
    enriched: Optional[EnrichedFields],
) -> Tuple[Dict[str, Any], Set[str]]:
    """
    Merge: prefer claimed, then stored, then enriched.
    Returns (flat dict, set of field keys that came from enrichment).
    """
    from_enriched: Set[str] = set()

    def pick(c: Any, s: Any, e: Any, key: str) -> Any:
        if c is not None and str(c).strip() != "":
            return c
        if s is not None and str(s).strip() != "":
            return s
        if e is not None and str(e).strip() != "":
            from_enriched.add(key)
            return e
        return None

    cn = normalize_country(claimed.hq.country or (stored.hq.country if stored else None) or (enriched.hq.country if enriched else None) or "")  # noqa: E501

    w = pick(
        claimed.company_website_url,
        stored.company_website_url if stored else None,
        enriched.company_website_url if enriched else None,
        "company_website_url",
    )
    li = pick(
        claimed.company_linkedin_url,
        stored.company_linkedin_url if stored else None,
        enriched.company_linkedin_url if enriched else None,
        "company_linkedin_url",
    )
    ec = pick(
        claimed.employee_count,
        stored.employee_count if stored else None,
        enriched.employee_count if enriched else None,
        "employee_count",
    )
    ind = pick(
        claimed.industry,
        stored.industry if stored else None,
        enriched.industry if enriched else None,
        "industry",
    )
    sub = pick(
        claimed.sub_industry,
        stored.sub_industry if stored else None,
        enriched.sub_industry if enriched else None,
        "sub_industry",
    )

    hc = pick(claimed.hq.country, stored.hq.country if stored else None, enriched.hq.country if enriched else None, "hq_country")  # noqa: E501
    hs = pick(claimed.hq.state, stored.hq.state if stored else None, enriched.hq.state if enriched else None, "hq_state")  # noqa: E501
    if normalize_country(hc or "") != "us":
        hs = None
    city = pick(claimed.hq.city, stored.hq.city if stored else None, enriched.hq.city if enriched else None, "hq_city")  # noqa: E501

    out = {
        "company_website_url": w,
        "company_linkedin_url": li,
        "employee_count": ec,
        "industry": ind,
        "sub_industry": sub,
        "hq_location": {
            "country": hc,
            "state": hs,
            "city": city,
        },
    }
    return out, from_enriched


def infer_status(
    matched: List[str],
    mismatched: List[str],
    missing: List[str],
    used_enrichment: bool,
    had_error: bool,
    website_resolved: bool,
) -> str:
    from app.company_enrichment.types import EnrichmentStatus

    if had_error:
        return EnrichmentStatus.FAILED.value
    if not website_resolved and not used_enrichment:
        return EnrichmentStatus.INSUFFICIENT_DATA.value
    if used_enrichment:
        if not mismatched and not missing:
            return EnrichmentStatus.ENRICHED.value
        return EnrichmentStatus.PARTIALLY_MATCHED.value
    if mismatched:
        return EnrichmentStatus.PARTIALLY_MATCHED.value
    if missing and matched:
        return EnrichmentStatus.PARTIALLY_MATCHED.value
    if matched and not mismatched and not missing:
        return EnrichmentStatus.MATCHED.value
    if missing and not matched:
        return EnrichmentStatus.INSUFFICIENT_DATA.value
    return EnrichmentStatus.PARTIALLY_MATCHED.value


def infer_source_used(from_cache: bool, from_db: bool, from_scrape: bool) -> str:
    n = sum(1 for x in (from_cache, from_db, from_scrape) if x)
    if n > 1:
        return "mixed"
    if from_cache:
        return "cache"
    if from_scrape:
        return "scrapingdog"
    if from_db:
        return "db"
    return "none"
