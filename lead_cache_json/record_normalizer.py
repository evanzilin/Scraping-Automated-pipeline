"""
Flatten cache payloads, normalize strings / URLs / countries / employee buckets, and build output rows.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Mapping, Optional
from urllib.parse import urlparse

from lead_cache_json.json_contract import (
    CACHE_FIELD_ALIASES,
    EMPLOYEE_COUNT_BUCKETS,
    K_BUSINESS,
    K_CITY,
    K_COMPANY_LINKEDIN,
    K_COUNTRY,
    K_DESCRIPTION,
    K_EMAIL,
    K_EMPLOYEE_COUNT,
    K_FIRST,
    K_FULL_NAME,
    K_HQ_CITY,
    K_HQ_COUNTRY,
    K_HQ_STATE,
    K_INDUSTRY,
    K_LAST,
    K_LINKEDIN,
    K_PHONE_NUMBERS,
    K_ROLE,
    K_SOCIALS,
    K_SOURCE_TYPE,
    K_SOURCE_URL,
    K_STATE,
    K_SUB_INDUSTRY,
    K_WEBSITE,
    SOCIALS_TWITTER_KEY,
    SOURCE_UNCLEAR_VALUE,
    US_COUNTRY_ALIASES,
    empty_output_record,
)

logger = logging.getLogger(__name__)

JsonRecord = Dict[str, Any]

# Numeric ranges → canonical bucket label (aligned with product spec)
_EMPLOYEE_RANGE_TO_LABEL: list[tuple[int, int, str]] = [
    (0, 1, "0-1"),
    (2, 10, "2-10"),
    (11, 50, "11-50"),
    (51, 200, "51-200"),
    (201, 500, "201-500"),
    (501, 1000, "501-1,000"),
    (1001, 5000, "1,001-5,000"),
    (5001, 10000, "5,001-10,000"),
    (10001, 10**9, "10,001+"),
]

_COUNTRY_ALIASES: Dict[str, str] = {
    "us": "United States",
    "usa": "United States",
    "u.s.": "United States",
    "u.s.a.": "United States",
    "united states": "United States",
    "united states of america": "United States",
    "america": "United States",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "great britain": "United Kingdom",
    "united kingdom": "United Kingdom",
    "uae": "United Arab Emirates",
    "united arab emirates": "United Arab Emirates",
}


def unwrap_cache_record(record: JsonRecord) -> JsonRecord:
    """
    Merge common cache wrapper shapes into one flat dict for alias resolution.

    Supports payloads like ``build_cache_payload`` (``enriched`` / ``final_resolved``).
    Later keys overwrite earlier ones.
    """
    if not isinstance(record, dict):
        return {}

    merged: JsonRecord = {}
    for key in ("enriched", "final_resolved", "data", "lead", "payload"):
        sub = record.get(key)
        if isinstance(sub, dict):
            merged.update(sub)
    merged.update(record)
    return merged


def get_alias(flat: Mapping[str, Any], logical: str) -> Any:
    """First non-empty value among ``CACHE_FIELD_ALIASES[logical]``."""
    keys = CACHE_FIELD_ALIASES.get(logical)
    if not keys:
        return None
    for k in keys:
        if k not in flat:
            continue
        v = flat[k]
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def trim_or_null(value: Any, *, required: bool = False) -> Optional[str]:
    """Strip strings; empty → None. Non-strings stringified then trimmed."""
    if value is None:
        return None if not required else ""
    s = str(value).strip()
    if not s:
        return None if not required else ""
    return s


def normalize_url(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    if not s.startswith(("http://", "https://")):
        s = "https://" + s
    try:
        p = urlparse(s)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = (p.path or "").rstrip("/")
        if not host:
            return None
        if "linkedin.com" in host:
            return f"https://{host}{path}" if path else f"https://{host}/"
        return f"https://{host}{path}/" if path else f"https://{host}/"
    except Exception:
        return None


def normalize_country_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    key = s.lower()
    if key in US_COUNTRY_ALIASES:
        return "United States"
    if key in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[key]
    # Title-case heuristic for full names
    return " ".join(w.capitalize() for w in s.split())


def is_us_country(value: Optional[str]) -> bool:
    if not value:
        return False
    return str(value).strip().lower() in US_COUNTRY_ALIASES or str(value).strip() == "United States"


def _parse_employee_range(text: str) -> Optional[tuple[int, int]]:
    """Parse (low, high) from common employee-count strings."""
    if not text:
        return None
    t = text.strip().lower()
    if "self-employed" in t or "self employed" in t:
        return (1, 1)
    t = t.replace(",", "")
    plus = re.search(r"(\d+)\+", t)
    if plus:
        return (int(plus.group(1)), 100_000)
    m = re.search(r"(\d+)\s*[-–—]\s*(\d+)", t)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    m2 = re.search(r"(\d+)", t)
    if m2:
        n = int(m2.group(1))
        return (n, n)
    return None


def normalize_employee_count_bucket(raw: Any) -> Optional[str]:
    """
    Map arbitrary employee text or number to one of ``EMPLOYEE_COUNT_BUCKETS``.

    Returns None if no safe mapping.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None

    # Already a valid bucket
    if s in EMPLOYEE_COUNT_BUCKETS:
        return s

    parsed = _parse_employee_range(s)
    if not parsed:
        return None
    low, high = parsed
    mid = (low + high) // 2
    if high >= 10001 or low >= 10001:
        return "10,001+"
    for lo, hi, label in _EMPLOYEE_RANGE_TO_LABEL:
        if lo <= mid <= hi:
            return label
    if mid > 10000:
        return "10,001+"
    return None


def _coerce_phone_list(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            data = json.loads(s)
            if isinstance(data, list):
                return [str(x).strip() for x in data if str(x).strip()]
        except json.JSONDecodeError:
            pass
        return [s]
    return [str(raw).strip()] if str(raw).strip() else []


def _coerce_socials(flat: Mapping[str, Any]) -> Dict[str, str]:
    raw = get_alias(flat, "socials")
    out: Dict[str, str] = {}
    if isinstance(raw, dict):
        tw = raw.get(SOCIALS_TWITTER_KEY) or raw.get("Twitter")
        u = normalize_url(trim_or_null(tw))
        if u:
            out[SOCIALS_TWITTER_KEY] = u
        return out
    # Twitter often stored as its own key
    for key in ("twitter", "Twitter", "twitter_url"):
        v = flat.get(key)
        if v:
            u = normalize_url(trim_or_null(v))
            if u:
                out[SOCIALS_TWITTER_KEY] = u
            break
    return out


def infer_source_type(source_url: Optional[str], website: Optional[str], company_li: Optional[str]) -> str:
    if not source_url:
        return ""
    u = source_url.lower()
    if "linkedin.com" in u:
        return "company_linkedin"
    if website and website.rstrip("/").lower() in u.rstrip("/").lower():
        return "company_website"
    if company_li and company_li.rstrip("/").lower() in u.rstrip("/").lower():
        return "company_linkedin"
    return "proprietary_database"


def resolve_source_url_and_type(
    flat: Mapping[str, Any],
    *,
    website: Optional[str],
    company_linkedin: Optional[str],
) -> tuple[str, str]:
    """Apply source_url / source_type rules from product spec."""
    su = trim_or_null(get_alias(flat, "source_url"))
    st = trim_or_null(get_alias(flat, "source_type"))

    if not su:
        if website:
            su = website
        elif company_linkedin:
            su = company_linkedin

    su = normalize_url(su) or su or ""

    if not st and su:
        st = infer_source_type(su, website, company_linkedin)

    if not su or not st:
        return SOURCE_UNCLEAR_VALUE, SOURCE_UNCLEAR_VALUE

    su_out = su or SOURCE_UNCLEAR_VALUE
    st_out = st or SOURCE_UNCLEAR_VALUE
    return su_out, st_out


def map_flat_to_contract(
    flat: Mapping[str, Any],
    *,
    industry: str,
    sub_industry: str,
) -> Dict[str, Any]:
    """
    Build the strict output dict (pre-validation).

    ``industry`` / ``sub_industry`` must already be taxonomy-valid strings.
    """
    row = empty_output_record()

    row[K_BUSINESS] = trim_or_null(get_alias(flat, "business"), required=True) or ""
    row[K_FIRST] = trim_or_null(get_alias(flat, "first"), required=True) or ""
    row[K_LAST] = trim_or_null(get_alias(flat, "last"), required=True) or ""
    fn = trim_or_null(get_alias(flat, "full_name"))
    if not fn and (row[K_FIRST] or row[K_LAST]):
        fn = f"{row[K_FIRST]} {row[K_LAST]}".strip() or None
    row[K_FULL_NAME] = fn or ""

    row[K_EMAIL] = trim_or_null(get_alias(flat, "email"), required=True) or ""
    row[K_ROLE] = trim_or_null(get_alias(flat, "role"), required=True) or ""

    w = normalize_url(trim_or_null(get_alias(flat, "website")))
    row[K_WEBSITE] = w or ""

    row[K_INDUSTRY] = industry
    row[K_SUB_INDUSTRY] = sub_industry

    country = normalize_country_name(trim_or_null(get_alias(flat, "country")))
    row[K_COUNTRY] = country or ""
    city = trim_or_null(get_alias(flat, "city"))
    row[K_CITY] = city or ""

    st_raw = trim_or_null(get_alias(flat, "state"))
    if is_us_country(country):
        row[K_STATE] = st_raw
    else:
        row[K_STATE] = None

    row[K_LINKEDIN] = normalize_url(trim_or_null(get_alias(flat, "linkedin"))) or ""
    row[K_COMPANY_LINKEDIN] = normalize_url(trim_or_null(get_alias(flat, "company_linkedin"))) or ""

    hq_c = normalize_country_name(trim_or_null(get_alias(flat, "hq_country")))
    row[K_HQ_COUNTRY] = hq_c or ""
    hq_st = trim_or_null(get_alias(flat, "hq_state"))
    if is_us_country(hq_c):
        row[K_HQ_STATE] = hq_st
    else:
        row[K_HQ_STATE] = None
    row[K_HQ_CITY] = trim_or_null(get_alias(flat, "hq_city"))

    row[K_DESCRIPTION] = trim_or_null(get_alias(flat, "description"), required=True) or ""

    ec = normalize_employee_count_bucket(get_alias(flat, "employee_count"))
    row[K_EMPLOYEE_COUNT] = ec or ""

    su, st = resolve_source_url_and_type(flat, website=w or None, company_linkedin=row[K_COMPANY_LINKEDIN] or None)
    row[K_SOURCE_URL] = su
    row[K_SOURCE_TYPE] = st

    row[K_PHONE_NUMBERS] = _coerce_phone_list(get_alias(flat, "phone_numbers"))
    row[K_SOCIALS] = _coerce_socials(flat)

    # Ensure types
    if not isinstance(row[K_PHONE_NUMBERS], list):
        row[K_PHONE_NUMBERS] = []
    if not isinstance(row[K_SOCIALS], dict):
        row[K_SOCIALS] = {}

    return row


def normalize_record_for_contract(record: JsonRecord) -> tuple[Dict[str, Any], JsonRecord]:
    """
    Unwrap and return ``(flat_merged, original_top)`` for downstream industry fix + mapping.
    """
    merged = unwrap_cache_record(record)
    return merged, record
