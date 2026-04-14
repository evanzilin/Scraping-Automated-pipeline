"""Normalize company profile fields for storage and comparison."""

from __future__ import annotations

import re
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from company_profile.company_contract import CompanyProfile


def _opt(v: Optional[str]) -> Optional[str]:
    if not v:
        return None
    s = str(v).strip()
    return s or None

_RE_MULTI_SPACE = re.compile(r"\s+")
_US = frozenset(
    {"us", "usa", "u.s.", "u.s.a.", "united states", "united states of america"}
)


def normalize_domain(value: Optional[str]) -> str:
    if not value:
        return ""
    s = str(value).strip().lower()
    if "://" in s:
        host = urlparse(s).netloc or urlparse("https://" + s).netloc
        s = host or s
    s = s.split("/")[0].split(":")[0]
    if s.startswith("www."):
        s = s[4:]
    return s


def normalize_website_url(value: Optional[str]) -> Optional[str]:
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
        return f"https://{host}{path}" if path else f"https://{host}/"
    except Exception:
        return None


def normalize_linkedin_url(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip()
    if "linkedin.com/company/" not in s.lower():
        return None
    m = re.search(
        r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/(company/[a-zA-Z0-9\-_%/]+)",
        s,
        re.I,
    )
    if m:
        return ("https://www.linkedin.com/" + m.group(1).rstrip("/")).lower()
    return None


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    s = _RE_MULTI_SPACE.sub(" ", str(value).strip().lower())
    return s or None


def normalize_country(value: Optional[str]) -> Optional[str]:
    t = normalize_text(value)
    if not t:
        return None
    if t in _US:
        return "US"
    return t.title() if len(t) <= 3 else t.title()


def normalize_state(value: Optional[str], country: Optional[str]) -> Optional[str]:
    c = (country or "").strip().upper()
    if c != "US":
        return None
    if not value:
        return None
    s = str(value).strip().upper()
    return s or None


def normalize_city(value: Optional[str]) -> Optional[str]:
    t = normalize_text(value)
    return t.title() if t else None


def normalize_employee_count(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    s = re.sub(r"[^\d]", "", str(value))
    return s or str(value).strip() or None


def normalize_profile(profile: CompanyProfile) -> CompanyProfile:
    """Return a new ``CompanyProfile`` with normalized fields; clears US state when country is not US."""
    country = normalize_country(profile.hq_location_country)
    state = normalize_state(profile.hq_location_state, country)
    return CompanyProfile(
        company_domain=normalize_domain(profile.company_domain),
        company_website_url=normalize_website_url(profile.company_website_url),
        employee_count=normalize_employee_count(profile.employee_count),
        industry=normalize_text(profile.industry),
        sub_industry=normalize_text(profile.sub_industry),
        company_linkedin_url=(
            normalize_linkedin_url(profile.company_linkedin_url)
            or _opt(profile.company_linkedin_url)
        ),
        hq_location_country=country,
        hq_location_state=state,
        hq_location_city=normalize_city(profile.hq_location_city),
        source_url=normalize_website_url(profile.source_url) or (profile.source_url and str(profile.source_url).strip() or None),
        source_type=normalize_text(profile.source_type) or profile.source_type,
        id=profile.id,
    )


def apply_us_state_rule(d: Dict[str, Any]) -> Dict[str, Any]:
    """If country is not US, force ``hq_location_state`` to None in a dict copy."""
    out = dict(d)
    c = str(out.get("hq_location_country") or "").strip().upper()
    if c != "US":
        out["hq_location_state"] = None
    return out
