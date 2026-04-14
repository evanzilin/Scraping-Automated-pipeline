"""Fast string/URL normalization for comparison (hot path: pure functions)."""

from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urlparse

# Precompiled
_RE_NON_DIGITS = re.compile(r"[^\d]")
_RE_MULTI_SPACE = re.compile(r"\s+")
_RE_SEPARATORS = re.compile(r"[/|,;]+")

_US_COUNTRY = frozenset(
    {
        "us",
        "usa",
        "u.s.",
        "u.s.a.",
        "united states",
        "united states of america",
    }
)


def extract_domain(url_or_email: str) -> str:
    s = (url_or_email or "").strip()
    if not s:
        return ""
    if "@" in s and "://" not in s:
        parts = s.rsplit("@", 1)
        if len(parts) == 2:
            return normalize_domain(parts[1])
    return normalize_domain(s)


def normalize_domain(value: Optional[str]) -> str:
    if not value:
        return ""
    s = str(value).strip().lower()
    if not s:
        return ""
    if "://" in s:
        netloc = urlparse(s).netloc or urlparse("https://" + s).netloc
        s = netloc or s
    s = s.split("/")[0].split(":")[0]
    if s.startswith("www."):
        s = s[4:]
    return s.strip().lower()


def normalize_url(value: Optional[str]) -> str:
    if not value:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    if "://" not in s:
        s = "https://" + s
    try:
        p = urlparse(s)
    except Exception:
        return ""
    host = (p.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    path = (p.path or "").rstrip("/")
    if path in ("", "/"):
        return host
    return f"{host}{path}".lower()


def normalize_linkedin_company_url(value: Optional[str]) -> str:
    if not value:
        return ""
    s = str(value).strip()
    low = s.lower()
    if "linkedin.com/company/" not in low:
        return ""
    m = re.search(
        r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/(company/[^?\s\"'#]+)",
        s,
        re.I,
    )
    if m:
        return ("https://www.linkedin.com/" + m.group(1).rstrip("/")).lower()
    m = re.search(r"linkedin\.com/(company/[^?\s\"'#]+)", s, re.I)
    if m:
        return ("https://www.linkedin.com/" + m.group(1).rstrip("/")).lower()
    return ""


def normalize_employee_count(value: Optional[str]) -> str:
    if value is None:
        return ""
    s = str(value).strip().lower()
    if not s:
        return ""
    digits = _RE_NON_DIGITS.sub("", s)
    if not digits:
        return s
    try:
        n = int(digits)
    except ValueError:
        return s
    if n < 10:
        return "1-9"
    if n < 50:
        return "10-49"
    if n < 200:
        return "50-199"
    if n < 500:
        return "200-499"
    if n < 1000:
        return "500-999"
    if n < 5000:
        return "1000-4999"
    if n < 10000:
        return "5000-9999"
    return "10000+"


def normalize_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    s = _RE_MULTI_SPACE.sub(" ", str(value).strip().lower())
    s = _RE_SEPARATORS.sub(" ", s)
    return s.strip()


def normalize_country(value: Optional[str]) -> str:
    t = normalize_text(value)
    if t in _US_COUNTRY:
        return "us"
    return t


def normalize_state(value: Optional[str], country_norm: str) -> Optional[str]:
    if country_norm != "us":
        return None
    s = normalize_text(value)
    return s or None


def normalize_city(value: Optional[str]) -> str:
    return normalize_text(value)


def website_domains_equal(a: Optional[str], b: Optional[str]) -> bool:
    return normalize_domain(a) == normalize_domain(b) and bool(normalize_domain(a))
