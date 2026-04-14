"""Scrapingdog scrape API — fetch homepage HTML and extract company fields."""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, Optional
from urllib.parse import urljoin, urlparse

import httpx

logger = logging.getLogger(__name__)

SCRAPINGDOG_SCRAPE = "https://api.scrapingdog.com/scrape"

_HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.I)
_JSONLD_RE = re.compile(
    r'<script[^>]+type\s*=\s*["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.I | re.S,
)
_LINKEDIN_RE = re.compile(
    r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/(company/[a-zA-Z0-9\-_%/]+)/?",
    re.I,
)
_EMPLOYEES_RE = re.compile(r"([\d,]+)\s*employees?\s*on\s*linkedin", re.I)


def fetch_homepage_html(api_key: str, url: str, *, timeout: float = 25.0, dynamic: bool = False) -> tuple[Optional[str], str]:
    """Return (html_or_none, error_message)."""
    if not (api_key or "").strip():
        return None, "missing_api_key"
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            r = client.get(
                SCRAPINGDOG_SCRAPE,
                params={
                    "api_key": api_key,
                    "url": url,
                    "dynamic": "true" if dynamic else "false",
                },
            )
        logger.info("Scrapingdog GET %s status=%s", url[:80], r.status_code)
        if r.status_code != 200:
            return None, f"http_{r.status_code}"
        text = (r.text or "").strip()
        if not text:
            return None, "empty_body"
        return text, ""
    except httpx.HTTPError as e:
        logger.warning("Scrapingdog transport error: %s", e)
        return None, str(e)


def parse_company_from_html(html: str, *, homepage_url: str, domain: str) -> Dict[str, Any]:
    """
    Best-effort extraction. Sets ``source_url`` / ``source_type`` when clear.

    If extraction is unclear, both ``source_url`` and ``source_type`` are set to ``\"unknown\"``
    (per product rule: unified unknown marker).
    """
    out: Dict[str, Any] = {
        "company_domain": domain,
        "company_website_url": homepage_url,
        "employee_count": None,
        "industry": None,
        "sub_industry": None,
        "company_linkedin_url": None,
        "hq_location_country": None,
        "hq_location_state": None,
        "hq_location_city": None,
        "source_url": homepage_url,
        "source_type": "company_website",
    }

    m = _LINKEDIN_RE.search(html)
    if m:
        out["company_linkedin_url"] = f"https://www.linkedin.com/{m.group(1).rstrip('/')}"

    em = _EMPLOYEES_RE.search(html)
    if em:
        out["employee_count"] = em.group(1).replace(",", "")

    org = _extract_jsonld_org(html)
    if org:
        if not out.get("industry"):
            ind = org.get("industry")
            if isinstance(ind, str):
                out["industry"] = ind
            elif isinstance(ind, list) and ind:
                out["industry"] = str(ind[0])
        addr = org.get("address")
        if isinstance(addr, dict):
            out["hq_location_country"] = _s(addr.get("addressCountry"))
            out["hq_location_state"] = _s(addr.get("addressRegion"))
            out["hq_location_city"] = _s(addr.get("addressLocality"))
        elif isinstance(addr, list) and addr and isinstance(addr[0], dict):
            a0 = addr[0]
            out["hq_location_country"] = out.get("hq_location_country") or _s(a0.get("addressCountry"))
            out["hq_location_state"] = out.get("hq_location_state") or _s(a0.get("addressRegion"))
            out["hq_location_city"] = out.get("hq_location_city") or _s(a0.get("addressLocality"))

    # If we still have almost nothing beyond URL, mark unknown
    has_signal = bool(
        out.get("company_linkedin_url")
        or out.get("employee_count")
        or out.get("industry")
        or out.get("hq_location_country")
    )
    if not has_signal:
        out["source_url"] = "unknown"
        out["source_type"] = "unknown"

    return out


def _s(v: Any) -> Optional[str]:
    if v is None:
        return None
    t = str(v).strip()
    return t or None


def _extract_jsonld_org(html: str) -> Optional[Dict[str, Any]]:
    for m in _JSONLD_RE.finditer(html):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        items: list = data if isinstance(data, list) else [data]
        for obj in items:
            if not isinstance(obj, dict):
                continue
            t = obj.get("@type")
            types = {str(x).lower() for x in (t if isinstance(t, list) else [t]) if x}
            if "organization" in types or "corporation" in types:
                return obj
            if obj.get("name") and (obj.get("url") or obj.get("sameAs")):
                return obj
    return None


def scrape_domain(
    domain: str,
    api_key: str,
    *,
    dynamic: bool = False,
) -> Dict[str, Any]:
    """
    Scrape ``https://{domain}/`` and return a flat dict compatible with ``CompanyProfile``.

    On hard failure returns minimal dict with ``source_url`` / ``source_type`` = ``\"unknown\"``.
    """
    d = (domain or "").strip().lower().replace("www.", "")
    if not d:
        return {
            "company_domain": "",
            "source_url": "unknown",
            "source_type": "unknown",
        }
    home = f"https://{d}/"
    html, err = fetch_homepage_html(api_key, home, dynamic=dynamic)
    if not html:
        logger.warning("Scrapingdog scrape failed domain=%s err=%s", d, err)
        return {
            "company_domain": d,
            "source_url": "unknown",
            "source_type": "unknown",
        }
    return parse_company_from_html(html, homepage_url=home, domain=d)
