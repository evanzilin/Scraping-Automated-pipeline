"""Async Scrapingdog scrape client with retries, bounded concurrency, and HTML/JSON-LD parsing."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from typing import Any, Dict, List, Optional

import httpx

from app.company_enrichment.models import EnrichedFields, HQLocation

logger = logging.getLogger(__name__)

SCRAPINGDOG_SCRAPE_URL = "https://api.scrapingdog.com/scrape"

_HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.I)
_JSONLD_RE = re.compile(
    r'<script[^>]+type\s*=\s*["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.I | re.S,
)
_LINKEDIN_COMPANY_RE = re.compile(
    r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/(company/[a-zA-Z0-9\-_%]+)/?",
    re.I,
)
_EMPLOYEES_LI_RE = re.compile(
    r"([\d,]+)\s*employees?\s*on\s*linkedin",
    re.I,
)


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in (429, 500, 502, 503, 504)
    return False


async def _sleep_backoff(attempt: int) -> None:
    base = min(8.0, 0.25 * (2**attempt))
    jitter = random.uniform(0, base * 0.25)
    await asyncio.sleep(base + jitter)


class ScrapingdogEnrichmentClient:
    """Shared async HTTP client + semaphore for domain homepage scrapes."""

    __slots__ = ("_api_key", "_client", "_sem", "_timeout", "_max_retries", "_owns_client")

    def __init__(
        self,
        api_key: str,
        *,
        max_concurrent: int = 12,
        timeout_s: float = 25.0,
        max_retries: int = 3,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self._api_key = (api_key or "").strip()
        self._sem = asyncio.Semaphore(max(1, max_concurrent))
        self._timeout = timeout_s
        self._max_retries = max_retries
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_s, connect=5.0),
            limits=httpx.Limits(max_connections=max_concurrent + 4, max_keepalive_connections=max_concurrent),
            follow_redirects=True,
        )
        self._owns_client = client is None

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def fetch_html(self, url: str, *, dynamic: bool = False) -> tuple[Optional[str], int, float]:
        if not self._api_key:
            return None, 0, 0.0
        params = {
            "api_key": self._api_key,
            "url": url,
            "dynamic": "true" if dynamic else "false",
        }
        t0 = time.perf_counter()
        last_status = 0
        for attempt in range(self._max_retries):
            try:
                async with self._sem:
                    r = await self._client.get(SCRAPINGDOG_SCRAPE_URL, params=params)
                last_status = r.status_code
                elapsed_ms = (time.perf_counter() - t0) * 1000
                logger.info(
                    "scrapingdog status=%s url=%s latency_ms=%.1f",
                    r.status_code,
                    url[:80],
                    elapsed_ms,
                )
                if r.status_code == 200 and (r.text or "").strip():
                    return r.text, r.status_code, elapsed_ms
                if r.status_code in (429, 500, 502, 503, 504):
                    await _sleep_backoff(attempt)
                    continue
                return None, r.status_code, elapsed_ms
            except Exception as e:
                logger.warning("scrapingdog error %s url=%s attempt=%s", e, url[:80], attempt)
                if attempt + 1 < self._max_retries and _is_retryable(e):
                    await _sleep_backoff(attempt)
                    continue
                return None, last_status, (time.perf_counter() - t0) * 1000
        return None, last_status, (time.perf_counter() - t0) * 1000

    def parse_enrichment_from_html(self, html: str, seed_domain: str) -> EnrichedFields:
        out = EnrichedFields()
        if not html:
            out.raw_error = "empty_html"
            return out
        low = html.lower()
        # Website canonical
        if seed_domain:
            out.company_website_url = f"https://{seed_domain}/"

        # LinkedIn from raw HTML
        m = _LINKEDIN_COMPANY_RE.search(html)
        if m:
            out.company_linkedin_url = f"https://www.linkedin.com/{m.group(1).rstrip('/')}"

        m2 = _EMPLOYEES_LI_RE.search(low)
        if m2:
            out.employee_count = m2.group(1).replace(",", "").strip()

        org = _extract_jsonld_organization(html)
        if org:
            self._apply_org(out, org)

        if not out.any_value() and not out.company_website_url:
            out.raw_error = "no_fields_extracted"
        return out

    def _apply_org(self, target: EnrichedFields, org: Dict[str, Any]) -> None:
        if not target.industry:
            industry = org.get("industry")
            if isinstance(industry, str):
                target.industry = industry
            elif isinstance(industry, list) and industry:
                target.industry = str(industry[0])
        if not target.sub_industry:
            naics = org.get("naics") or org.get("subIndustry")
            if isinstance(naics, str):
                target.sub_industry = naics
        addr = org.get("address")
        if isinstance(addr, dict):
            if not target.hq.country:
                target.hq.country = str(addr.get("addressCountry") or "") or None
            if not target.hq.city:
                target.hq.city = str(addr.get("addressLocality") or "") or None
            if not target.hq.state:
                target.hq.state = str(addr.get("addressRegion") or "") or None
        elif isinstance(addr, list) and addr and isinstance(addr[0], dict):
            a0 = addr[0]
            if not target.hq.country:
                target.hq.country = str(a0.get("addressCountry") or "") or None
            if not target.hq.city:
                target.hq.city = str(a0.get("addressLocality") or "") or None
            if not target.hq.state:
                target.hq.state = str(a0.get("addressRegion") or "") or None


def _extract_jsonld_organization(html: str) -> Optional[Dict[str, Any]]:
    for m in _JSONLD_RE.finditer(html):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        candidates: List[Dict[str, Any]] = []
        if isinstance(data, dict):
            candidates.append(data)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    candidates.append(item)
        for obj in candidates:
            t = obj.get("@type")
            types = t if isinstance(t, list) else ([t] if t else [])
            types_l = {str(x).lower() for x in types if x}
            if "organization" in types_l or "corporation" in types_l:
                return obj
            if obj.get("name") and (obj.get("url") or obj.get("sameAs")):
                return obj
    return None


async def enrich_domain(
    client: ScrapingdogEnrichmentClient,
    domain: str,
    *,
    try_dynamic: bool = False,
) -> EnrichedFields:
    d = (domain or "").strip().lower()
    if not d or d.startswith("."):
        return EnrichedFields(raw_error="bad_domain")
    url = f"https://{d}/"
    html, status, _ms = await client.fetch_html(url, dynamic=False)
    if not html and try_dynamic:
        html, status, _ms = await client.fetch_html(url, dynamic=True)
    if not html:
        return EnrichedFields(raw_error=f"http_{status}")
    return client.parse_enrichment_from_html(html, d)
