"""
Single scrape key for Scrapingdog, LinkedIn scraper, and ScraperAPI-style fallbacks.

Set ``SCRAPE_API_KEY`` once. Legacy variables are still read if ``SCRAPE_API_KEY`` is unset
(so existing deployments keep working).
"""

from __future__ import annotations

import os

# First match wins. Keep vendor-specific names as fallbacks for backward compatibility.
_UNIFIED_ENV_NAMES: tuple[str, ...] = (
    "SCRAPE_API_KEY",
    "SCRAPINGDOG_API_KEY",
    "SCRAPINGDOG_LINKEDIN_API_KEY",
    "LINKEDIN_SCRAPER_API_KEY",
    "SCRAPERAPI_API_KEY",
    "SCRAPERAPI_KEY",
    "SCRAPERAPI",
)


def get_scrape_api_key() -> str:
    """Return the first non-empty configured API key, or ``\"\"``."""
    for name in _UNIFIED_ENV_NAMES:
        v = (os.environ.get(name) or "").strip()
        if v:
            return v
    return ""
