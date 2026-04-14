"""
Resolve a company's HQ country using Scrapingdog's LinkedIn company API and optional Google Search.

1. Fetch company JSON via ``GET https://api.scrapingdog.com/profile`` (``type=company``) with
   fallback to ``/linkedin`` (``linkId``), reusing the same client as ``company_b2b_scrapingdog``.
2. Parse the headquarters / location string; if it matches a US state name or abbreviation,
   ``resolved_country`` is ``United States``.
3. If missing or non-US, search ``{company_name} headquarters country`` via Scrapingdog's
   Google Search API and infer country from organic snippets.

Output shape::

    {"company_name": str, "raw_location": str, "resolved_country": str}
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

from company_b2b_scrapingdog import (
    _fetch_scrapingdog_linkedin_company_profile,
    _google_organic_results,
    _linkedin_profile_data_roots,
    _snippet_from_google_organic_item,
)
from scrape_api_key import get_scrape_api_key

# --- US states: all 50 full names + USPS abbreviations (reference set) ---
_US_PAIRS: List[Tuple[str, str]] = [
    ("Alabama", "AL"),
    ("Alaska", "AK"),
    ("Arizona", "AZ"),
    ("Arkansas", "AR"),
    ("California", "CA"),
    ("Colorado", "CO"),
    ("Connecticut", "CT"),
    ("Delaware", "DE"),
    ("Florida", "FL"),
    ("Georgia", "GA"),
    ("Hawaii", "HI"),
    ("Idaho", "ID"),
    ("Illinois", "IL"),
    ("Indiana", "IN"),
    ("Iowa", "IA"),
    ("Kansas", "KS"),
    ("Kentucky", "KY"),
    ("Louisiana", "LA"),
    ("Maine", "ME"),
    ("Maryland", "MD"),
    ("Massachusetts", "MA"),
    ("Michigan", "MI"),
    ("Minnesota", "MN"),
    ("Mississippi", "MS"),
    ("Missouri", "MO"),
    ("Montana", "MT"),
    ("Nebraska", "NE"),
    ("Nevada", "NV"),
    ("New Hampshire", "NH"),
    ("New Jersey", "NJ"),
    ("New Mexico", "NM"),
    ("New York", "NY"),
    ("North Carolina", "NC"),
    ("North Dakota", "ND"),
    ("Ohio", "OH"),
    ("Oklahoma", "OK"),
    ("Oregon", "OR"),
    ("Pennsylvania", "PA"),
    ("Rhode Island", "RI"),
    ("South Carolina", "SC"),
    ("South Dakota", "SD"),
    ("Tennessee", "TN"),
    ("Texas", "TX"),
    ("Utah", "UT"),
    ("Vermont", "VT"),
    ("Virginia", "VA"),
    ("Washington", "WA"),
    ("West Virginia", "WV"),
    ("Wisconsin", "WI"),
    ("Wyoming", "WY"),
]

# Longest names first so "New York" wins over "York" and "West Virginia" over "Virginia"
_US_FULL_NAMES_SORTED: Tuple[str, ...] = tuple(
    sorted((p[0] for p in _US_PAIRS), key=len, reverse=True)
)
_US_ABBREVS: Tuple[str, ...] = tuple(p[1].upper() for p in _US_PAIRS)

# Snippet / text hints for United States (fallback path)
_US_COUNTRY_PHRASES = (
    "united states",
    "u.s.a.",
    "u.s.",
    "usa",
    "america",
)


def _text_implies_united_states(text: str) -> bool:
    """True if ``text`` mentions a US state/DC-style pattern we treat as US HQ."""
    if not (text or "").strip():
        return False
    low = text.lower()
    for phrase in _US_COUNTRY_PHRASES:
        if phrase in low:
            return True
    for name in _US_FULL_NAMES_SORTED:
        if name.lower() in low:
            return True
    for abbr in _US_ABBREVS:
        if re.search(rf"\b{re.escape(abbr)}\b", text, flags=re.IGNORECASE):
            return True
    return False


def _company_display_name_from_profile(
    profile: Optional[Dict[str, Any]],
    company_slug: str,
) -> str:
    if not profile:
        return company_slug.replace("-", " ").strip() or company_slug
    for root in _linkedin_profile_data_roots(profile):
        for k in ("name", "localizedName", "companyName", "universalName"):
            v = root.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return company_slug.replace("-", " ").strip() or company_slug


def _raw_headquarters_string_from_profile(profile: Optional[Dict[str, Any]]) -> str:
    """Best-effort single-line HQ/location from Scrapingdog LinkedIn company JSON."""
    if not profile:
        return ""
    for root in _linkedin_profile_data_roots(profile):
        for key in (
            "headquarters",
            "headquarter",
            "hq",
            "formattedLocation",
            "formattedAddress",
            "location",
        ):
            v = root.get(key)
            if isinstance(v, str) and len(v.strip()) > 2:
                return re.sub(r"\s+", " ", v.strip())
        loc = root.get("location")
        if isinstance(loc, dict):
            parts = []
            for part_key in (
                "line1",
                "line2",
                "city",
                "addressLocality",
                "geographicArea",
                "state",
                "addressRegion",
                "country",
                "countryName",
            ):
                p = loc.get(part_key)
                if isinstance(p, str) and p.strip():
                    parts.append(p.strip())
            if parts:
                return ", ".join(parts)
    return ""


# Common countries mentioned in Google snippets (order: more specific patterns first)
_COUNTRY_SNIPPET_PATTERNS: List[Tuple[str, str]] = [
    (r"United\s+States|U\.S\.A?\.|USA\b|American\s+company", "United States"),
    (r"United\s+Kingdom|U\.K\.|UK\b|England\b|Scotland\b|Wales\b", "United Kingdom"),
    (r"Canada\b|Canadian\b", "Canada"),
    (r"Germany\b|German\b", "Germany"),
    (r"France\b|French\b", "France"),
    (r"India\b|Indian\b", "India"),
    (r"Australia\b|Australian\b", "Australia"),
    (r"Japan\b|Japanese\b", "Japan"),
    (r"China\b|Chinese\b", "China"),
    (r"Brazil\b|Brazilian\b", "Brazil"),
    (r"Netherlands\b|Dutch\b", "Netherlands"),
    (r"Singapore\b", "Singapore"),
    (r"Ireland\b|Irish\b", "Ireland"),
    (r"Israel\b|Israeli\b", "Israel"),
    (r"Spain\b|Spanish\b", "Spain"),
    (r"Italy\b|Italian\b", "Italy"),
    (r"Mexico\b|Mexican\b", "Mexico"),
    (r"South\s+Africa\b", "South Africa"),
    (r"Switzerland\b|Swiss\b", "Switzerland"),
    (r"Sweden\b|Swedish\b", "Sweden"),
    (r"Norway\b|Norwegian\b", "Norway"),
    (r"Finland\b|Finnish\b", "Finland"),
    (r"Poland\b|Polish\b", "Poland"),
    (r"Belgium\b|Belgian\b", "Belgium"),
    (r"Austria\b|Austrian\b", "Austria"),
    (r"United\s+Arab\s+Emirates|UAE\b|Dubai\b", "United Arab Emirates"),
]


def _country_from_search_snippets(blob: str) -> str:
    if not (blob or "").strip():
        return ""
    for rx, label in _COUNTRY_SNIPPET_PATTERNS:
        if re.search(rx, blob, flags=re.IGNORECASE):
            return label
    low = blob.lower()
    if _text_implies_united_states(blob):
        return "United States"
    return ""


def _google_headquarters_country_snippets(
    api_key: str,
    company_name: str,
    *,
    timeout: float,
    country: str,
) -> str:
    q = f"{company_name.strip()} headquarters country"
    items = _google_organic_results(
        api_key,
        q,
        timeout=timeout,
        country=country,
        results=8,
    )
    chunks: List[str] = []
    for it in items:
        if isinstance(it, dict):
            s = _snippet_from_google_organic_item(it)
            t = (it.get("title") or "") if isinstance(it.get("title"), str) else ""
            if s:
                chunks.append(s)
            if t:
                chunks.append(t)
    return " ".join(chunks)


def resolve_company_hq_country(
    scrapingdog_api_key: str,
    company_slug: str,
    *,
    company_name: Optional[str] = None,
    timeout: float = 60.0,
    google_search_country: str = "us",
) -> Dict[str, Any]:
    """
    Return ``company_name``, ``raw_location``, and ``resolved_country``.

    ``company_slug`` is the LinkedIn company id (e.g. ``amazon`` from ``/company/amazon``).
    """
    key = (scrapingdog_api_key or "").strip()
    slug = (company_slug or "").strip()
    out_company = (company_name or "").strip()

    profile: Optional[Dict[str, Any]] = None
    if key and slug:
        prof_a, prof_b = _fetch_scrapingdog_linkedin_company_profile(
            key,
            slug,
            timeout=timeout,
        )
        profile = prof_a or prof_b

    if not out_company:
        out_company = _company_display_name_from_profile(profile, slug)

    raw_location = _raw_headquarters_string_from_profile(profile)

    resolved = ""
    if raw_location and _text_implies_united_states(raw_location):
        resolved = "United States"
    elif key and out_company:
        blob = _google_headquarters_country_snippets(
            key,
            out_company,
            timeout=timeout,
            country=google_search_country,
        )
        resolved = _country_from_search_snippets(blob)

    return {
        "company_name": out_company,
        "raw_location": raw_location,
        "resolved_country": resolved,
    }


def main() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    p = argparse.ArgumentParser(
        description="Resolve HQ country via Scrapingdog LinkedIn + Google Search APIs."
    )
    p.add_argument(
        "company_slug",
        help="LinkedIn company id (e.g. krispcall from linkedin.com/company/krispcall)",
    )
    p.add_argument(
        "--api-key",
        default=get_scrape_api_key(),
        help="Scrape API key (default: SCRAPE_API_KEY or legacy SCRAPINGDOG_*)",
    )
    p.add_argument(
        "--company-name",
        default="",
        help="Display name for Google query (default: from API or slug)",
    )
    p.add_argument("--timeout", type=float, default=60.0)
    p.add_argument("--google-country", default="us", help="Google Search country code")
    args = parser.parse_args()

    if not (args.api_key or "").strip():
        print(
            "ERROR: Set SCRAPE_API_KEY or pass --api-key",
            file=sys.stderr,
        )
        sys.exit(1)

    result = resolve_company_hq_country(
        args.api_key,
        args.company_slug,
        company_name=(args.company_name or None),
        timeout=args.timeout,
        google_search_country=args.google_country,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
