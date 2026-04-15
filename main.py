"""
B2B-only company web enrichment via Scrapingdog scrape API.

Skips work when ``is_b2b`` is False. Otherwise scrapes the company domain,
extracts business name from HTML metadata, then resolves LinkedIn company URL,
about-page URL, and major social links.

When no suitable about page is found on the scraped site, ``source_url`` is set
to the sentinel ``proprietary_database`` and ``source_type`` must be
``proprietary_database`` (per product rules).

``industry`` / ``sub_industry`` come from call arguments; scraped **industry** hints use only
LinkedIn (profile JSON plus scraped company page), while scraped **sub_industry** hints use only
the company website (homepage and about page). All are **refined** to canonical pairs from
``industry_taxonomy.py`` via ``lead_cache_json.industry_normalizer.normalize_industry_pair``.
When no taxonomy match is found, merged **raw** strings are kept. Set
``COMPANY_B2B_SKIP_INDUSTRY_TAXONOMY=1`` to skip canonical mapping. Hints are **ordered** so
taxonomy-aligned strings (API + page) merge first.

``to_dict()`` includes ``description`` from on-page meta / JSON-LD when present; if still
empty, the **Google Search Scraper API** (``/google``) may supply an organic-result snippet
(see ``google_search_for_description``). Also ``employee_count``, ``hq_country``, ``hq_state``
(only when ``hq_country`` is the United States, including when inferred because ``hq_state`` is
a US code/name but country was empty, or because a US state was wrongly stored in the country
field; country is shown as ``United States``, and state codes expand to full names, e.g.
``NY`` → ``New York``), ``hq_city``, and ``company_linkedin`` (empty string
when unknown). ``employee_count`` is normalized to canonical range strings such as ``51-200`` or
``5,001-10,000`` (no trailing ``employees``). ``to_dict()`` uses lead-style keys ``website``, ``business``, ``company_linkedin``,
and ``socials`` (see ``lead_format.json``).

Use :func:`domain_to_lead_format_json` to go from **domain → full lead-shaped dict** matching
``lead_format.json`` (person fields left empty; company + HQ + website filled from enrichment).

When the homepage cannot be scraped, optional **domain correction** uses the Scrapingdog
**Google Search Scraper API** (same ``api_key``, extra credits): organic-result hosts are
collected from ``link``, ``displayed_link``, ``cite`` / ``domain``, scored for similarity to
the input host, with a relaxed pass on early results if needed; a host blocklist drops
aggregators and social directories. After a successful load, **canonical** / ``og:url``
(same host) is followed so ``company_website_url`` matches the page; ``resolved_domain`` is
aligned to that URL’s normalized host.

When a LinkedIn company URL is found, the module can **scrape that page** with Scrapingdog
(see ``scrape_linkedin_company_page``) to pull extra industry/name/meta signals; LinkedIn
markup changes over time, so extraction is best-effort.

When ``linkedin_scraper_api_key`` is set (or env ``SCRAPE_API_KEY``, with legacy fallbacks
``SCRAPINGDOG_LINKEDIN_API_KEY`` / ``LINKEDIN_SCRAPER_API_KEY`` / ``SCRAPINGDOG_API_KEY``), **industry** strings (plus
**employee_count** and **HQ**) are read from both **LinkedIn company profile** endpoints
(``/profile?id=`` and ``/linkedin?linkId=`` with ``type=company``); API industry hints are
merged **before** HTML and drive taxonomy refinement. Your dashboard must include the LinkedIn /
profile scraper product for that key.
"""

from __future__ import annotations

import html as html_module
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urljoin, urlparse

import requests

from lead_cache_json.lead_json_io import write_lead_json_to_temp_file
from lead_cache_json.validators import apply_empty_string_to_null, validate_output_record
from scrape_api_key import get_scrape_api_key

# Web Scrape API — returns page HTML (parse JSON-LD / meta locally). Typically lowest cost per
# company page vs. SERP calls; use ``dynamic=false`` when static HTML is enough (see Scrapingdog pricing).
SCRAPINGDOG_SCRAPE_URL = "https://api.scrapingdog.com/scrape"
# Google Search Scraper API — returns parsed JSON (e.g. ``organic_results``), not SERP HTML, when
# ``html`` is not enabled. Cheapest tier is ~5 credits/request with ``advance_search`` and
# ``mob_search`` false; enabling those is ~10 credits each (see Scrapingdog Google Search docs).
SCRAPINGDOG_GOOGLE_URL = "https://api.scrapingdog.com/google"
# LinkedIn company profile JSON (Scrapingdog Profile / LinkedIn scraper API — see product docs).
SCRAPINGDOG_LINKEDIN_PROFILE_URL = "https://api.scrapingdog.com/profile"
# Alternate LinkedIn product URL (``linkId`` param); used if ``/profile`` returns no usable JSON.
SCRAPINGDOG_LINKEDIN_LINKID_URL = "https://api.scrapingdog.com/linkedin"

# Hosts to skip when picking a “company website” from Google organic links (domain correction).
_DOMAIN_CORRECTION_HOST_BLOCKLIST = frozenset(
    {
        "linkedin.com",
        "facebook.com",
        "twitter.com",
        "x.com",
        "instagram.com",
        "youtube.com",
        "tiktok.com",
        "pinterest.com",
        "wikipedia.org",
        "wikimedia.org",
        "crunchbase.com",
        "bloomberg.com",
        "reuters.com",
        "glassdoor.com",
        "indeed.com",
        "zoominfo.com",
        "rocketreach.co",
        "yelp.com",
        "yellowpages.com",
        "bbb.org",
        "google.com",
        "gstatic.com",
        "bing.com",
        "apple.com",
        "play.google.com",
        "apps.apple.com",
    }
)

SOURCE_TYPE_COMPANY_WEBSITE = "company_website"
SOURCE_TYPE_PROPRIETARY_DATABASE = "proprietary_database"
PROPRIETARY_SOURCE_URL = "proprietary_database"

_HREF_RE = re.compile(r'href\s*=\s*["\']([^"\']+)["\']', re.I)
_TITLE_RE = re.compile(r"<title[^>]*>([^<]+)</title>", re.I | re.S)
_OG_SITE_NAME_RE = re.compile(
    r'<meta[^>]+property\s*=\s*["\']og:site_name["\'][^>]*content\s*=\s*["\']([^"\']+)["\']',
    re.I,
)
_OG_SITE_NAME_RE_ALT = re.compile(
    r'<meta[^>]+content\s*=\s*["\']([^"\']+)["\'][^>]*property\s*=\s*["\']og:site_name["\']',
    re.I,
)
_APP_NAME_RE = re.compile(
    r'<meta[^>]+name\s*=\s*["\']application-name["\'][^>]*content\s*=\s*["\']([^"\']+)["\']',
    re.I,
)
_CANONICAL_LINK_RE = re.compile(
    r'<link[^>]+rel\s*=\s*["\']canonical["\'][^>]*href\s*=\s*["\']([^"\']+)["\']',
    re.I,
)
_CANONICAL_LINK_RE_ALT = re.compile(
    r'<link[^>]+href\s*=\s*["\']([^"\']+)["\'][^>]*rel\s*=\s*["\']canonical["\']',
    re.I,
)
_OG_URL_RE = re.compile(
    r'<meta[^>]+property\s*=\s*["\']og:url["\'][^>]*content\s*=\s*["\']([^"\']+)["\']',
    re.I,
)
_OG_URL_RE_ALT = re.compile(
    r'<meta[^>]+content\s*=\s*["\']([^"\']+)["\'][^>]*property\s*=\s*["\']og:url["\']',
    re.I,
)

_LD_JSON_SCRIPT_RE = re.compile(
    r'<script[^>]*type\s*=\s*["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.I | re.S,
)
_META_DESC_RE = re.compile(
    r'<meta[^>]+name\s*=\s*["\']description["\'][^>]*content\s*=\s*["\']([^"\']+)["\']',
    re.I,
)
_META_DESC_RE_ALT = re.compile(
    r'<meta[^>]+content\s*=\s*["\']([^"\']+)["\'][^>]*name\s*=\s*["\']description["\']',
    re.I,
)
_META_OG_DESC_RE = re.compile(
    r'<meta[^>]+property\s*=\s*["\']og:description["\'][^>]*content\s*=\s*["\']([^"\']+)["\']',
    re.I,
)
_META_OG_DESC_RE_ALT = re.compile(
    r'<meta[^>]+content\s*=\s*["\']([^"\']+)["\'][^>]*property\s*=\s*["\']og:description["\']',
    re.I,
)
_META_ARTICLE_SECTION_RE = re.compile(
    r'<meta[^>]+property\s*=\s*["\']article:section["\'][^>]*content\s*=\s*["\']([^"\']+)["\']',
    re.I,
)
_META_KEYWORDS_RE = re.compile(
    r'<meta[^>]+name\s*=\s*["\']keywords["\'][^>]*content\s*=\s*["\']([^"\']+)["\']',
    re.I,
)

_ORG_LD_TYPE_MARKERS = (
    "organization",
    "corporation",
    "localbusiness",
    "professional",
    "airline",
    "newsmedia",
    "store",
    "brand",
    "business",
    "medical",
    "dentist",
    "lawyer",
    "attorney",
    "financial",
    "library",
    "government",
    "school",
    "college",
    "university",
    "restaurant",
    "foodestablishment",
)

_INDUSTRY_LD_KEYS = frozenset(
    {
        "industry",
        "knowsabout",
        "occupationalcategory",
        "servicetype",
        "additionaltype",
        "category",
        "businesscategory",
        "jobtitle",
    }
)


def _env_skip_industry_taxonomy() -> bool:
    v = os.environ.get("COMPANY_B2B_SKIP_INDUSTRY_TAXONOMY", "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _iter_parsed_ld_json_objects(html: str):
    """Yield top-level JSON-LD objects (and ``@graph`` items) from ``html``."""
    for m in _LD_JSON_SCRIPT_RE.finditer(html or ""):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            for item in data:
                yield item
        elif isinstance(data, dict) and isinstance(data.get("@graph"), list):
            for item in data["@graph"]:
                yield item
        else:
            yield data


def _json_ld_type_tokens(d: Dict[str, Any]) -> List[str]:
    t = d.get("@type")
    if isinstance(t, str):
        return [t.lower()]
    if isinstance(t, list):
        return [str(x).lower() for x in t]
    return []


def _is_org_like_ld(d: Dict[str, Any]) -> bool:
    for ts in _json_ld_type_tokens(d):
        compact = ts.replace(" ", "").replace("-", "")
        for m in _ORG_LD_TYPE_MARKERS:
            if m in compact:
                return True
    return False


def _ld_value_to_strings(value: Any) -> List[str]:
    """Turn JSON-LD industry-like field values into plain strings (DefinedTerm, lists, etc.)."""
    out: List[str] = []
    if value is None:
        return out
    if isinstance(value, str):
        s = value.strip()
        if s:
            out.append(s)
        return out
    if isinstance(value, (int, float)):
        out.append(str(value))
        return out
    if isinstance(value, dict):
        for kk in ("name", "alternateName", "headline", "title", "description"):
            x = value.get(kk)
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
        return out
    if isinstance(value, list):
        for it in value:
            out.extend(_ld_value_to_strings(it))
    return out


def _append_industry_candidate(acc: List[str], seen: set[str], text: str, *, max_len: int = 220) -> None:
    t = re.sub(r"\s+", " ", (text or "")).strip()
    if len(t) < 3 or len(t) > max_len:
        return
    key = t.lower()
    if key in seen:
        return
    seen.add(key)
    acc.append(t)


def _walk_ld_collect_industry_candidates(obj: Any, acc: List[str], seen: set[str]) -> None:
    if isinstance(obj, dict):
        if _is_org_like_ld(obj):
            for k, v in obj.items():
                kl = str(k).lower().replace(" ", "").replace("-", "")
                if kl in _INDUSTRY_LD_KEYS or kl in ("subindustry", "sub_industry"):
                    for s in _ld_value_to_strings(v):
                        _append_industry_candidate(acc, seen, s)
                if kl == "description" and isinstance(v, str):
                    frag = v.strip().split(".")[0]
                    if 12 < len(frag) < 200:
                        _append_industry_candidate(acc, seen, frag)
                if kl == "additionalproperty":
                    props = v if isinstance(v, list) else [v]
                    for p in props:
                        if not isinstance(p, dict):
                            continue
                        nm = p.get("name")
                        val = p.get("value")
                        if (
                            isinstance(nm, str)
                            and isinstance(val, str)
                            and "industry" in nm.lower()
                            and val.strip()
                        ):
                            _append_industry_candidate(acc, seen, val.strip())
        for v in obj.values():
            _walk_ld_collect_industry_candidates(v, acc, seen)
    elif isinstance(obj, list):
        for it in obj:
            _walk_ld_collect_industry_candidates(it, acc, seen)


def _meta_industry_candidates(html: str, acc: List[str], seen: set[str]) -> None:
    for cre in (
        _META_ARTICLE_SECTION_RE,
        _META_OG_DESC_RE,
        _META_OG_DESC_RE_ALT,
        _META_DESC_RE,
        _META_DESC_RE_ALT,
    ):
        m = cre.search(html or "")
        if not m:
            continue
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        first = raw.split(".")[0].strip()
        if len(first) >= 12:
            _append_industry_candidate(acc, seen, first)
        if len(raw) >= 12 and first.lower() != raw.lower():
            _append_industry_candidate(acc, seen, raw[:200])
    mk = _META_KEYWORDS_RE.search(html or "")
    if mk:
        for part in (mk.group(1) or "").split(","):
            p = part.strip()
            if 3 < len(p) < 80:
                _append_industry_candidate(acc, seen, p)


def _industry_candidate_strings_from_html(html: str) -> List[str]:
    """Ordered raw hints: JSON-LD org fields, then meta tags."""
    acc: List[str] = []
    seen: set[str] = set()
    for data in _iter_parsed_ld_json_objects(html or ""):
        _walk_ld_collect_industry_candidates(data, acc, seen)
    _meta_industry_candidates(html or "", acc, seen)
    return acc


def _strip_industry_seeds(
    industry: Optional[str],
    sub_industry: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    i = (industry or "").strip() or None
    s = (sub_industry or "").strip() or None
    return i, s


# Longest keys in ``INDUSTRY_TAXONOMY`` are ~47 chars; allow slack for future keys.
_SUB_INDUSTRY_RAW_TEXT_MAX_LEN = 56


def _coerce_sub_industry_raw_value(s: Optional[str]) -> Optional[str]:
    """
    Reject meta/OG marketing sentences masquerading as ``sub_industry``.
    Keep short hints (taxonomy sub keys are short) or an exact case-insensitive taxonomy key.
    """
    if not s:
        return None
    t = str(s).strip()
    if not t:
        return None
    try:
        from industry_taxonomy import INDUSTRY_TAXONOMY

        for k in INDUSTRY_TAXONOMY:
            if k.lower() == t.lower():
                return k
    except ImportError:
        pass
    if len(t) <= _SUB_INDUSTRY_RAW_TEXT_MAX_LEN:
        return t
    return None


# Minimum ``SequenceMatcher`` ratio between scraped brief text and ``INDUSTRY_TAXONOMY`` ``definition``.
_TAXONOMY_DEFINITION_BRIEF_MIN_RATIO = 0.6
_TAXONOMY_DEFINITION_COMPARE_MAX_CHARS = 1200


def _pick_sub_industry_from_taxonomy_definition_similarity(
    brief: Optional[str],
    industry_hint: Optional[str],
    *,
    min_ratio: float = _TAXONOMY_DEFINITION_BRIEF_MIN_RATIO,
) -> Optional[str]:
    """
    Pick a taxonomy sub-industry key whose ``definition`` text best matches ``brief``
    (similarity ``>= min_ratio``). Delegates to
    ``lead_cache_json.industry_normalizer.pick_sub_industry_from_definition_similarity`` (full
    taxonomy). For scoped classification use
    :func:`classify_industry_sub_from_description_in_scope` in that module.
    """
    try:
        from lead_cache_json.industry_normalizer import pick_sub_industry_from_definition_similarity

        return pick_sub_industry_from_definition_similarity(
            brief,
            industry_hint,
            scope_sub_keys=None,
            min_ratio=min_ratio,
            min_brief_len=16,
            max_compare_chars=_TAXONOMY_DEFINITION_COMPARE_MAX_CHARS,
        )
    except ImportError:
        return None


def _merge_raw_industry_from_candidates(
    candidates: List[str],
    seed_industry: Optional[str],
    seed_sub_industry: Optional[str],
    *,
    assign_industry_from_candidates: bool = True,
    assign_sub_industry_from_candidates: bool = True,
) -> Tuple[Optional[str], Optional[str]]:
    """Fill ``industry`` / ``sub_industry`` from seeds first, then distinct page candidates (no taxonomy).

    ``normalize_industry_pair`` expects a parent-like industry and a sub-industry-like second field.
    Prioritized candidates list sub keys before parents; without slotting, the first string (often a
    taxonomy **sub** key) was stored as ``industry`` and the second as ``sub_industry``, which
    breaks pairing. We first assign exact taxonomy **parent** labels to the industry slot and exact
    **sub** keys to the sub slot, then fill any remaining gaps in crawl order.

    When ``assign_industry_from_candidates`` or ``assign_sub_industry_from_candidates`` is False,
    that slot is not filled from ``candidates`` (only the seed value, if any, applies).
    """
    si = (seed_industry or "").strip() or None
    ss = (seed_sub_industry or "").strip() or None
    if si and ss:
        return si, ss
    used: set[str] = set()
    if si:
        used.add(si.lower())
    if ss:
        used.add(ss.lower())

    subs_l: set[str] = set()
    parents_l: set[str] = set()
    try:
        from lead_cache_json.industry_normalizer import get_all_valid_industries
        from industry_taxonomy import INDUSTRY_TAXONOMY

        subs_l = {k.lower() for k in INDUSTRY_TAXONOMY}
        parents_l = {g.lower() for g in get_all_valid_industries()}
    except ImportError:
        pass

    if subs_l or parents_l:
        if assign_industry_from_candidates:
            for c in candidates:
                t = (c or "").strip()
                if not t:
                    continue
                tl = t.lower()
                if tl in used:
                    continue
                if si is None and tl in parents_l:
                    si = t
                    used.add(tl)
        if assign_sub_industry_from_candidates:
            for c in candidates:
                t = (c or "").strip()
                if not t:
                    continue
                tl = t.lower()
                if tl in used:
                    continue
                if ss is None and tl in subs_l:
                    ss = t
                    used.add(tl)

    for c in candidates:
        t = (c or "").strip()
        if not t:
            continue
        tl = t.lower()
        if tl in used:
            continue
        if assign_industry_from_candidates and si is None:
            si = t
            used.add(tl)
        elif assign_sub_industry_from_candidates and ss is None:
            ok_len = len(t) <= _SUB_INDUSTRY_RAW_TEXT_MAX_LEN
            if not ok_len and subs_l and tl in subs_l:
                ok_len = True
            if ok_len:
                ss = t
                used.add(tl)
        if si is not None and ss is not None:
            break
    return si, ss


def _refine_industry_sub_with_taxonomy(
    raw_industry: Optional[str],
    raw_sub_industry: Optional[str],
    candidates: List[str],
) -> Tuple[Optional[str], Optional[str]]:
    """
    Map merged raw strings to canonical ``(industry, sub_industry)`` allowed by
    ``industry_taxonomy.py`` (via ``normalize_industry_pair``). Tries the merged pair, then
    pairs built with each candidate string, then cross-pairs among top candidates.
    If still unresolved, ``refine_raw_industry_sub_taxonomy_fallback`` maps ``sub_industry``
    using the same taxonomy (parent-restricted match, then global fuzzy). Otherwise returns
    trimmed raw strings.
    """
    ri = (raw_industry or "").strip() or None
    rs = (raw_sub_industry or "").strip() or None
    if _env_skip_industry_taxonomy():
        return ri, rs
    try:
        from lead_cache_json.industry_normalizer import (
            normalize_industry_pair,
            refine_raw_industry_sub_taxonomy_fallback,
        )
    except ImportError:
        return ri, rs

    tried: set[Tuple[Optional[str], Optional[str]]] = set()

    def try_pair(a: Optional[str], b: Optional[str]) -> Optional[Tuple[str, str]]:
        if not a and not b:
            return None
        key = (a, b)
        if key in tried:
            return None
        tried.add(key)
        io, so, _reason = normalize_industry_pair(a, b)
        if io and so:
            return (io, so)
        return None

    hit = try_pair(ri, rs)
    if hit:
        return hit

    if rs:
        hit = try_pair(None, rs)
        if hit:
            return hit
    if ri:
        hit = try_pair(ri, None)
        if hit:
            return hit

    # ``normalize_industry_pair`` only returns a hit when *both* taxonomy slots resolve.
    # Merged (ri, rs) often puts a long meta line in the first slot and a real label second,
    # which fails pairing — so try each candidate alone as sub-industry or as industry-like
    # text *before* mixing with possibly junk merged fields.
    for c in candidates:
        c = (c or "").strip()
        if not c:
            continue
        hit = try_pair(None, c)
        if hit:
            return hit
        hit = try_pair(c, None)
        if hit:
            return hit

    si, ss = ri, rs
    for c in candidates:
        c = (c or "").strip()
        if not c:
            continue
        for pair in ((si, c), (c, ss), (None, c), (c, None)):
            hit = try_pair(pair[0], pair[1])
            if hit:
                return hit

    lim = min(len(candidates), 14)
    for i in range(lim):
        for j in range(i + 1, min(i + 8, lim)):
            hit = try_pair(candidates[i], candidates[j])
            if hit:
                return hit

    fi, fs = refine_raw_industry_sub_taxonomy_fallback(ri, rs)
    return fi, fs


def _refine_seeds_only(industry: Optional[str], sub_industry: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Taxonomy-refine optional call arguments when there are no page candidates."""
    ri, rs = _strip_industry_seeds(industry, sub_industry)
    return _refine_industry_sub_with_taxonomy(ri, rs, [])


def _prioritize_taxonomy_like_candidates(candidates: List[str]) -> List[str]:
    """Prefer strings that exactly match taxonomy sub-industry keys, then parent industries."""
    if not candidates:
        return []
    try:
        from lead_cache_json.industry_normalizer import get_all_valid_industries
        from industry_taxonomy import INDUSTRY_TAXONOMY
    except ImportError:
        return list(candidates)
    parents_lower = {g.lower() for g in get_all_valid_industries()}
    subs_lower = {k.lower() for k in INDUSTRY_TAXONOMY}

    def rank(c: str) -> Tuple[int, int]:
        cl = (c or "").strip().lower()
        if cl in subs_lower:
            return (0, len(c))
        if cl in parents_lower:
            return (1, len(c))
        return (2, len(c))

    return sorted(candidates, key=rank)


_HQ_LINE_RE = re.compile(
    r"(?:Headquarters|Head office|HQ|Located in|Based in)\s*[:\s]\s*"
    r"([A-Za-z0-9À-ÿ'’\-\s,.]{8,140}?)(?:\.|$|\||Follow|Employees|\d{4})",
    re.I,
)
_EMPLOYEE_RE = re.compile(
    r"(\d{1,3}(?:,\d{3})*)\s*[-–—]\s*(\d{1,3}(?:,\d{3})*)\s*employees?",
    re.I,
)
_EMPLOYEE_PLUS_RE = re.compile(r"(\d{1,3}(?:,\d{3})*)\+\s*employees?", re.I)
_SIMPLE_EMP_RANGE = re.compile(r"(\d+)\s*[-–—]\s*(\d+)\s*employees?", re.I)

# Canonical ``employee_count`` labels (LinkedIn-style ranges).
_EMPLOYEE_COUNT_CANONICAL: List[Tuple[str, int, Optional[int]]] = [
    ("0-1", 0, 1),
    ("2-10", 2, 10),
    ("11-50", 11, 50),
    ("51-200", 51, 200),
    ("201-500", 201, 500),
    ("501-1,000", 501, 1000),
    ("1,001-5,000", 1001, 5000),
    ("5,001-10,000", 5001, 10000),
    ("10,001+", 10001, None),
]


def _html_to_plain_text(html: str, max_len: int = 150_000) -> str:
    t = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html or "")
    t = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", t)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html_module.unescape(t)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:max_len]


def _is_united_states_country(country: str) -> bool:
    c = (country or "").strip().lower()
    return c in (
        "united states",
        "usa",
        "us",
        "u.s.",
        "u.s.a.",
        "america",
        "united states of america",
    )


# Canonical string for US HQ in API / ``to_dict()`` output (not ``USA`` / ``US``).
_HQ_COUNTRY_DISPLAY_UNITED_STATES = "United States"


def _display_hq_country(country: str) -> str:
    """Return ``United States`` for any recognized US alias; otherwise trimmed ``country``."""
    raw = (country or "").strip()
    if _is_united_states_country(raw):
        return _HQ_COUNTRY_DISPLAY_UNITED_STATES
    return raw


# USPS state / DC abbreviations → display name (used when ``hq_country`` is US).
_US_STATE_ABBREV_TO_NAME: Dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
    "PR": "Puerto Rico",
    "GU": "Guam",
    "VI": "U.S. Virgin Islands",
    "AS": "American Samoa",
    "MP": "Northern Mariana Islands",
}

_US_STATE_FULLNAMES_LOWER = frozenset(
    x.lower() for x in _US_STATE_ABBREV_TO_NAME.values()
)


def _looks_like_us_state(state: str) -> bool:
    """True if ``state`` is a known USPS code or full US state / DC / territory name."""
    s = (state or "").strip()
    if not s:
        return False
    compact = re.sub(r"[\s.]+", "", s)
    if len(compact) == 2 and compact.isalpha():
        if compact.upper() in _US_STATE_ABBREV_TO_NAME:
            return True
    return s.lower() in _US_STATE_FULLNAMES_LOWER


def _us_state_tokens_equivalent(a: str, b: str) -> bool:
    """True if two strings denote the same US state (abbrev vs full name allowed)."""
    if not (a or "").strip() or not (b or "").strip():
        return False
    ea = _expand_us_state_abbreviation(a.strip()).lower()
    eb = _expand_us_state_abbreviation(b.strip()).lower()
    return bool(ea) and ea == eb


def _infer_united_states_hq_if_us_state(
    country: Optional[str], state: Optional[str]
) -> Tuple[str, str]:
    """
    Many APIs omit ``country`` when ``state`` is a US code (e.g. ``NY``). Some put a US state
    name or abbrev in the **country** slot (e.g. ``California``). In those cases set country
    to ``United States`` and move that value into ``state``. Does not alter real non-US
    countries (e.g. ``Germany``). If country and state are two different US states, leaves
    the pair unchanged.
    """
    co = (country or "").strip()
    st = (state or "").strip()

    # Country field holds a US state/territory, not a country (e.g. LinkedIn: "California").
    if co and _looks_like_us_state(co) and not _is_united_states_country(co):
        if st and _looks_like_us_state(st) and not _us_state_tokens_equivalent(co, st):
            return co, st
        return _HQ_COUNTRY_DISPLAY_UNITED_STATES, co

    if not co and st and _looks_like_us_state(st):
        return _HQ_COUNTRY_DISPLAY_UNITED_STATES, st

    return co, st


def _expand_us_state_abbreviation(state: str) -> str:
    """
    If ``state`` is a 2-letter USPS code (optionally spaced or dotted, e.g. ``N.Y.``),
    return the full name; otherwise return the trimmed original string.
    """
    s = (state or "").strip()
    if not s:
        return ""
    compact = re.sub(r"[\s.]+", "", s)
    if len(compact) == 2 and compact.isalpha():
        full = _US_STATE_ABBREV_TO_NAME.get(compact.upper())
        if full:
            return full
    return s


def _strip_scalar(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, dict):
        n = x.get("name")
        if isinstance(n, str) and n.strip():
            return n.strip()
        return None
    s = str(x).strip()
    return s or None


def _normalize_schema_number_of_employees(ne: Any) -> Optional[str]:
    if ne is None or isinstance(ne, bool):
        return None
    if isinstance(ne, int):
        return str(ne)
    if isinstance(ne, str):
        t = ne.strip()
        return t or None
    if isinstance(ne, dict):
        mn, mx = ne.get("minValue"), ne.get("maxValue")
        if mn is not None and mx is not None:
            return f"{mn}-{mx}"
        v = ne.get("value")
        if v is not None:
            return str(v).strip() or None
    return None


def _walk_ld_employee_count(obj: Any) -> Optional[str]:
    if isinstance(obj, dict):
        ne = obj.get("numberOfEmployees")
        s = _normalize_schema_number_of_employees(ne)
        if s:
            return s
        for v in obj.values():
            hit = _walk_ld_employee_count(v)
            if hit:
                return hit
    elif isinstance(obj, list):
        for it in obj:
            hit = _walk_ld_employee_count(it)
            if hit:
                return hit
    return None


def _employee_count_from_json_ld(html: str) -> Optional[str]:
    for data in _iter_parsed_ld_json_objects(html or ""):
        hit = _walk_ld_employee_count(data)
        if hit:
            return hit
    return None


def _employee_count_from_plain_text(html: str) -> Optional[str]:
    blob = _html_to_plain_text(html or "")
    for rx in (_EMPLOYEE_RE, _SIMPLE_EMP_RANGE, _EMPLOYEE_PLUS_RE):
        m = rx.search(blob)
        if m:
            return m.group(0).replace(",", "").strip()
    return None


def _employee_count_linkedin_embed(html: str) -> Optional[str]:
    m = re.search(
        r'["\']employeeCountRange["\']\s*:\s*\{[^}]*["\']start["\']\s*:\s*(\d+)[^}]*["\']end["\']\s*:\s*(\d+)',
        html or "",
    )
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.search(r'["\']staffCount["\']\s*:\s*(\d+)', html or "", re.I)
    if m:
        return m.group(1)
    m = re.search(r"(\d{1,4}\s*[-–]\s*\d{1,4})\s*employees?", html or "", re.I)
    if m:
        return m.group(1).replace(" ", "")
    return None


def _extract_employee_count_from_page(html: Optional[str]) -> Optional[str]:
    if not html:
        return None
    for fn in (_employee_count_linkedin_embed, _employee_count_from_json_ld, _employee_count_from_plain_text):
        hit = fn(html)
        if hit:
            return hit
    return None


def _walk_ld_postal_address(obj: Any) -> List[Tuple[Optional[str], Optional[str], Optional[str]]]:
    out: List[Tuple[Optional[str], Optional[str], Optional[str]]] = []
    if isinstance(obj, dict):
        types = obj.get("@type")
        type_list = [types] if isinstance(types, str) else list(types) if isinstance(types, list) else []
        tl = " ".join(str(x).lower() for x in type_list).replace(" ", "")
        if "postaladdress" in tl:
            loc = _strip_scalar(obj.get("addressLocality"))
            reg = _strip_scalar(obj.get("addressRegion"))
            ctry = _strip_scalar(obj.get("addressCountry"))
            if loc or reg or ctry:
                out.append((loc, reg, ctry))
        for v in obj.values():
            out.extend(_walk_ld_postal_address(v))
    elif isinstance(obj, list):
        for it in obj:
            out.extend(_walk_ld_postal_address(it))
    return out


def _merge_hq_triple(
    a: Tuple[Optional[str], Optional[str], Optional[str]],
    b: Tuple[Optional[str], Optional[str], Optional[str]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    def pick(x: Optional[str], y: Optional[str]) -> Optional[str]:
        ys = (y or "").strip()
        if ys:
            return ys
        xs = (x or "").strip()
        return xs or None

    return (pick(a[0], b[0]), pick(a[1], b[1]), pick(a[2], b[2]))


def _hq_triple_from_json_ld(html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    best = (None, None, None)
    for data in _iter_parsed_ld_json_objects(html or ""):
        for trip in _walk_ld_postal_address(data):
            best = _merge_hq_triple(best, trip)
    return best


def _parse_hq_line(line: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    line = re.sub(r"\s+", " ", (line or "")).strip(" ,.-")
    parts = [p.strip() for p in line.split(",") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return parts[0], None, parts[1]
    if len(parts) == 1:
        return parts[0], None, None
    return None, None, None


def _hq_triple_from_plain_text(html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    blob = _html_to_plain_text(html or "")
    m = _HQ_LINE_RE.search(blob)
    if not m:
        return None, None, None
    return _parse_hq_line(m.group(1))


def _hq_from_page(html: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not html:
        return None, None, None
    t1 = _hq_triple_from_json_ld(html)
    t2 = _hq_triple_from_plain_text(html)
    return _merge_hq_triple(t1, t2)


def _merge_hq_pages(
    *pages: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    acc = (None, None, None)
    for p in pages:
        if not p:
            continue
        acc = _merge_hq_triple(acc, _hq_from_page(p))
    return acc


def _parse_employee_int_fragment(chunk: str) -> int:
    return int(re.sub(r"[^\d]", "", chunk) or "0")


def _strip_employee_count_noise(s: str) -> str:
    t = (s or "").strip()
    t = re.sub(
        r"\b(employees?|employee|staff(?:ing)?|people|fte|workers?|headcount|on\s+linkedin)\b",
        " ",
        t,
        flags=re.I,
    )
    t = re.sub(r"\s+", " ", t).strip(" ,.-—")
    return t


def _parse_employee_count_bounds(s: str) -> Optional[Tuple[int, Optional[int], bool]]:
    """
    Parse stripped-ish text into ``(low, high_inclusive, is_trailing_plus)``.
    ``is_trailing_plus`` is True for ``N+`` (open high); then ``high`` is None.
    """
    t = _strip_employee_count_noise(s)
    if not t:
        return None
    m = re.match(r"^(\d[\d,\s]*)\s*\+\s*$", t)
    if m:
        return (_parse_employee_int_fragment(m.group(1)), None, True)
    m = re.match(r"^(\d[\d,\s]*)\s*[-–—]\s*(\d[\d,\s]*)$", t)
    if m:
        a, b = _parse_employee_int_fragment(m.group(1)), _parse_employee_int_fragment(m.group(2))
        if a > b:
            a, b = b, a
        return (a, b, False)
    m = re.match(r"^(\d[\d,\s]*)$", t)
    if m:
        n = _parse_employee_int_fragment(m.group(1))
        return (n, n, False)
    return None


def _employee_count_bucket_for_point(n: int) -> str:
    for label, bmin, bmax in _EMPLOYEE_COUNT_CANONICAL:
        if n < bmin:
            continue
        if bmax is None:
            return label
        if n <= bmax:
            return label
    return "10,001+"


def _canonical_employee_count_label(raw: Optional[str]) -> Optional[str]:
    """
    Map free-text employee size strings to a canonical range label (no ``employees`` suffix).
    """
    if not raw or not str(raw).strip():
        return None
    parsed = _parse_employee_count_bounds(str(raw))
    if not parsed:
        noise = _strip_employee_count_noise(str(raw))
        m = re.search(r"\d[\d,\s]*", noise)
        if not m:
            return None
        return _employee_count_bucket_for_point(_parse_employee_int_fragment(m.group(0)))
    lo, hi, is_plus = parsed
    if is_plus:
        if lo >= 10001:
            return "10,001+"
        return _employee_count_bucket_for_point(lo)
    if hi is None:
        return _employee_count_bucket_for_point(lo)
    for label, bmin, bmax in _EMPLOYEE_COUNT_CANONICAL:
        if bmax is None:
            continue
        if lo == bmin and hi == bmax:
            return label
    mid = (lo + hi) // 2
    return _employee_count_bucket_for_point(mid)


def _pick_employee_count(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        if v and str(v).strip():
            canon = _canonical_employee_count_label(str(v).strip())
            if canon:
                return canon
    return None


_ABOUT_PATH_HINTS = (
    "/about",
    "/about-us",
    "/about_us",
    "/aboutus",
    "/company",
    "/our-story",
    "/who-we-are",
    "/whoweare",
    "/about-company",
    "/company/about",
    "/en/about",
    "/us/about",
)


def _normalize_domain(domain: str) -> str:
    """Lowercase host; strip path, ``www.``, and IPv4 ``:port`` (not IPv6 literals)."""
    s = (domain or "").strip()
    if not s:
        return ""
    if "://" in s:
        host = urlparse(s).netloc or urlparse(s).path.split("/")[0]
    else:
        host = s.split("/")[0].split("?")[0].split("#")[0]
    host = host.strip()
    if host.startswith("["):
        return host.lower()
    if ":" in host:
        host = host.rsplit(":", 1)[0]
    if host.lower().startswith("www."):
        host = host[4:]
    return host.lower().strip()


def _homepage_candidates(root_domain: str, preferred_url: Optional[str] = None) -> List[str]:
    if not root_domain:
        return []
    out: List[str] = []
    seen: set[str] = set()

    def push(url: Optional[str]) -> None:
        u = _absolute_http_url(str(url or ""))
        if not u:
            return
        if not _homepage_url_allowed_for_root(u, root_domain):
            return
        key = u.rstrip("/").lower()
        if key in seen:
            return
        seen.add(key)
        out.append(u)

    push(preferred_url)
    push(f"https://www.{root_domain}/")
    push(f"https://{root_domain}/")
    return out


def _is_blocked_corporate_lookup_host(host: str) -> bool:
    h = (host or "").lower()
    if not h:
        return True
    for b in _DOMAIN_CORRECTION_HOST_BLOCKLIST:
        if h == b or h.endswith("." + b):
            return True
    return False


def _host_from_http_url(url: str) -> Optional[str]:
    """Hostname from a URL string, without port or ``www.`` (uses ``urlparse().hostname``)."""
    if not url or not isinstance(url, str):
        return None
    u = url.strip()
    if not u:
        return None
    if not u.startswith(("http://", "https://", "//")):
        u = "https://" + u
    elif u.startswith("//"):
        u = "https:" + u
    try:
        p = urlparse(u)
        h = p.hostname
        if not h:
            return None
        out = _normalize_domain(h)
        return out if out and "." in out else None
    except Exception:
        return None


def _google_organic_results(
    api_key: str,
    query: str,
    *,
    timeout: float,
    country: str = "us",
    results: int = 10,
    page: int = 0,
) -> List[Dict[str, Any]]:
    """
    Fetch Google organic results as JSON via Scrapingdog's Google Search Scraper API.

    Forces the light (cheaper) tier: ``advance_search`` and ``mob_search`` stay false so each
    successful call stays on the ~5-credit parsed-JSON plan per Scrapingdog documentation.
    Does not set ``html=true`` (full SERP HTML is a different use case and not needed here).
    """
    try:
        r = requests.get(
            SCRAPINGDOG_GOOGLE_URL,
            params={
                "api_key": api_key,
                "query": query,
                "country": country,
                "results": str(min(max(results, 1), 20)),
                "page": str(max(page, 0)),
                "advance_search": "false",
                "mob_search": "false",
            },
            timeout=timeout,
        )
    except requests.RequestException:
        return []
    if r.status_code != 200 or not (r.text or "").strip():
        return []
    try:
        data = r.json()
    except json.JSONDecodeError:
        return []
    org = data.get("organic_results")
    return list(org) if isinstance(org, list) else []


def _linkedin_company_url_from_google_search(
    api_key: str,
    *,
    company_name: Optional[str],
    domain_root: str,
    timeout: float,
    country: str,
) -> Optional[str]:
    """
    Resolve ``linkedin.com/company/...`` via Scrapingdog Google Search organic results.

    Tries targeted queries first (``site:linkedin.com/company`` + quoted name, then name + LinkedIn),
    then a domain-apex hint when the name is missing. Does not scrape the company website for this step.
    """
    if not (api_key or "").strip():
        return None
    name = (company_name or "").strip()
    root = (domain_root or "").strip().lower()
    apex = root.split(".")[0] if root else ""

    queries: List[str] = []
    if len(name) > 1:
        safe = re.sub(r"\s+", " ", name.replace('"', " ")).strip()
        queries.append(f'site:linkedin.com/company "{safe}"')
        queries.append(f'"{safe}" LinkedIn')
        queries.append(f"{safe} LinkedIn company")
    if len(apex) > 2:
        if not name or apex.lower() not in name.lower():
            queries.append(f"site:linkedin.com/company {apex}")

    seen_q: set[str] = set()
    for q in queries:
        qn = (q or "").strip()
        if not qn:
            continue
        kl = qn.lower()
        if kl in seen_q:
            continue
        seen_q.add(kl)
        items = _google_organic_results(
            api_key, qn, timeout=timeout, country=country, results=10
        )
        for it in items:
            if not isinstance(it, dict):
                continue
            link = it.get("link")
            if isinstance(link, str) and link.strip():
                li = _normalize_linkedin(link.strip())
                if li:
                    return li
            title = it.get("title")
            if isinstance(title, str) and "linkedin.com/company/" in title.lower():
                li = _normalize_linkedin(title.strip())
                if li:
                    return li
    return None


_MIN_SCRAPED_DESCRIPTION_LEN = 16


def _snippet_from_google_organic_item(item: Dict[str, Any]) -> str:
    """Plain snippet text from one organic result dict (field names vary by API version)."""
    sn = item.get("snippet")
    if isinstance(sn, str) and sn.strip():
        return sn.strip()
    sn = item.get("description")
    if isinstance(sn, str) and sn.strip():
        return sn.strip()
    about = item.get("about_this_result")
    if isinstance(about, dict):
        t = about.get("snippet") or about.get("source")
        if isinstance(t, str) and t.strip():
            return t.strip()
    return ""


def _description_from_google_search_scraper(
    api_key: str,
    root: str,
    *,
    business_name: Optional[str],
    timeout: float,
    country: str,
    max_len: int = 2000,
) -> Optional[str]:
    """
    Fill ``description`` from Scrapingdog **Google Search Scraper API** organic snippets
    when on-page meta/JSON-LD did not yield text.
    """
    if not (api_key or "").strip() or not root:
        return None
    bn = (business_name or "").strip()
    # Fewer queries = fewer billed Google Search calls. Try broad match first, then site-limited,
    # then name+domain only if still empty.
    queries: List[str] = [
        f"{root} company",
        f"site:{root} about",
    ]
    if bn and len(bn) > 2:
        queries.append(f"{bn} {root}")
    seen_lower: set[str] = set()
    for q in queries:
        qn = (q or "").strip()
        if not qn:
            continue
        items = _google_organic_results(
            api_key, qn, timeout=timeout, country=country, results=10
        )
        for it in items:
            raw = _snippet_from_google_organic_item(it) if isinstance(it, dict) else ""
            if not raw:
                continue
            t = html_module.unescape(re.sub(r"\s+", " ", raw.strip()))
            if len(t) < _MIN_SCRAPED_DESCRIPTION_LEN:
                continue
            low = t.lower()
            if low in seen_lower:
                continue
            seen_lower.add(low)
            if len(t) > max_len:
                t = t[:max_len]
            return t
    return None


def _string_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _registrable_tail(host: str) -> str:
    """Last two DNS labels (``co.uk`` / ``com.br`` are imperfect; good enough for typo correction)."""
    parts = (host or "").lower().strip().split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return parts[0] if parts else ""


def _domain_correction_match_score(wrong_root: str, candidate_host: str) -> float:
    """
    Score how likely ``candidate_host`` is a typo/variant of ``wrong_root`` (normalized hosts).
    Uses full-string, apex-label, registrable-tail, and de-dotted string similarity.
    """
    wr = (wrong_root or "").strip().lower()
    h = (candidate_host or "").strip().lower()
    if not wr or not h or wr == h:
        return 0.0
    apex_w = wr.split(".")[0]
    apex_h = h.split(".")[0]
    sim_full = _string_similarity(wr, h)
    sim_apex = _string_similarity(apex_w, apex_h)
    sim_reg = _string_similarity(_registrable_tail(wr), _registrable_tail(h))
    sim_dotless = _string_similarity(wr.replace(".", ""), h.replace(".", ""))
    base = max(sim_full, sim_apex, sim_reg, sim_dotless)
    if _registrable_tail(wr) == _registrable_tail(h) and wr != h and apex_w != apex_h:
        return min(base, _MIN_DOMAIN_CORRECTION_SCORE - 0.05)
    return base


_MIN_DOMAIN_CORRECTION_SCORE = 0.38
_MIN_DOMAIN_CORRECTION_SCORE_RELAXED = 0.18
_DOMAIN_CORRECTION_RELAXED_MAX_RANK = 5


def _hosts_from_google_organic_item(item: Dict[str, Any]) -> List[str]:
    """
    Hostnames from one Google organic dict (field names vary by Scrapingdog/API version).
    Order: primary ``link``, then ``displayed_link`` / ``displayedLink`` breadcrumb, then
    ``cite`` / ``domain`` when they look like hosts.
    """
    found: List[str] = []
    seen: set[str] = set()

    def push(raw: Optional[str]) -> None:
        if not raw or not isinstance(raw, str):
            return
        u = raw.strip()
        if not u:
            return
        h = _host_from_http_url(u)
        if h and h not in seen:
            seen.add(h)
            found.append(h)

    push(item.get("link") or "")
    disp = item.get("displayed_link") or item.get("displayedLink") or ""
    if isinstance(disp, str) and disp.strip():
        d = disp.strip()
        head = re.split(r"\s*(?:›|>|\u203a)\s*", d, maxsplit=1)[0].strip()
        head = re.split(r"\s+[–—\-]\s+", head, maxsplit=1)[0].strip()
        if head:
            if "://" not in head and "." in head and not re.search(r"\s", head):
                push("https://" + head.lstrip("/"))
            else:
                push(head)
    cite = item.get("cite") or item.get("domain") or ""
    if isinstance(cite, str) and cite.strip():
        c = cite.strip()
        if "://" not in c and "." in c and not re.search(r"\s", c):
            push("https://" + c.lstrip("/"))
    return found


def _pick_corrected_domain_via_google(
    api_key: str,
    wrong_root: str,
    *,
    timeout: float,
    country: str,
) -> Optional[str]:
    """
    Resolve a likely typo/wrong host using Scrapingdog's Google Search API (costs API credits).
    Tries ``{domain} official website`` then a brand-style query from the apex label.
    """
    if not wrong_root or not api_key.strip():
        return None
    wrong_n = _normalize_domain(wrong_root)
    if not wrong_n or "." not in wrong_n:
        return None
    apex = wrong_n.split(".")[0]
    if len(apex) < 2:
        return None
    brand_q = apex.replace("-", " ").strip()
    primary_q = f"{wrong_n} official website"
    queries = [
        primary_q,
        f"{brand_q} official website" if brand_q else "",
    ]
    best_host: Optional[str] = None
    best_score = 0.0
    primary_items: List[Dict[str, Any]] = []
    for q in queries:
        qn = (q or "").strip()
        if not qn:
            continue
        items = _google_organic_results(api_key, qn, timeout=timeout, country=country)
        if qn == primary_q and items:
            primary_items = [x for x in items if isinstance(x, dict)]
        for item in items:
            if not isinstance(item, dict):
                continue
            for host in _hosts_from_google_organic_item(item):
                if not host or host == wrong_n:
                    continue
                if _is_blocked_corporate_lookup_host(host):
                    continue
                score = _domain_correction_match_score(wrong_n, host)
                if score > best_score:
                    best_score = score
                    best_host = host
    if best_host and best_score >= _MIN_DOMAIN_CORRECTION_SCORE:
        return best_host
    for item in primary_items[:_DOMAIN_CORRECTION_RELAXED_MAX_RANK]:
        for host in _hosts_from_google_organic_item(item):
            if not host or host == wrong_n:
                continue
            if _is_blocked_corporate_lookup_host(host):
                continue
            score = _domain_correction_match_score(wrong_n, host)
            if score >= _MIN_DOMAIN_CORRECTION_SCORE_RELAXED:
                return host
    return None


def _extract_canonical_or_og_url(html: str) -> Optional[str]:
    """First absolute ``https?`` URL from ``rel=canonical`` or ``og:url``."""
    if not (html or "").strip():
        return None
    for cre in (_CANONICAL_LINK_RE, _CANONICAL_LINK_RE_ALT, _OG_URL_RE, _OG_URL_RE_ALT):
        m = cre.search(html)
        if not m:
            continue
        u = html_module.unescape((m.group(1) or "").strip())
        if not u:
            continue
        u = u.split("#")[0].strip()
        if u.startswith(("http://", "https://")):
            return u
    return None


def _homepage_url_allowed_for_root(page_url: str, root: str) -> bool:
    """True if ``page_url`` host matches normalized ``root`` (same company site, not a redirect off-domain)."""
    host = _host_from_http_url(page_url)
    if not host:
        return False
    return host == _normalize_domain(root)


def _fetch_homepage_html(
    api_key: str,
    root: str,
    *,
    timeout: float,
    dynamic: bool,
    preferred_url: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Return ``(company_website_url, html)`` for the first working homepage candidate, then
    optionally re-scrape **canonical** or **og:url** when it points to the same host (exact URL).
    """
    for candidate in _homepage_candidates(root, preferred_url=preferred_url):
        html = _scrape_html(api_key, candidate, timeout=timeout, dynamic=dynamic)
        if not html:
            continue
        pref = _extract_canonical_or_og_url(html)
        if (
            pref
            and _homepage_url_allowed_for_root(pref, root)
            and pref.rstrip("/") != candidate.rstrip("/")
        ):
            html2 = _scrape_html(api_key, pref, timeout=timeout, dynamic=dynamic)
            if html2:
                return pref, html2
        return candidate, html
    return None, None


def _scrape_html(api_key: str, target_url: str, *, timeout: float, dynamic: bool) -> Optional[str]:
    try:
        r = requests.get(
            SCRAPINGDOG_SCRAPE_URL,
            params={
                "api_key": api_key,
                "url": target_url,
                "dynamic": "true" if dynamic else "false",
            },
            timeout=timeout,
        )
    except requests.RequestException:
        return None
    if r.status_code != 200 or not (r.text or "").strip():
        return None
    return r.text


def _walk_ld_org_description(obj: Any) -> Optional[str]:
    """First Organization-like JSON-LD ``description`` string, depth-first."""
    if isinstance(obj, dict):
        t_raw = obj.get("@type")
        if isinstance(t_raw, str):
            blob = t_raw.lower()
        elif isinstance(t_raw, list):
            blob = " ".join(str(x).lower() for x in t_raw)
        else:
            blob = ""
        orgish = any(m in blob for m in _ORG_LD_TYPE_MARKERS)
        if orgish:
            d = obj.get("description")
            if isinstance(d, str) and len(d.strip()) >= _MIN_SCRAPED_DESCRIPTION_LEN:
                return d.strip()
        for v in obj.values():
            hit = _walk_ld_org_description(v)
            if hit:
                return hit
    elif isinstance(obj, list):
        for it in obj:
            hit = _walk_ld_org_description(it)
            if hit:
                return hit
    return None


def _extract_description_from_html(html: Optional[str], *, max_len: int = 2000) -> Optional[str]:
    """Brief company description: og:description / meta description, then JSON-LD org description."""
    if not html:
        return None
    for cre in (_META_OG_DESC_RE, _META_OG_DESC_RE_ALT, _META_DESC_RE, _META_DESC_RE_ALT):
        m = cre.search(html)
        if m:
            t = html_module.unescape((m.group(1) or "").strip())
            t = re.sub(r"\s+", " ", t)
            if len(t) >= _MIN_SCRAPED_DESCRIPTION_LEN:
                return t[:max_len] if len(t) > max_len else t
    for data in _iter_parsed_ld_json_objects(html):
        d = _walk_ld_org_description(data)
        if d:
            t = html_module.unescape(re.sub(r"\s+", " ", d.strip()))
            if len(t) >= _MIN_SCRAPED_DESCRIPTION_LEN:
                return t[:max_len] if len(t) > max_len else t
    return None


def _pick_description_from_pages(*pages: Optional[str], max_len: int = 2000) -> Optional[str]:
    """Prefer about page, then homepage, then LinkedIn."""
    for p in pages:
        hit = _extract_description_from_html(p, max_len=max_len)
        if hit:
            return hit
    return None


def _extract_business_name(html: str) -> Optional[str]:
    for pattern in (_OG_SITE_NAME_RE, _OG_SITE_NAME_RE_ALT):
        m = pattern.search(html)
        if m:
            name = m.group(1).strip()
            if name:
                return name
    m = _APP_NAME_RE.search(html)
    if m:
        name = m.group(1).strip()
        if name:
            return name
    m = _TITLE_RE.search(html)
    if m:
        name = re.sub(r"\s+", " ", m.group(1)).strip()
        # Drop common suffix noise
        name = re.split(r"\s*[|\u2013\u2014-]\s*", name)[0].strip()
        if name and len(name) > 1:
            return name
    return None


def _absolute_links(base_url: str, html: str) -> List[str]:
    out: List[str] = []
    for m in _HREF_RE.finditer(html):
        href = (m.group(1) or "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        abs_url = urljoin(base_url, href)
        out.append(abs_url)
    return out


def _normalize_linkedin(url: str) -> Optional[str]:
    low = url.lower()
    if "linkedin.com/company/" not in low:
        return None
    m = re.search(r"https?://(?:[a-z]{2,3}\.)?linkedin\.com/(company/[^?\s\"'#]+)", url, re.I)
    if m:
        return "https://www.linkedin.com/" + m.group(1).rstrip("/")
    m = re.search(r"linkedin\.com/(company/[^?\s\"'#]+)", url, re.I)
    if m:
        return "https://www.linkedin.com/" + m.group(1).rstrip("/")
    return None


def _normalize_profile_employee_value(v: Any) -> Optional[str]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        s = v.strip()
        return s or None
    if isinstance(v, dict):
        st = v.get("start") or v.get("min") or v.get("minValue")
        en = v.get("end") or v.get("max") or v.get("maxValue")
        if st is not None and en is not None:
            return f"{st}-{en}"
        for k in ("text", "label", "value", "name"):
            t = v.get(k)
            if isinstance(t, str) and len(t.strip()) > 2:
                return t.strip()
    return None


_EMP_COUNT_JSON_KEYS = frozenset(
    {
        "staffcount",
        "staff_count",
        "employeecount",
        "employee_count",
        "companysize",
        "company_size",
        "numberofemployees",
        "staffcountrange",
        "employeecountrange",
        "size",
        "employeerange",
        "membercount",
        "member_count",
        "localizedstaffcount",
        "localized_staff_count",
    }
)


def _json_key_looks_like_employee_count(key: str) -> bool:
    kl = str(key).replace("-", "_").lower()
    if kl in _EMP_COUNT_JSON_KEYS:
        return True
    if "employee" in kl and ("count" in kl or "size" in kl or "range" in kl):
        return True
    if "staff" in kl and ("count" in kl or "range" in kl or "size" in kl):
        return True
    if "member" in kl and "count" in kl:
        return True
    return False


def _employee_count_from_linkedin_profile_data(obj: Any, depth: int = 0) -> Optional[str]:
    if depth > 14 or obj is None:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            if _json_key_looks_like_employee_count(k):
                hit = _normalize_profile_employee_value(v)
                if hit:
                    return hit
        for v in obj.values():
            hit = _employee_count_from_linkedin_profile_data(v, depth + 1)
            if hit:
                return hit
    elif isinstance(obj, list):
        for it in obj:
            hit = _employee_count_from_linkedin_profile_data(it, depth + 1)
            if hit:
                return hit
    return None


def _hq_trip_from_address_like_dict(v: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Map LinkedIn-style location objects to (city, state, country)."""
    if not isinstance(v, dict):
        return None, None, None
    city = _strip_scalar(
        v.get("city")
        or v.get("cityName")
        or v.get("addressLocality")
        or v.get("locality")
    )
    state = _strip_scalar(
        v.get("state")
        or v.get("addressRegion")
        or v.get("region")
    )
    country = _strip_scalar(
        v.get("country")
        or v.get("countryName")
        or v.get("addressCountry")
        or v.get("countryCode")
    )
    ga = v.get("geographicArea")
    if isinstance(ga, str) and ga.strip():
        g = ga.strip()
        if not city and "," in g:
            parts = [p.strip() for p in g.split(",") if p.strip()]
            if len(parts) >= 2:
                city = city or parts[0]
                state = state or parts[-1]
            elif len(parts) == 1:
                city = city or parts[0]
        elif not city:
            city = g
    if city or state or country:
        return city, state, country
    return None, None, None


def _hq_triple_from_linkedin_profile_data(
    obj: Any, depth: int = 0
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if depth > 14 or obj is None:
        return None, None, None
    best: Tuple[Optional[str], Optional[str], Optional[str]] = (None, None, None)
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower().replace("-", "")
            if kl in (
                "headquarters",
                "headquarter",
                "headoffice",
                "hq",
                "primarylocation",
                "mainoffice",
                "office",
            ):
                if isinstance(v, str) and len(v.strip()) >= 4:
                    trip = _parse_hq_line(v)
                    best = _merge_hq_triple(best, trip)
                elif isinstance(v, dict):
                    trip = _hq_trip_from_address_like_dict(v)
                    best = _merge_hq_triple(best, trip)
            elif kl in (
                "address",
                "headquartersaddress",
                "location",
                "hqlocation",
                "registeredaddress",
            ) and isinstance(v, dict):
                trip = _hq_trip_from_address_like_dict(v)
                best = _merge_hq_triple(best, trip)
            elif kl in ("locations", "offices", "officelocations") and isinstance(v, list):
                for loc in v[:6]:
                    if isinstance(loc, dict):
                        trip = _hq_trip_from_address_like_dict(loc)
                        if any(trip):
                            best = _merge_hq_triple(best, trip)
                            break
        for v in obj.values():
            sub = _hq_triple_from_linkedin_profile_data(v, depth + 1)
            best = _merge_hq_triple(best, sub)
    elif isinstance(obj, list):
        for it in obj:
            sub = _hq_triple_from_linkedin_profile_data(it, depth + 1)
            best = _merge_hq_triple(best, sub)
    return best


def _linkedin_company_slug_from_url(url: str) -> Optional[str]:
    if not (url or "").strip():
        return None
    m = re.search(r"linkedin\.com/company/([^/?#]+)", url, re.I)
    if not m:
        return None
    s = unquote(m.group(1)).strip()
    return s or None


def _resolve_linkedin_scraper_api_key(explicit: Optional[str]) -> str:
    s = (explicit or "").strip()
    if s:
        return s
    return get_scrape_api_key()


def _linkedin_profile_json_from_response(raw: Any) -> Optional[Dict[str, Any]]:
    """Normalize Scrapingdog response: list root, or dict, else None."""
    if raw is None:
        return None
    if isinstance(raw, list):
        if not raw:
            return None
        if isinstance(raw[0], dict):
            return raw[0]
        return None
    if isinstance(raw, dict):
        return raw
    return None


def _linkedin_profile_data_roots(data: Any, depth: int = 0) -> List[Dict[str, Any]]:
    """Collect dict nodes to search (root + common wrappers like ``data`` / ``company``)."""
    if depth > 10:
        return []
    if isinstance(data, list):
        out: List[Dict[str, Any]] = []
        for it in data[:12]:
            out.extend(_linkedin_profile_data_roots(it, depth + 1))
        return out
    if not isinstance(data, dict):
        return []
    out: List[Dict[str, Any]] = [data]
    for k in (
        "data",
        "company",
        "result",
        "body",
        "profile",
        "response",
        "items",
        "elements",
        "included",
    ):
        v = data.get(k)
        if isinstance(v, dict):
            out.extend(_linkedin_profile_data_roots(v, depth + 1))
        elif isinstance(v, list):
            for it in v[:12]:
                if isinstance(it, dict):
                    out.extend(_linkedin_profile_data_roots(it, depth + 1))
    return out


def _linkedin_scalar_industry_value(v: Any) -> Optional[str]:
    if isinstance(v, str):
        t = v.strip()
        return t if len(t) >= 2 else None
    if isinstance(v, dict):
        for kk in ("name", "localizedName", "label", "text", "value", "title"):
            x = v.get(kk)
            if isinstance(x, str) and len(x.strip()) >= 2:
                return x.strip()
    return None


def _walk_linkedin_industry_strings(obj: Any, depth: int = 0) -> List[str]:
    """Collect industry-like strings from Scrapingdog LinkedIn company JSON (nested)."""
    out: List[str] = []
    if depth > 14 or obj is None:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower().replace("-", "").replace("_", "")
            if "specialt" in kl and isinstance(v, list):
                for it in v:
                    s = _linkedin_scalar_industry_value(it)
                    if s:
                        out.append(s)
            elif "industry" in kl or kl == "sector" or "vertical" in kl:
                if isinstance(v, list):
                    for it in v:
                        s = _linkedin_scalar_industry_value(it)
                        if s:
                            out.append(s)
                else:
                    s = _linkedin_scalar_industry_value(v)
                    if s:
                        out.append(s)
        for v in obj.values():
            out.extend(_walk_linkedin_industry_strings(v, depth + 1))
    elif isinstance(obj, list):
        for it in obj[:48]:
            out.extend(_walk_linkedin_industry_strings(it, depth + 1))
    return out


def _linkedin_profile_api_industry_candidates(*sources: Optional[Dict[str, Any]]) -> List[str]:
    """Deduplicated industry strings from one or two LinkedIn API JSON payloads."""
    seen: set[str] = set()
    acc: List[str] = []
    for data in sources:
        if not data:
            continue
        for s in _walk_linkedin_industry_strings(data):
            _append_industry_candidate(acc, seen, s)
    return acc


def _get_linkedin_company_profile_request(
    base_url: str,
    linkedin_api_key: str,
    company_id: str,
    *,
    id_param: str,
    timeout: float,
) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(
            base_url,
            params={
                "api_key": linkedin_api_key.strip(),
                "type": "company",
                id_param: company_id.strip(),
            },
            timeout=timeout,
        )
    except requests.RequestException:
        return None
    if r.status_code == 202:
        return None
    if r.status_code != 200 or not (r.text or "").strip():
        return None
    try:
        raw = r.json()
    except json.JSONDecodeError:
        return None
    return _linkedin_profile_json_from_response(raw)


def _fetch_scrapingdog_linkedin_company_profile(
    linkedin_api_key: str,
    company_id: str,
    *,
    timeout: float,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Scrapingdog LinkedIn company JSON: ``/profile?id=`` and ``/linkedin?linkId=`` (both tried).

    Returns ``(profile_json, linkid_json)`` — either may be ``None``. Industry strings may appear
    in only one response; merge with ``_linkedin_profile_api_industry_candidates``.
    """
    if not (linkedin_api_key or "").strip() or not (company_id or "").strip():
        return None, None
    key = linkedin_api_key.strip()
    cid = company_id.strip()
    data = _get_linkedin_company_profile_request(
        SCRAPINGDOG_LINKEDIN_PROFILE_URL,
        key,
        cid,
        id_param="id",
        timeout=timeout,
    )
    alt = _get_linkedin_company_profile_request(
        SCRAPINGDOG_LINKEDIN_LINKID_URL,
        key,
        cid,
        id_param="linkId",
        timeout=timeout,
    )
    return data, alt


def _linkedin_profile_api_employee_and_hq(
    data: Optional[Dict[str, Any]],
) -> Tuple[Optional[str], Tuple[Optional[str], Optional[str], Optional[str]]]:
    if data is None:
        return None, (None, None, None)
    roots = _linkedin_profile_data_roots(data)
    seen_ids: set[int] = set()
    best_ec: Optional[str] = None
    best_hq: Tuple[Optional[str], Optional[str], Optional[str]] = (None, None, None)
    for root in roots:
        rid = id(root)
        if rid in seen_ids:
            continue
        seen_ids.add(rid)
        ec = _employee_count_from_linkedin_profile_data(root)
        hq = _hq_triple_from_linkedin_profile_data(root)
        if ec and not best_ec:
            best_ec = ec
        if any(hq):
            best_hq = _merge_hq_triple(best_hq, hq)
    return best_ec, best_hq


def _normalize_social_twitter(url: str) -> Optional[str]:
    low = url.lower()
    if "/intent/" in low or "/share" in low:
        return None
    if "twitter.com/" not in low and "x.com/" not in low:
        return None
    m = re.search(r"https?://(?:www\.)?(?:twitter|x)\.com/([^/?\s\"'#]+)", url, re.I)
    if not m or m.group(1).lower() in ("share", "intent", "home", "search", "i"):
        return None
    full = re.search(r"https?://(?:www\.)?(?:twitter|x)\.com/[^?\s\"'#]+", url, re.I)
    return full.group(0).split("?")[0] if full else None


def _normalize_social_facebook(url: str) -> Optional[str]:
    low = url.lower()
    if "facebook.com/" not in low:
        return None
    if "/sharer" in low or "/share.php" in low:
        return None
    m = re.search(r"https?://(?:www\.)?facebook\.com/[^?\s\"']+", url, re.I)
    return m.group(0).rstrip("/") if m else None


def _normalize_social_instagram(url: str) -> Optional[str]:
    low = url.lower()
    if "instagram.com/" not in low:
        return None
    m = re.search(r"https?://(?:www\.)?instagram\.com/[^?\s\"']+", url, re.I)
    return m.group(0).rstrip("/") if m else None


def _pick_about_url(root_domain: str, base_url: str, links: List[str]) -> Optional[str]:
    host_root = root_domain.lower()
    best: Optional[Tuple[int, str]] = None

    def host_matches(u: str) -> bool:
        h = urlparse(u).netloc.lower()
        if not h:
            return False
        if h.startswith("www."):
            h = h[4:]
        return h == host_root or h.endswith("." + host_root)

    for link in links:
        if not host_matches(link):
            continue
        path = urlparse(link).path.lower().rstrip("/") or "/"
        path_full = path + "/"
        score = 0
        for hint in _ABOUT_PATH_HINTS:
            h = hint.rstrip("/")
            if path == h or path_full.startswith(h + "/") or path.endswith(h):
                score = max(score, 100 - len(hint))
        if "/about" in path and score == 0:
            score = 50
        if score > 0 and (best is None or score > best[0]):
            best = (score, link.split("#")[0].rstrip("/"))

    return best[1] if best else None


def _merge_social(acc: Dict[str, str], links: List[str]) -> None:
    for u in links:
        tw = _normalize_social_twitter(u)
        if tw and not acc.get("Twitter"):
            acc["Twitter"] = tw
        fb = _normalize_social_facebook(u)
        if fb and not acc.get("Facebook"):
            acc["Facebook"] = fb
        ig = _normalize_social_instagram(u)
        if ig and not acc.get("Instagram"):
            acc["Instagram"] = ig


def _merge_social_from_raw_html(acc: Dict[str, str], html: str) -> None:
    """Fallback social extraction when URLs appear outside normal anchor hrefs."""
    if not html:
        return
    pats = (
        re.compile(r'https?://(?:www\.)?(?:twitter|x)\.com/[^?\s"\'<>]+', re.I),
        re.compile(r'https?://(?:www\.)?facebook\.com/[^?\s"\'<>]+', re.I),
        re.compile(r'https?://(?:www\.)?instagram\.com/[^?\s"\'<>]+', re.I),
        re.compile(r'//(?:www\.)?(?:twitter|x)\.com/[^?\s"\'<>]+', re.I),
        re.compile(r'//(?:www\.)?facebook\.com/[^?\s"\'<>]+', re.I),
        re.compile(r'//(?:www\.)?instagram\.com/[^?\s"\'<>]+', re.I),
    )
    urls: List[str] = []
    for cre in pats:
        for m in cre.finditer(html):
            raw = (m.group(0) or "").strip()
            if raw:
                urls.append("https:" + raw if raw.startswith("//") else raw)
    if urls:
        _merge_social(acc, urls)


def _merge_linkedin(acc: Optional[str], links: List[str]) -> Optional[str]:
    for u in links:
        li = _normalize_linkedin(u)
        if li:
            return li
    return acc


# LinkedIn often appears in JSON-LD sameAs, inline config, or data-* — not only <a href>.
_LINKEDIN_COMPANY_IN_HTML_RES = (
    re.compile(
        r'https?://(?:[a-z]{2,3}\.)?(?:www\.)?linkedin\.com/company/[^?\s"\'<>]+',
        re.I,
    ),
    re.compile(
        r'//(?:[a-z]{2,3}\.)?(?:www\.)?linkedin\.com/company/[^?\s"\'<>]+',
        re.I,
    ),
)


def _linkedin_from_raw_html(html: str) -> Optional[str]:
    """First normalized company LinkedIn URL found anywhere in HTML source."""
    for cre in _LINKEDIN_COMPANY_IN_HTML_RES:
        for m in cre.finditer(html):
            raw = m.group(0)
            if raw.startswith("//"):
                raw = "https:" + raw
            li = _normalize_linkedin(raw)
            if li:
                return li
    return None


# LinkedIn company pages embed industry-like strings in JSON blobs (not stable API).
_LINKEDIN_INDUSTRY_PATTERNS = (
    re.compile(r'["\']localizedIndustryName["\']\s*:\s*["\']([^"\']{2,200})["\']', re.I),
    re.compile(r'["\']industry["\']\s*:\s*["\']([^"\']{3,200})["\']', re.I),
    re.compile(
        r'industryTaxonomy["\']\s*:\s*\{[^}]{0,400}?["\']localizedName["\']\s*:\s*["\']([^"\']{2,200})["\']',
        re.I | re.S,
    ),
    re.compile(r'["\']localizedSpecialties["\']\s*:\s*\[([^\]]{5,800})\]', re.I),
)


def _extract_linkedin_company_page_signals(html: str) -> List[str]:
    """
    Pull free-text industry/specialty hints from a scraped LinkedIn company HTML payload.
    Complements generic JSON-LD / meta parsing on the same document.
    """
    if not (html or "").strip():
        return []
    acc: List[str] = []
    seen: set[str] = set()

    def add(raw: str) -> None:
        t = re.sub(r"\s+", " ", raw).strip().strip(",").strip()
        if len(t) < 3 or len(t) > 220:
            return
        k = t.lower()
        if k in seen:
            return
        seen.add(k)
        acc.append(t)

    for cre in _LINKEDIN_INDUSTRY_PATTERNS:
        for m in cre.finditer(html):
            chunk = m.group(1)
            if not chunk:
                continue
            if "," in chunk and len(chunk) > 40:
                for part in chunk.split(","):
                    add(part)
            else:
                add(chunk)

    _meta_industry_candidates(html, acc, seen)
    return acc


def _merge_unique_candidates(
    candidates: List[str],
    seen_lower: set[str],
    new_items: List[str],
) -> None:
    for x in new_items:
        k = x.lower()
        if k not in seen_lower:
            seen_lower.add(k)
            candidates.append(x)


@dataclass
class CompanyWebEnrichmentResult:
    """Structured output for B2B Scrapingdog enrichment."""

    skipped: bool
    source_type: Optional[str]
    company_website_url: Optional[str]
    company_business_name: Optional[str]
    linkedin_url: Optional[str]
    source_url: Optional[str]
    social: Dict[str, str] = field(default_factory=dict)
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    resolved_domain: Optional[str] = None
    domain_corrected_from: Optional[str] = None
    linkedin_company_page_scraped: bool = False
    employee_count: Optional[str] = None
    hq_country: Optional[str] = None
    hq_state: Optional[str] = None
    hq_city: Optional[str] = None
    description: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        hq_co_raw, hq_st_raw = _infer_united_states_hq_if_us_state(
            self.hq_country, self.hq_state
        )
        hq_co = _display_hq_country(hq_co_raw)
        if _is_united_states_country(hq_co_raw):
            hq_st = _expand_us_state_abbreviation(hq_st_raw)
        else:
            hq_st = ""
        out: Dict[str, Any] = {
            "skipped": self.skipped,
            "business": (self.company_business_name or "").strip(),
            "source_type": self.source_type,
            "website": (self.company_website_url or "").strip(),
            "source_url": self.source_url,
            "industry": (self.industry or "").strip(),
            "sub_industry": (self.sub_industry or "").strip(),
            "description": (self.description or "").strip(),
            "employee_count": (self.employee_count or "").strip(),
            "hq_country": hq_co,
            "hq_state": hq_st,
            "hq_city": (self.hq_city or "").strip(),
            "company_linkedin": (self.linkedin_url or "").strip(),
            "error": self.error,
        }
        if self.resolved_domain:
            out["resolved_domain"] = self.resolved_domain
        if self.domain_corrected_from:
            out["domain_corrected_from"] = self.domain_corrected_from
        soc = {k: v for k, v in self.social.items() if isinstance(v, str) and v.strip()}
        if soc:
            out["socials"] = soc
        return out


def enrich_company_web_profile(
    website_domain: str,
    api_key: str,
    *,
    is_b2b: bool = True,
    industry: Optional[str] = None,
    sub_industry: Optional[str] = None,
    timeout: float = 60.0,
    dynamic_first_scrape: bool = False,
    dynamic_about_scrape: bool = False,
    allow_domain_correction: bool = True,
    google_search_country: str = "us",
    scrape_linkedin_company_page: bool = True,
    dynamic_linkedin_scrape: bool = True,
    google_search_for_description: bool = True,
    linkedin_scraper_api_key: Optional[str] = None,
) -> CompanyWebEnrichmentResult:
    """
    Scrape company domain via Scrapingdog and extract web profile fields.

    Args:
        website_domain: Bare domain or URL host (e.g. ``example.com``).
        api_key: Scrapingdog API key.
        is_b2b: If False, returns immediately with ``skipped=True`` (no API calls).
        industry: Optional seed industry (merged with scraped hints, then taxonomy-refined).
        sub_industry: Optional seed sub-industry (merged with scraped hints, then taxonomy-refined).
        timeout: HTTP timeout per scrape request (seconds).
        dynamic_first_scrape: Scrapingdog ``dynamic`` for homepage attempt.
        dynamic_about_scrape: Scrapingdog ``dynamic`` for optional about-page scrape.
        allow_domain_correction: If the homepage scrape fails, call Scrapingdog Google Search
            (uses extra credits) to infer a corrected host, then retry the homepage scrape.
        google_search_country: Two-letter country for Scrapingdog Google Search (e.g. ``us``).
        scrape_linkedin_company_page: After a ``linkedin.com/company/...`` URL is found, scrape
            that page via Scrapingdog (extra credits) to improve industry/name signals.
        dynamic_linkedin_scrape: Use JS rendering for LinkedIn (recommended; LinkedIn is dynamic).
        google_search_for_description: If True and ``description`` is still empty after page scrapes,
            call Scrapingdog's Google Search Scraper API for organic snippets (uses search credits).
        linkedin_scraper_api_key: Optional Scrapingdog **LinkedIn company profile** API key.
            When unset, ``get_scrape_api_key()`` is used (``SCRAPE_API_KEY`` and legacy env names).
            ``employee_count`` and HQ prefer this JSON over HTML.

    Returns:
        CompanyWebEnrichmentResult with ``source_type`` / ``source_url`` rules applied.
        ``social`` on the result object only holds platforms with a URL; ``to_dict()`` omits
        ``socials`` when empty. JSON keys align with ``lead_format.json``: ``website``, ``business``,
        ``company_linkedin``, ``socials``.
        ``industry`` is merged from LinkedIn-only hints (profile API + LinkedIn page scrape);
        ``sub_industry`` from website-only hints (homepage/about). Taxonomy refinement uses
        ``normalize_industry_pair`` (``industry_taxonomy.py``).
        ``to_dict()`` always includes ``description``, ``employee_count``, ``hq_country``,
        ``hq_state`` (only for US), ``hq_city``, and ``company_linkedin``.
        When taxonomy mapping is enabled, if ``sub_industry`` cannot be resolved (including by
        matching the scraped description to ``INDUSTRY_TAXONOMY`` definitions at similarity
        ≥ 0.6), the result returns with ``error`` set to ``missing_sub_industry`` and
        ``source_url`` set to the proprietary sentinel.
        On success, ``resolved_domain`` is the host that worked; ``domain_corrected_from`` is set
        when that host differed from the normalized input.
    """

    if not is_b2b:
        ri, rs = _refine_seeds_only(industry, sub_industry)
        return CompanyWebEnrichmentResult(
            skipped=True,
            source_type=None,
            company_website_url=None,
            company_business_name=None,
            linkedin_url=None,
            source_url=None,
            social={},
            industry=ri,
            sub_industry=rs,
        )

    if not (api_key or "").strip():
        ri, rs = _refine_seeds_only(industry, sub_industry)
        return CompanyWebEnrichmentResult(
            skipped=False,
            source_type=SOURCE_TYPE_PROPRIETARY_DATABASE,
            company_website_url=None,
            company_business_name=None,
            linkedin_url=None,
            source_url=PROPRIETARY_SOURCE_URL,
            social={},
            industry=ri,
            sub_industry=rs,
            error="missing_api_key",
        )

    root_input = _normalize_domain(website_domain)
    if not root_input:
        ri, rs = _refine_seeds_only(industry, sub_industry)
        return CompanyWebEnrichmentResult(
            skipped=False,
            source_type=SOURCE_TYPE_PROPRIETARY_DATABASE,
            company_website_url=None,
            company_business_name=None,
            linkedin_url=None,
            source_url=PROPRIETARY_SOURCE_URL,
            social={},
            industry=ri,
            sub_industry=rs,
            error="invalid_domain",
        )

    root = root_input
    domain_corrected_from: Optional[str] = None
    company_website_url, homepage_html = _fetch_homepage_html(
        api_key,
        root,
        timeout=timeout,
        dynamic=dynamic_first_scrape,
        preferred_url=website_domain,
    )
    if (
        not homepage_html
        and allow_domain_correction
        and (api_key or "").strip()
    ):
        fixed = _pick_corrected_domain_via_google(
            api_key,
            root_input,
            timeout=timeout,
            country=google_search_country,
        )
        if fixed and fixed != root_input:
            domain_corrected_from = root_input
            root = fixed
            company_website_url, homepage_html = _fetch_homepage_html(
                api_key, root, timeout=timeout, dynamic=dynamic_first_scrape
            )

    if homepage_html and company_website_url:
        exact_host = _host_from_http_url(company_website_url)
        if exact_host:
            root = exact_host

    if not homepage_html:
        ri, rs = _refine_seeds_only(industry, sub_industry)
        return CompanyWebEnrichmentResult(
            skipped=False,
            source_type=SOURCE_TYPE_PROPRIETARY_DATABASE,
            company_website_url=None,
            company_business_name=None,
            linkedin_url=None,
            source_url=PROPRIETARY_SOURCE_URL,
            social={},
            industry=ri,
            sub_industry=rs,
            domain_corrected_from=domain_corrected_from,
            error="homepage_scrape_failed",
        )

    business_name = _extract_business_name(homepage_html)
    home_links = _absolute_links(company_website_url or f"https://{root}/", homepage_html)

    social: Dict[str, str] = {}
    _merge_social(social, home_links)
    _merge_social_from_raw_html(social, homepage_html)
    linkedin_url = _linkedin_company_url_from_google_search(
        api_key,
        company_name=business_name,
        domain_root=root,
        timeout=timeout,
        country=google_search_country,
    )
    if not linkedin_url:
        linkedin_url = _merge_linkedin(None, home_links)
    if not linkedin_url:
        linkedin_url = _linkedin_from_raw_html(homepage_html)

    about_url = _pick_about_url(root, company_website_url or "", home_links)

    about_html: Optional[str] = None
    if about_url and about_url.rstrip("/") != (company_website_url or "").rstrip("/"):
        about_html = _scrape_html(api_key, about_url, timeout=timeout, dynamic=dynamic_about_scrape)
        if about_html:
            about_links = _absolute_links(about_url, about_html)
            _merge_social(social, about_links)
            _merge_social_from_raw_html(social, about_html)
            if not linkedin_url:
                linkedin_url = _merge_linkedin(linkedin_url, about_links)
            if not linkedin_url:
                linkedin_url = _linkedin_from_raw_html(about_html)

    li_data: Optional[Dict[str, Any]] = None
    li_alt: Optional[Dict[str, Any]] = None
    li_scraper_key = _resolve_linkedin_scraper_api_key(linkedin_scraper_api_key)
    if li_scraper_key and linkedin_url:
        li_slug = _linkedin_company_slug_from_url(linkedin_url)
        if li_slug:
            li_data, li_alt = _fetch_scrapingdog_linkedin_company_profile(
                li_scraper_key,
                li_slug,
                timeout=timeout,
            )
    li_api_industry = _linkedin_profile_api_industry_candidates(li_data, li_alt)

    linkedin_company_page_scraped = False
    linkedin_html: Optional[str] = None
    if (
        scrape_linkedin_company_page
        and linkedin_url
        and (api_key or "").strip()
    ):
        li_page = linkedin_url.rstrip("/") + "/"
        linkedin_html = _scrape_html(
            api_key,
            li_page,
            timeout=timeout,
            dynamic=dynamic_linkedin_scrape,
        )
        if not linkedin_html and not dynamic_linkedin_scrape:
            linkedin_html = _scrape_html(
                api_key,
                li_page,
                timeout=timeout,
                dynamic=True,
            )
        if linkedin_html:
            linkedin_company_page_scraped = True
            if not (business_name or "").strip():
                bn_li = _extract_business_name(linkedin_html)
                if bn_li:
                    business_name = bn_li
            _merge_social(social, _absolute_links(li_page, linkedin_html))

    li_candidates: List[str] = []
    site_candidates: List[str] = []
    seen_li: set[str] = set()
    seen_site: set[str] = set()
    _merge_unique_candidates(li_candidates, seen_li, li_api_industry)
    if linkedin_html:
        _merge_unique_candidates(
            li_candidates,
            seen_li,
            _industry_candidate_strings_from_html(linkedin_html),
        )
        _merge_unique_candidates(
            li_candidates,
            seen_li,
            _extract_linkedin_company_page_signals(linkedin_html),
        )
    _merge_unique_candidates(
        site_candidates,
        seen_site,
        _industry_candidate_strings_from_html(homepage_html),
    )
    if about_html:
        _merge_unique_candidates(
            site_candidates,
            seen_site,
            _industry_candidate_strings_from_html(about_html),
        )
    li_candidates = _prioritize_taxonomy_like_candidates(li_candidates)
    site_candidates = _prioritize_taxonomy_like_candidates(site_candidates)
    raw_ind, _ = _merge_raw_industry_from_candidates(
        li_candidates,
        industry,
        sub_industry,
        assign_sub_industry_from_candidates=False,
    )
    _, raw_sub = _merge_raw_industry_from_candidates(
        site_candidates,
        industry,
        sub_industry,
        assign_industry_from_candidates=False,
    )
    seed_sub = (sub_industry or "").strip()
    if not (
        seed_sub
        and raw_sub
        and raw_sub.strip().lower() == seed_sub.lower()
    ):
        raw_sub = _coerce_sub_industry_raw_value(raw_sub)
    refine_candidates: List[str] = []
    seen_ref: set[str] = set()
    _merge_unique_candidates(refine_candidates, seen_ref, li_candidates)
    _merge_unique_candidates(refine_candidates, seen_ref, site_candidates)
    ind_out, sub_out = _refine_industry_sub_with_taxonomy(
        raw_ind,
        raw_sub,
        refine_candidates,
    )

    li_api_ec: Optional[str] = None
    li_api_hq: Tuple[Optional[str], Optional[str], Optional[str]] = (None, None, None)
    for _li_blob in (li_data, li_alt):
        if _li_blob:
            ec_p, hq_p = _linkedin_profile_api_employee_and_hq(_li_blob)
            if ec_p and not li_api_ec:
                li_api_ec = ec_p
            li_api_hq = _merge_hq_triple(li_api_hq, hq_p)

    hq_city_v, hq_state_v, hq_country_v = _merge_hq_triple(
        _merge_hq_pages(
            linkedin_html,
            about_html,
            homepage_html,
        ),
        li_api_hq,
    )
    _inf_co, _inf_st = _infer_united_states_hq_if_us_state(hq_country_v, hq_state_v)
    hq_country_v = _inf_co or None
    hq_state_v = _inf_st or None
    if hq_country_v and _is_united_states_country(hq_country_v):
        hq_country_v = _display_hq_country(hq_country_v)
    if hq_state_v and _is_united_states_country(hq_country_v or ""):
        hq_state_v = _expand_us_state_abbreviation(hq_state_v.strip()) or hq_state_v
    employee_count_v = _pick_employee_count(
        li_api_ec,
        _extract_employee_count_from_page(linkedin_html),
        _extract_employee_count_from_page(about_html),
        _extract_employee_count_from_page(homepage_html),
    )

    description_v = _pick_description_from_pages(about_html, homepage_html, linkedin_html)
    if (
        not description_v
        and google_search_for_description
        and (api_key or "").strip()
    ):
        description_v = _description_from_google_search_scraper(
            api_key,
            root,
            business_name=business_name,
            timeout=timeout,
            country=google_search_country,
        )

    if not _env_skip_industry_taxonomy():
        if not (sub_out or "").strip():
            guessed_sub = _pick_sub_industry_from_taxonomy_definition_similarity(
                description_v, ind_out
            )
            if guessed_sub:
                sub_out = guessed_sub
                try:
                    from lead_cache_json.industry_normalizer import normalize_industry_pair

                    io, so, _reason = normalize_industry_pair(ind_out, sub_out)
                    if io and so:
                        ind_out, sub_out = io, so
                except ImportError:
                    pass
        try:
            from lead_cache_json.industry_normalizer import coerce_industry_sub_to_taxonomy_scope

            ind_out, sub_out = coerce_industry_sub_to_taxonomy_scope(ind_out, sub_out)
        except ImportError:
            pass
        if not (sub_out or "").strip():
            return CompanyWebEnrichmentResult(
                skipped=False,
                source_type=SOURCE_TYPE_PROPRIETARY_DATABASE,
                company_website_url=company_website_url,
                company_business_name=business_name,
                linkedin_url=linkedin_url,
                source_url=PROPRIETARY_SOURCE_URL,
                social=social,
                industry=ind_out,
                sub_industry=None,
                resolved_domain=root,
                domain_corrected_from=domain_corrected_from,
                linkedin_company_page_scraped=linkedin_company_page_scraped,
                employee_count=employee_count_v,
                hq_country=hq_country_v,
                hq_state=hq_state_v,
                hq_city=hq_city_v,
                description=description_v,
                error="missing_sub_industry",
            )

    if about_url:
        source_url = about_url
        source_type = SOURCE_TYPE_COMPANY_WEBSITE
    else:
        source_url = PROPRIETARY_SOURCE_URL
        source_type = SOURCE_TYPE_PROPRIETARY_DATABASE

    if source_url == PROPRIETARY_SOURCE_URL:
        source_type = SOURCE_TYPE_PROPRIETARY_DATABASE

    return CompanyWebEnrichmentResult(
        skipped=False,
        source_type=source_type,
        company_website_url=company_website_url,
        company_business_name=business_name,
        linkedin_url=linkedin_url,
        source_url=source_url,
        social=social,
        industry=ind_out,
        sub_industry=sub_out,
        resolved_domain=root,
        domain_corrected_from=domain_corrected_from,
        linkedin_company_page_scraped=linkedin_company_page_scraped,
        employee_count=employee_count_v,
        hq_country=hq_country_v,
        hq_state=hq_state_v,
        hq_city=hq_city_v,
        description=description_v,
    )


def _absolute_http_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        return "https:" + u
    if not u.startswith(("http://", "https://")):
        return "https://" + u.lstrip("/")
    return u


def lead_format_json_from_enrichment(result: CompanyWebEnrichmentResult) -> Dict[str, Any]:
    """
    Map enrichment output to the same top-level keys as repo ``lead_format.json`` (README sample).

    Person-level fields (``full_name``, ``first``, ``last``, ``email``, ``role``, ``seniority``,
    ``linkedin``, ``country``, ``state``, ``city``) are empty strings—this flow is company-only.
    ``phone_numbers`` is always ``[]``; ``socials`` mirrors enrichment when non-empty.
    """
    raw = result.to_dict()
    website = _absolute_http_url(str(raw.get("website") or ""))
    company_li = (raw.get("company_linkedin") or "").strip()
    if company_li:
        company_li = _absolute_http_url(company_li)
    src = (raw.get("source_url") or "").strip()
    if not src:
        src = PROPRIETARY_SOURCE_URL
    soc = raw.get("socials")
    socials: Dict[str, str] = {}
    if isinstance(soc, dict):
        socials = {str(k): str(v).strip() for k, v in soc.items() if str(v).strip()}
    return {
        "business": (raw.get("business") or "").strip(),
        "full_name": "",
        "first": "",
        "last": "",
        "email": "",
        "role": "",
        "seniority": "",
        "website": website,
        "industry": (raw.get("industry") or "").strip(),
        "sub_industry": (raw.get("sub_industry") or "").strip(),
        "country": "",
        "state": "",
        "city": "",
        "linkedin": "",
        "company_linkedin": company_li,
        "source_url": src,
        "description": (raw.get("description") or "").strip(),
        "employee_count": (raw.get("employee_count") or "").strip(),
        "hq_country": (raw.get("hq_country") or "").strip(),
        "hq_state": (raw.get("hq_state") or "").strip(),
        "hq_city": (raw.get("hq_city") or "").strip(),
        "phone_numbers": [],
        "socials": socials,
    }


def domain_to_lead_format_json(
    domain: str,
    *,
    api_key: Optional[str] = None,
    is_b2b: bool = True,
    **enrich_kw: Any,
) -> Dict[str, Any]:
    """
    Enrich a company domain and return a **lead-format** dict (``lead_format.json`` / README shape).

    Parameters
    ----------
    domain:
        Website host or bare domain (e.g. ``stripe.com``).
    api_key:
        Scrapingdog key; defaults to :func:`get_scrape_api_key` when omitted or blank.
    is_b2b:
        Passed through to :func:`enrich_company_web_profile`.
    **enrich_kw:
        Forwarded to :func:`enrich_company_web_profile` (e.g. ``industry``, ``sub_industry``,
        ``timeout``, ``dynamic_first_scrape``, ``allow_domain_correction``, ``google_search_country``,
        ``scrape_linkedin_company_page``, ``linkedin_scraper_api_key``, etc.).

    Returns
    -------
    dict
        Keys aligned with ``lead_format.json``; contact/location fields are empty strings.
    """
    key = (api_key or "").strip() or get_scrape_api_key()
    enr = enrich_company_web_profile(domain, key, is_b2b=is_b2b, **enrich_kw)
    return lead_format_json_from_enrichment(enr)


def default_not_dup_extract_json_path(xlsx_path: Path | str) -> Path:
    """``{workbook_stem}_not_dup_extract`` directory beside the workbook."""
    p = Path(xlsx_path).resolve()
    return p.parent / f"{p.stem}_not_dup_extract"


def _lead_created_time_token(lead: Dict[str, Any], *, fallback_index: int) -> str:
    """
    Build a filesystem-safe created-time token for one lead file.

    Prefers a pre-existing created-time-ish field when present; otherwise uses current UTC time
    with microseconds plus the row index for uniqueness.
    """
    for key in ("created_time", "created_at", "timestamp"):
        raw = lead.get(key)
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        token = re.sub(r"[^0-9A-Za-z]+", "", text)
        if token:
            return token
    now = datetime.now(timezone.utc)
    return now.strftime("%Y%m%dT%H%M%S%fZ") + f"_{fallback_index:04d}"


def _write_not_dup_extract_lead_files(leads: List[Dict[str, Any]], output_root: Path) -> List[Path]:
    """
    Write one JSON file per lead object under ``output_root`` using ``lead{created_time}.json`` names.
    """
    out_dir = output_root.resolve()
    written: List[Path] = []
    used_names: set[str] = set()
    out_dir.mkdir(parents=True, exist_ok=True)
    for idx, lead in enumerate(leads, start=1):
        base = f"lead{_lead_created_time_token(lead, fallback_index=idx)}"
        name = base
        suffix = 2
        while f"{name}.json" in used_names or (out_dir / f"{name}.json").exists():
            name = f"{base}_{suffix}"
            suffix += 1
        path = write_lead_json_to_temp_file(lead, out_dir, filename=name)
        used_names.add(path.name)
        written.append(path)
    return written


def _filter_complete_export_leads(
    leads: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Keep only leads that satisfy the final JSON contract; return skipped items with reasons.
    """
    kept: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for idx, lead in enumerate(leads, start=1):
        row = apply_empty_string_to_null(dict(lead))
        ok, reasons = validate_output_record(row)
        if ok:
            kept.append(lead)
        else:
            skipped.append(
                {
                    "index": idx,
                    "website": str(lead.get("website") or "").strip(),
                    "business": str(lead.get("business") or "").strip(),
                    "reasons": reasons,
                }
            )
    return kept, skipped


# Columns omitted from ``database_*_not_dup_extract.json`` (header match is case-insensitive).
_NOT_DUP_JSON_EXCLUDE_KEYS_LOWER = frozenset(
    {
        "duplicate_check",
        "email_hash",
        "email_validation_status",
        "email_verification",
        "email_validation_passed",
        "email_validation_reject_reason",
        "email_validation_detail",
        "email_deliverability_summary",
        "email_mailbox_verify_status",
        "company address",
        "extracted_domain",
    }
)


def _records_for_not_dup_json_export(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert workbook rows to lead-format-shaped dicts with nulls for missing scalar fields."""
    def _null_if_blank(v: Any) -> Any:
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            return s or None
        return v

    out: List[Dict[str, Any]] = []
    for rec in records:
        cleaned: Dict[str, Any] = {}
        for k, v in rec.items():
            kl = str(k).strip().lower()
            if kl in _NOT_DUP_JSON_EXCLUDE_KEYS_LOWER:
                continue
            cleaned[k] = v
        first = str(cleaned.pop("First Name", "") or "").strip()
        last = str(cleaned.pop("Last Name", "") or "").strip()
        role = cleaned.pop("Title", "")
        business = cleaned.pop("Company Name", "")
        phone = _null_if_blank(cleaned.get("Phone"))
        website = _absolute_http_url(str(cleaned.get("Website") or ""))
        linkedin = _absolute_http_url(str(cleaned.get("Person Linkedin Url") or ""))
        full_name = " ".join(x for x in (first, last) if x).strip()
        out.append(
            {
                "business": _null_if_blank(str(business or "")),
                "full_name": _null_if_blank(full_name),
                "first": _null_if_blank(first),
                "last": _null_if_blank(last),
                "email": _null_if_blank(cleaned.get("personal_email")),
                "role": _null_if_blank(role),
                "website": _null_if_blank(website),
                "industry": None,
                "sub_industry": None,
                "country": _null_if_blank(cleaned.get("Country")),
                "state": _null_if_blank(cleaned.get("State")),
                "city": _null_if_blank(cleaned.get("City")),
                "linkedin": _null_if_blank(linkedin),
                "company_linkedin": None,
                "source_url": PROPRIETARY_SOURCE_URL,
                "description": None,
                "employee_count": None,
                "hq_country": _null_if_blank(cleaned.get("Company Country")),
                "hq_state": _null_if_blank(cleaned.get("Company State")),
                "hq_city": _null_if_blank(cleaned.get("Company City")),
                "phone_numbers": [phone] if phone else [],
                "socials": {},
            }
        )
    return out


def _merge_export_lead_with_scraped(
    base_lead: Dict[str, Any], scraped_lead: Dict[str, Any]
) -> Dict[str, Any]:
    """Overlay non-empty Scrapingdog lead-format values onto one exported workbook lead."""
    merged = dict(base_lead)

    def _non_blank(v: Any) -> bool:
        if v is None:
            return False
        if isinstance(v, str):
            return bool(v.strip())
        if isinstance(v, (list, dict)):
            return bool(v)
        return True

    scalar_keys = (
        "business",
        "website",
        "industry",
        "sub_industry",
        "company_linkedin",
        "source_url",
        "description",
        "employee_count",
        "hq_country",
        "hq_state",
        "hq_city",
    )
    for key in scalar_keys:
        val = scraped_lead.get(key)
        if _non_blank(val):
            merged[key] = val

    socials = scraped_lead.get("socials")
    if isinstance(socials, dict) and socials:
        merged["socials"] = socials
    phone_numbers = scraped_lead.get("phone_numbers")
    if isinstance(phone_numbers, list) and phone_numbers:
        merged["phone_numbers"] = phone_numbers

    return merged


def not_dup_rows_from_database_xlsx(
    path: Path | str,
    *,
    sheet_name: Optional[str] = None,
    only_row_indices: Optional[set[int]] = None,
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    Load ``database.xlsx``-style workbook: rows with ``duplicate_check`` equal to
    ``not dup`` (case-insensitive).

    Parameters
    ----------
    sheet_name :
        Worksheet name for :func:`pandas.read_excel`. When ``None``, the first sheet is used.
    only_row_indices :
        Optional Excel row numbers (1-based, row 1 is header). When set, only these data rows
        are considered for not-dup extraction.

    Returns
    -------
    domains :
        Unique normalized domains from ``website`` (first-seen order); blanks skipped.
    records :
        JSON-friendly list of dicts for export (duplicate check, email validation/hash, company
        address fields, and ``extracted_domain`` are omitted).
    """
    import json

    import pandas as pd

    p = Path(path).resolve()
    if not p.is_file():
        raise FileNotFoundError(f"Database workbook not found: {p}")
    df = pd.read_excel(
        p,
        engine="openpyxl",
        sheet_name=sheet_name if sheet_name is not None else 0,
    )
    if df.empty:
        return [], []
    col_by_lower = {str(c).strip().lower(): c for c in df.columns}
    dup_col = col_by_lower.get("duplicate_check")
    web_col = col_by_lower.get("website")
    if dup_col is None:
        raise ValueError(
            f"{p.name}: missing column 'duplicate_check' (headers: {list(df.columns)!r})"
        )
    if web_col is None:
        raise ValueError(
            f"{p.name}: missing column 'website' (headers: {list(df.columns)!r})"
        )
    mask = (
        df[dup_col]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .eq("not dup")
    )
    sub = df.loc[mask]
    if only_row_indices is not None:
        allowed_df_idx = {r - 2 for r in only_row_indices if int(r) >= 2}
        if allowed_df_idx:
            sub = sub.loc[sub.index.isin(allowed_df_idx)]
        else:
            sub = sub.iloc[0:0]
    if sub.empty:
        return [], []

    records: List[Dict[str, Any]] = json.loads(
        sub.to_json(orient="records", date_format="iso", default_handler=str)
    )
    records = _records_for_not_dup_json_export(records)

    seen: set[str] = set()
    domains: List[str] = []
    for _, row in sub.iterrows():
        raw_w = row[web_col]
        w = "" if pd.isna(raw_w) else str(raw_w).strip()
        dom = _normalize_domain(w)
        if dom and dom not in seen:
            seen.add(dom)
            domains.append(dom)

    return domains, records


def not_dup_website_domains_from_database_xlsx(
    path: Path | str, *, sheet_name: Optional[str] = None
) -> List[str]:
    """
    Load ``database.xlsx``-style workbook: keep rows whose ``duplicate_check`` value is
    ``not dup`` (case-insensitive), take ``website``, normalize to bare domain via
    :func:`_normalize_domain`, dedupe in first-seen order, skip blanks.
    """
    domains, _ = not_dup_rows_from_database_xlsx(path, sheet_name=sheet_name)
    return domains


def _parse_excel_row_numbers(spec: str) -> set[int]:
    """Parse ``'5,10,12-20'`` into 1-based Excel row numbers (inclusive)."""
    out: set[int] = set()
    for part in spec.replace(" ", "").split(","):
        if not part:
            continue
        if "-" in part:
            a, _, b = part.partition("-")
            lo, hi = int(a), int(b)
            if lo > hi:
                lo, hi = hi, lo
            for r in range(lo, hi + 1):
                out.add(r)
        else:
            out.add(int(part))
    return out


if __name__ == "__main__":
    import argparse
    import json
    import os

    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    _default_db = Path(__file__).resolve().parent / "database.xlsx"
    parser = argparse.ArgumentParser(
        description="Smoke-test B2B company enrichment (Scrapingdog). "
        "Set SCRAPE_API_KEY or pass --api-key. "
        "Omit domain to read domains from database.xlsx (not dup + website)."
    )
    parser.add_argument(
        "domain",
        nargs="?",
        default=None,
        help="Single website domain, e.g. stripe.com (omit to use --database-xlsx)",
    )
    parser.add_argument(
        "--database-xlsx",
        type=Path,
        default=_default_db,
        help=(
            "Excel with columns duplicate_check and website; "
            f"only rows with duplicate_check 'not dup' are used (default: {_default_db.name} next to this script)"
        ),
    )
    parser.add_argument(
        "--api-key",
        default=get_scrape_api_key(),
        help="Scrape API key (default: env SCRAPE_API_KEY or legacy SCRAPINGDOG_*/LINKEDIN_*/SCRAPERAPI_*)",
    )
    parser.add_argument(
        "--no-b2b",
        action="store_true",
        help="Exercise non-B2B skip path (no HTTP)",
    )
    parser.add_argument(
        "--dynamic",
        action="store_true",
        help="Use dynamic=true for homepage (and about) scrapes",
    )
    parser.add_argument(
        "--no-domain-correction",
        action="store_true",
        help="Do not call Google Search when homepage scrape fails (saves Scrapingdog credits)",
    )
    parser.add_argument(
        "--no-linkedin-scrape",
        action="store_true",
        help="Do not scrape LinkedIn company page after URL is found (saves Scrapingdog credits)",
    )
    parser.add_argument(
        "--no-google-search-description",
        action="store_true",
        help="Do not call Google Search Scraper API for description fallback (saves credits)",
    )
    parser.add_argument(
        "--linkedin-scraper-api-key",
        default=get_scrape_api_key(),
        help="LinkedIn company profile key (default: same as SCRAPE_API_KEY / unified scrape key)",
    )
    parser.add_argument(
        "--lead-format",
        action="store_true",
        help="Print lead_format.json-shaped object (domain → full lead keys; person fields empty)",
    )
    parser.add_argument(
        "--extract-json-out",
        type=Path,
        default=None,
        help=(
            "When using --database-xlsx (no positional domain), write one JSON file per lead object "
            "into this directory (default: <workbook_stem>_not_dup_extract next to the workbook)"
        ),
    )
    parser.add_argument(
        "--no-extract-json",
        action="store_true",
        help="Do not write per-lead JSON files when loading from the database workbook",
    )
    parser.add_argument(
        "--skip-email-check",
        action="store_true",
        help="Skip workbook email validation + duplicate check before B2B enrichment (database mode only)",
    )
    parser.add_argument(
        "--rows",
        type=str,
        default=None,
        help=(
            "With email check: validate only these Excel row numbers (1-based), e.g. 5,10,12-20. "
            "Omit to process all pending rows in the sheet."
        ),
    )
    cli = parser.parse_args()
    only_rows: Optional[set[int]] = None
    extract_records: List[Dict[str, Any]] = []
    domain_to_record_indices: Dict[str, List[int]] = {}
    out_json: Optional[Path] = None
    if cli.domain:
        domains = [_normalize_domain(cli.domain)]
        if not domains[0]:
            raise SystemExit("Empty or invalid domain argument.")
        resolved_sheet: Optional[str] = None
    else:
        import asyncio

        import openpyxl

        import check_email_dup as dup

        db_path = cli.database_xlsx.resolve()
        wb0 = openpyxl.load_workbook(db_path, data_only=False)
        try:
            _, resolved_sheet = dup.resolve_worksheet(wb0, sheet_name=None)
        finally:
            wb0.close()

        if not cli.skip_email_check:
            if cli.rows:
                try:
                    only_rows = _parse_excel_row_numbers(cli.rows)
                except ValueError as e:
                    raise SystemExit(f"Invalid --rows: {e}") from e
                if not only_rows:
                    raise SystemExit("--rows must list at least one row number.")
            print(
                "\n--- Step 1: Email validation (workbook) ---\n"
                f"  File: {db_path}\n"
                f"  Sheet: {resolved_sheet!r}\n",
                flush=True,
            )
            asyncio.run(
                dup.run_workbook_validation(
                    db_path,
                    db_path,
                    sheet_name=resolved_sheet,
                    write_validation_columns=True,
                    only_row_indices=only_rows,
                )
            )
            print("\n--- Step 2: Duplicate check (subnet API) ---\n", flush=True)
            asyncio.run(
                dup.run_duplicate_check_phase(
                    str(db_path),
                    sheet_name=resolved_sheet,
                    only_row_indices=only_rows,
                    batch_size=(len(only_rows) if only_rows else dup.DUP_CHECK_BATCH_SIZE),
                )
            )
            print("\n--- Step 3: Load not-dup rows + B2B enrichment ---\n", flush=True)
        elif cli.rows:
            print(
                "Note: --rows ignored because --skip-email-check was set.",
                flush=True,
            )

        domains, extract_records = not_dup_rows_from_database_xlsx(
            cli.database_xlsx,
            sheet_name=resolved_sheet,
            only_row_indices=only_rows if cli.rows else None,
        )
        if not cli.no_extract_json and extract_records:
            out_json = (
                cli.extract_json_out.resolve()
                if cli.extract_json_out is not None
                else default_not_dup_extract_json_path(cli.database_xlsx)
            )
        if not domains:
            raise SystemExit(
                f"No domains from {cli.database_xlsx!s}: "
                "need rows with duplicate_check='not dup' and a parsable website."
            )
        print(
            f"Loaded {len(domains)} unique domain(s) from {len(extract_records)} row(s) in "
            f"{cli.database_xlsx!s} (duplicate_check='not dup').",
            flush=True,
        )
        for idx, rec in enumerate(extract_records):
            dom_k = _normalize_domain(str(rec.get("website") or ""))
            if dom_k:
                domain_to_record_indices.setdefault(dom_k, []).append(idx)

    enrich_kw = dict(
        is_b2b=not cli.no_b2b,
        dynamic_first_scrape=cli.dynamic,
        dynamic_about_scrape=cli.dynamic,
        allow_domain_correction=not cli.no_domain_correction,
        scrape_linkedin_company_page=not cli.no_linkedin_scrape,
        google_search_for_description=not cli.no_google_search_description,
        linkedin_scraper_api_key=(cli.linkedin_scraper_api_key or None),
    )

    total = len(domains)
    for i, dom in enumerate(domains, start=1):
        print(f"\n{'=' * 60}\n[{i}/{total}] {dom}\n{'=' * 60}", flush=True)
        try:
            enr = enrich_company_web_profile(dom, cli.api_key, **enrich_kw)
        except Exception as e:
            print(f"Skipping {dom}: scraping failed with exception: {e}", flush=True)
            continue

        if enr.error:
            print(
                f"Skipping {dom}: scraping failed ({enr.error}).",
                flush=True,
            )
            continue

        if cli.lead_format:
            scraped_lead = lead_format_json_from_enrichment(enr)
            print(json.dumps(scraped_lead, indent=2, ensure_ascii=False))
        else:
            print(json.dumps(enr.to_dict(), indent=2))
            scraped_lead = lead_format_json_from_enrichment(enr)
        if extract_records:
            for idx in domain_to_record_indices.get(dom, []):
                extract_records[idx] = _merge_export_lead_with_scraped(
                    extract_records[idx], scraped_lead
                )
    if out_json is not None and extract_records:
        out_root = out_json
        if out_root.suffix.lower() == ".json":
            out_root = out_root.with_suffix("")
        complete_records, skipped_records = _filter_complete_export_leads(extract_records)
        if skipped_records:
            print(
                f"Skipped {len(skipped_records)} incomplete lead(s) after Scrapingdog fetch.",
                flush=True,
            )
            for item in skipped_records[:10]:
                label = item["business"] or item["website"] or f"row {item['index']}"
                print(f"  - {label}: {', '.join(item['reasons'])}", flush=True)
            if len(skipped_records) > 10:
                print(f"  ... and {len(skipped_records) - 10} more", flush=True)
        written_files = _write_not_dup_extract_lead_files(complete_records, out_root)
        print(
            f"Saved {len(written_files)} lead JSON file(s) in {out_root}",
            flush=True,
        )
