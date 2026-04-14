"""
Central schema: output JSON keys, cache input aliases, and shared constants.

All modules import field identifiers from here — do not scatter magic strings.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Final, Mapping, Tuple

# ---------------------------------------------------------------------------
# Output contract (strict JSON keys)
# ---------------------------------------------------------------------------

K_BUSINESS: Final = "business"
K_COUNTRY: Final = "country"
K_CITY: Final = "city"
K_FULL_NAME: Final = "full_name"
K_FIRST: Final = "first"
K_LAST: Final = "last"
K_EMAIL: Final = "email"
K_ROLE: Final = "role"
K_WEBSITE: Final = "website"
K_INDUSTRY: Final = "industry"
K_SUB_INDUSTRY: Final = "sub_industry"
K_STATE: Final = "state"
K_LINKEDIN: Final = "linkedin"
K_COMPANY_LINKEDIN: Final = "company_linkedin"
K_SOURCE_URL: Final = "source_url"
K_DESCRIPTION: Final = "description"
K_EMPLOYEE_COUNT: Final = "employee_count"
K_HQ_COUNTRY: Final = "hq_country"
K_HQ_STATE: Final = "hq_state"
K_HQ_CITY: Final = "hq_city"
K_SOURCE_TYPE: Final = "source_type"
K_PHONE_NUMBERS: Final = "phone_numbers"
K_SOCIALS: Final = "socials"

OUTPUT_KEYS: Final[Tuple[str, ...]] = (
    K_BUSINESS,
    K_FULL_NAME,
    K_FIRST,
    K_LAST,
    K_EMAIL,
    K_ROLE,
    K_WEBSITE,
    K_INDUSTRY,
    K_SUB_INDUSTRY,
    K_COUNTRY,
    K_STATE,
    K_CITY,
    K_LINKEDIN,
    K_COMPANY_LINKEDIN,
    K_SOURCE_URL,
    K_DESCRIPTION,
    K_EMPLOYEE_COUNT,
    K_HQ_COUNTRY,
    K_HQ_STATE,
    K_HQ_CITY,
    K_SOURCE_TYPE,
    K_PHONE_NUMBERS,
    K_SOCIALS,
)

# Required for every record (null not allowed after normalization)
REQUIRED_STRING_FIELDS: Final[Tuple[str, ...]] = (
    K_BUSINESS,
    K_FULL_NAME,
    K_FIRST,
    K_LAST,
    K_EMAIL,
    K_ROLE,
    K_WEBSITE,
    K_INDUSTRY,
    K_SUB_INDUSTRY,
    K_COUNTRY,
    K_CITY,
    K_LINKEDIN,
    K_COMPANY_LINKEDIN,
    K_SOURCE_URL,
    K_DESCRIPTION,
    K_EMPLOYEE_COUNT,
    K_HQ_COUNTRY,
)

# US-only conditional requirements (validated in validators.py)
US_COUNTRY_ALIASES: Final[frozenset[str]] = frozenset(
    {
        "united states",
        "usa",
        "us",
        "u.s.",
        "u.s.a.",
        "america",
        "united states of america",
    }
)

# Allowed employee_count buckets (exact display strings)
EMPLOYEE_COUNT_BUCKETS: Final[Tuple[str, ...]] = (
    "0-1",
    "2-10",
    "11-50",
    "51-200",
    "201-500",
    "501-1,000",
    "1,001-5,000",
    "5,001-10,000",
    "10,001+",
)

# When source_url / source_type cannot be inferred (per product rule)
SOURCE_UNCLEAR_VALUE: Final = "source_type"

SOCIALS_TWITTER_KEY: Final = "twitter"

# Default output paths (relative to process CWD unless overridden)
DEFAULT_OUTPUT_JSON: Final[Path] = Path("temp") / "json-date.json"
DEFAULT_REJECT_JSON: Final[Path] = Path("temp") / "json-date.rejects.json"

# ---------------------------------------------------------------------------
# Cache / lead input aliases → logical names (used only in record_normalizer)
# ---------------------------------------------------------------------------

# Maps logical name → tuple of keys to try on a flat dict (first non-empty wins)
CACHE_FIELD_ALIASES: Final[Mapping[str, Tuple[str, ...]]] = {
    "business": (
        "business",
        "company",
        "Company",
        "company_name",
        "Company Name",
        "organization",
    ),
    "full_name": ("full_name", "Full_name", "Full Name", "name", "Name"),
    "first": ("first", "First", "first_name", "FirstName"),
    "last": ("last", "Last", "last_name", "LastName"),
    "email": ("email", "Email", "work_email"),
    "role": ("role", "Role", "title", "Title", "job_title"),
    "website": ("website", "Website", "company_website", "company_website_url", "url"),
    "industry": ("industry", "Industry"),
    "sub_industry": ("sub_industry", "sub-industry", "Sub-industry", "Sub_industry"),
    "country": ("country", "Country", "location_country", "person_country"),
    "state": ("state", "State", "location_state", "person_state"),
    "city": ("city", "City", "location_city", "person_city"),
    "linkedin": ("linkedin", "LinkedIn", "linkedin_url", "person_linkedin"),
    "company_linkedin": (
        "company_linkedin",
        "company_linkedin_url",
        "Company Linkedin",
        "company_linkedin_slug",
    ),
    "source_url": ("source_url", "Source_url", "sourceURL"),
    "source_type": ("source_type", "Source_type"),
    "description": ("description", "Description", "bio", "summary"),
    "employee_count": (
        "employee_count",
        "Employee_count",
        "company_size",
        "employees",
        "employee_range",
    ),
    "hq_country": (
        "hq_country",
        "hq_location_country",
        "HQ_country",
        "company_country",
        "headquarters_country",
    ),
    "hq_state": ("hq_state", "hq_location_state", "HQ_state", "company_state"),
    "hq_city": ("hq_city", "hq_location_city", "HQ_city", "headquarters_city"),
    "phone_numbers": ("phone_numbers", "phones", "phone", "Phone"),
    "socials": ("socials", "Socials", "twitter", "Twitter"),
}


def empty_output_record() -> Dict[str, Any]:
    """Skeleton dict with all output keys (for tests / templates)."""
    return {
        K_BUSINESS: None,
        K_FULL_NAME: None,
        K_FIRST: None,
        K_LAST: None,
        K_EMAIL: None,
        K_ROLE: None,
        K_WEBSITE: None,
        K_INDUSTRY: None,
        K_SUB_INDUSTRY: None,
        K_COUNTRY: None,
        K_STATE: None,
        K_CITY: None,
        K_LINKEDIN: None,
        K_COMPANY_LINKEDIN: None,
        K_SOURCE_URL: None,
        K_DESCRIPTION: None,
        K_EMPLOYEE_COUNT: None,
        K_HQ_COUNTRY: None,
        K_HQ_STATE: None,
        K_HQ_CITY: None,
        K_SOURCE_TYPE: None,
        K_PHONE_NUMBERS: [],
        K_SOCIALS: {},
    }
