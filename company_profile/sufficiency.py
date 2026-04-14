"""Decide whether a stored company row is good enough to skip external enrichment."""

from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

# Minimum signals: website OR linkedin, plus at least one of industry / employee / hq country
_REQUIRED_ANY = (
    "company_website_url",
    "company_linkedin_url",
)
_RICHNESS_ANY = (
    "industry",
    "employee_count",
    "hq_location_country",
)


def is_sufficient(profile: Dict[str, Any]) -> bool:
    """
    True if PostgreSQL row has enough data to skip Scrapingdog.

    Rules:
    - Must have at least one identity URL (website or LinkedIn company URL).
    - Must have at least one richness field (industry, employee_count, or hq country).
    """
    if not profile:
        logger.debug("sufficiency: empty profile")
        return False
    has_id = bool(
        str(profile.get("company_website_url") or "").strip()
        or str(profile.get("company_linkedin_url") or "").strip()
    )
    if not has_id:
        logger.debug("sufficiency: no website or linkedin")
        return False
    has_rich = any(str(profile.get(k) or "").strip() for k in _RICHNESS_ANY)
    if not has_rich:
        logger.debug("sufficiency: missing industry/employee/hq_country")
        return False
    return True
