"""
Strict validation for formatted lead JSON rows.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

from lead_cache_json.json_contract import (
    EMPLOYEE_COUNT_BUCKETS,
    K_COUNTRY,
    K_EMPLOYEE_COUNT,
    K_HQ_COUNTRY,
    K_HQ_STATE,
    K_PHONE_NUMBERS,
    K_SOCIALS,
    K_STATE,
    REQUIRED_STRING_FIELDS,
    US_COUNTRY_ALIASES,
)

logger = logging.getLogger(__name__)


def _is_blank(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and not v.strip():
        return True
    return False


def is_us_lead_country(value: Any) -> bool:
    if not value or not isinstance(value, str):
        return False
    return value.strip().lower() in US_COUNTRY_ALIASES or value.strip() == "United States"


def apply_empty_string_to_null(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert empty strings to None for optional fields; keep required string fields as-is for validation.
    """
    out = dict(row)
    for k, v in list(out.items()):
        if k in (K_PHONE_NUMBERS, K_SOCIALS):
            continue
        if k in REQUIRED_STRING_FIELDS:
            continue
        if isinstance(v, str) and not v.strip():
            out[k] = None
    return out


def validate_output_record(row: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate one formatted record against the output contract.

    Returns ``(ok, reasons)`` where ``reasons`` is empty when ``ok``.
    """
    reasons: List[str] = []

    for field in REQUIRED_STRING_FIELDS:
        if _is_blank(row.get(field)):
            reasons.append(f"missing_or_empty:{field}")

    ec = row.get(K_EMPLOYEE_COUNT)
    if ec not in EMPLOYEE_COUNT_BUCKETS:
        reasons.append(f"invalid_employee_count:{ec!r}")

    phones = row.get(K_PHONE_NUMBERS)
    if not isinstance(phones, list):
        reasons.append("phone_numbers_must_be_list")
    else:
        for i, p in enumerate(phones):
            if not isinstance(p, str) or not p.strip():
                reasons.append(f"invalid_phone_entry:{i}")

    socials = row.get(K_SOCIALS)
    if not isinstance(socials, dict):
        reasons.append("socials_must_be_object")

    country = row.get(K_COUNTRY)
    if is_us_lead_country(country):
        if _is_blank(row.get(K_STATE)):
            reasons.append("state_required_for_us_lead")

    hq = row.get(K_HQ_COUNTRY)
    if is_us_lead_country(hq):
        if _is_blank(row.get(K_HQ_STATE)):
            reasons.append("hq_state_required_for_us_company")

    ok = not reasons
    if not ok:
        logger.debug("Validation failed: %s", reasons)
    return ok, reasons
