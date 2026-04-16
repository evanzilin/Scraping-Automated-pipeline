"""
Strict validation for formatted lead JSON rows.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Tuple

from lead_cache_json.json_contract import (
    EMPLOYEE_COUNT_BUCKETS,
    K_COMPANY_LINKEDIN,
    K_COUNTRY,
    K_EMPLOYEE_COUNT,
    K_EMAIL,
    K_FIRST,
    K_HQ_COUNTRY,
    K_HQ_STATE,
    K_LAST,
    K_PHONE_NUMBERS,
    K_SOCIALS,
    K_STATE,
    K_SOURCE_TYPE,
    K_SOURCE_URL,
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


def _check_name_email_match(email: Any, first_name: Any, last_name: Any) -> bool:
    em = str(email or "").strip().lower()
    fn = str(first_name or "").strip().lower()
    ln = str(last_name or "").strip().lower()
    if "@" not in em or not fn or not ln:
        return False
    local = em.split("@", 1)[0]
    local_normalized = re.sub(r"[^a-z0-9]", "", local)
    first_normalized = re.sub(r"[^a-z0-9]", "", fn)
    last_normalized = re.sub(r"[^a-z0-9]", "", ln)
    min_len = 3
    patterns = []
    if len(first_normalized) >= min_len:
        patterns.append(first_normalized)
    if len(last_normalized) >= min_len:
        patterns.append(last_normalized)
    patterns.append(f"{first_normalized}{last_normalized}")
    if first_normalized:
        patterns.append(f"{first_normalized[0]}{last_normalized}")
        patterns.append(f"{last_normalized}{first_normalized[0]}")
    patterns = [p for p in patterns if p and len(p) >= min_len]
    if any(pattern in local_normalized for pattern in patterns):
        return True
    if len(local_normalized) >= min_len:
        if len(first_normalized) >= len(local_normalized) and first_normalized.startswith(local_normalized):
            return True
        if len(last_normalized) >= len(local_normalized) and last_normalized.startswith(local_normalized):
            return True
        for length in range(min_len, min(len(first_normalized) + 1, 7)):
            if first_normalized[:length] in local_normalized:
                return True
        for length in range(min_len, min(len(last_normalized) + 1, 7)):
            if last_normalized[:length] in local_normalized:
                return True
    return False


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

    if not _check_name_email_match(row.get(K_EMAIL), row.get(K_FIRST), row.get(K_LAST)):
        reasons.append("name_email_mismatch")

    if str(row.get(K_SOURCE_URL) or "").strip() == "proprietary_database":
        if str(row.get(K_SOURCE_TYPE) or "").strip() != "proprietary_database":
            reasons.append("source_type_must_match_proprietary_database")

    ok = not reasons
    if not ok:
        logger.debug("Validation failed: %s", reasons)
    return ok, reasons
