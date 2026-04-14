"""
Single source of truth for company profile shape (fields, types, helpers).

All layers (Postgres, Elasticsearch, API responses) map to / from this contract.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional


@dataclass
class CompanyProfile:
    """Normalized company profile returned by ``get_company_profile``."""

    company_domain: str
    company_website_url: Optional[str] = None
    employee_count: Optional[str] = None
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    company_linkedin_url: Optional[str] = None
    hq_location_country: Optional[str] = None
    hq_location_state: Optional[str] = None
    hq_location_city: Optional[str] = None
    source_url: Optional[str] = None
    source_type: Optional[str] = None
    # Optional internal key for Postgres / ES (not always exposed)
    id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """JSON-serializable dict; omits None values for optional fields if desired — keep explicit Nones for clarity."""
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None or k in ("company_domain",)}

    def to_dict_full(self) -> Dict[str, Any]:
        """All keys present (API / DB round-trip)."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CompanyProfile:
        return cls(
            company_domain=str(data.get("company_domain") or "").strip().lower(),
            company_website_url=_opt_str(data.get("company_website_url")),
            employee_count=_opt_str(data.get("employee_count")),
            industry=_opt_str(data.get("industry")),
            sub_industry=_opt_str(data.get("sub_industry")),
            company_linkedin_url=_opt_str(data.get("company_linkedin_url")),
            hq_location_country=_opt_str(data.get("hq_location_country")),
            hq_location_state=_opt_str(data.get("hq_location_state")),
            hq_location_city=_opt_str(data.get("hq_location_city")),
            source_url=_opt_str(data.get("source_url")),
            source_type=_opt_str(data.get("source_type")),
            id=_opt_str(data.get("id")),
        )


def _opt_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


# Canonical field list for SQL SELECT / ES _source projection
PROFILE_FIELD_NAMES: tuple[str, ...] = (
    "id",
    "company_domain",
    "company_website_url",
    "employee_count",
    "industry",
    "sub_industry",
    "company_linkedin_url",
    "hq_location_country",
    "hq_location_state",
    "hq_location_city",
    "source_url",
    "source_type",
)
