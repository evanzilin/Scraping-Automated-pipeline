"""Dataclasses for input rows, metadata, and enrichment results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(slots=True)
class HQLocation:
    country: Optional[str] = None
    state: Optional[str] = None
    city: Optional[str] = None

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {"country": self.country, "state": self.state, "city": self.city}

    @classmethod
    def from_dict(cls, d: Optional[Dict[str, Any]]) -> HQLocation:
        if not d:
            return HQLocation()
        return cls(
            country=d.get("country"),
            state=d.get("state"),
            city=d.get("city"),
        )


@dataclass(slots=True)
class ClaimedFields:
    company_website_url: Optional[str] = None
    company_linkedin_url: Optional[str] = None
    employee_count: Optional[str] = None
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    hq: HQLocation = field(default_factory=HQLocation)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "company_website_url": self.company_website_url,
            "company_linkedin_url": self.company_linkedin_url,
            "employee_count": self.employee_count,
            "industry": self.industry,
            "sub_industry": self.sub_industry,
            "hq_location": self.hq.to_dict(),
        }


@dataclass(slots=True)
class StoredCompanyMetadata:
    company_id: Optional[str] = None
    email_domain: Optional[str] = None
    company_website_url: Optional[str] = None
    company_linkedin_url: Optional[str] = None
    employee_count: Optional[str] = None
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    hq: HQLocation = field(default_factory=HQLocation)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "company_id": self.company_id,
            "email_domain": self.email_domain,
            "company_website_url": self.company_website_url,
            "company_linkedin_url": self.company_linkedin_url,
            "employee_count": self.employee_count,
            "industry": self.industry,
            "sub_industry": self.sub_industry,
            "hq_location": self.hq.to_dict(),
        }


@dataclass(slots=True)
class EnrichedFields:
    company_website_url: Optional[str] = None
    company_linkedin_url: Optional[str] = None
    employee_count: Optional[str] = None
    industry: Optional[str] = None
    sub_industry: Optional[str] = None
    hq: HQLocation = field(default_factory=HQLocation)
    raw_error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "company_website_url": self.company_website_url,
            "company_linkedin_url": self.company_linkedin_url,
            "employee_count": self.employee_count,
            "industry": self.industry,
            "sub_industry": self.sub_industry,
            "hq_location": self.hq.to_dict(),
            "raw_error": self.raw_error,
        }

    def any_value(self) -> bool:
        return bool(
            self.company_website_url
            or self.company_linkedin_url
            or self.employee_count
            or self.industry
            or self.sub_industry
            or self.hq.country
            or self.hq.state
            or self.hq.city
        )


@dataclass(slots=True)
class CompanyEnrichmentResult:
    company_id: Optional[str]
    lead_id: Optional[str]
    email_domain: str
    claimed: Dict[str, Any]
    stored: Dict[str, Any]
    enriched: Dict[str, Any]
    final_resolved_fields: Dict[str, Any]
    matched_fields: List[str]
    mismatched_fields: List[str]
    missing_fields: List[str]
    status: str
    confidence_notes: List[str]
    processing_time_ms: float
    source_used: str
    error_detail: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "company_id": self.company_id,
            "lead_id": self.lead_id,
            "email_domain": self.email_domain,
            "claimed": self.claimed,
            "stored": self.stored,
            "enriched": self.enriched,
            "final_resolved_fields": self.final_resolved_fields,
            "matched_fields": self.matched_fields,
            "mismatched_fields": self.mismatched_fields,
            "missing_fields": self.missing_fields,
            "status": self.status,
            "confidence_notes": self.confidence_notes,
            "processing_time_ms": self.processing_time_ms,
            "source_used": self.source_used,
            "error_detail": self.error_detail,
        }


@dataclass(slots=True)
class BatchSummary:
    total_records: int = 0
    matched_count: int = 0
    partially_matched_count: int = 0
    enriched_count: int = 0
    insufficient_data_count: int = 0
    failed_count: int = 0
    cache_hits: int = 0
    db_hits: int = 0
    scrapingdog_calls: int = 0
    total_processing_time_ms: float = 0.0

    @property
    def average_processing_time_ms(self) -> float:
        if self.total_records <= 0:
            return 0.0
        return self.total_processing_time_ms / self.total_records

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_records": self.total_records,
            "matched_count": self.matched_count,
            "partially_matched_count": self.partially_matched_count,
            "enriched_count": self.enriched_count,
            "insufficient_data_count": self.insufficient_data_count,
            "failed_count": self.failed_count,
            "cache_hits": self.cache_hits,
            "db_hits": self.db_hits,
            "scrapingdog_calls": self.scrapingdog_calls,
            "total_processing_time_ms": self.total_processing_time_ms,
            "average_processing_time_ms": self.average_processing_time_ms,
        }


@dataclass(frozen=True, slots=True)
class SchemaMapping:
    """Map logical fields to SQL column names (adapt to your tables)."""

    # leads / queue table
    leads_table: str = "leads"
    col_company_id: str = "company_id"
    col_lead_id: str = "lead_id"
    col_email: str = "email"
    col_email_domain: str = "email_domain"
    col_claimed_website: str = "claimed_company_website_url"
    col_claimed_linkedin: str = "claimed_company_linkedin_url"
    col_claimed_employees: str = "claimed_employee_count"
    col_claimed_industry: str = "claimed_industry"
    col_claimed_sub_industry: str = "claimed_sub_industry"
    col_claimed_hq_country: str = "claimed_hq_country"
    col_claimed_hq_state: str = "claimed_hq_state"
    col_claimed_hq_city: str = "claimed_hq_city"
    leads_pk_column: str = "id"
    leads_pending_predicate: str = "TRUE"  # e.g. "enrichment_status IS NULL"

    # companies table
    companies_table: str = "companies"
    comp_col_id: str = "id"
    comp_col_domain: str = "email_domain"
    comp_col_website: str = "company_website_url"
    comp_col_linkedin: str = "company_linkedin_url"
    comp_col_employees: str = "employee_count"
    comp_col_industry: str = "industry"
    comp_col_sub_industry: str = "sub_industry"
    comp_col_hq_country: str = "hq_country"
    comp_col_hq_state: str = "hq_state"
    comp_col_hq_city: str = "hq_city"


def row_to_claimed(row: Dict[str, Any], m: SchemaMapping) -> ClaimedFields:
    return ClaimedFields(
        company_website_url=_g(row, m.col_claimed_website),
        company_linkedin_url=_g(row, m.col_claimed_linkedin),
        employee_count=_g(row, m.col_claimed_employees),
        industry=_g(row, m.col_claimed_industry),
        sub_industry=_g(row, m.col_claimed_sub_industry),
        hq=HQLocation(
            country=_g(row, m.col_claimed_hq_country),
            state=_g(row, m.col_claimed_hq_state),
            city=_g(row, m.col_claimed_hq_city),
        ),
    )


def _g(row: Dict[str, Any], key: str) -> Optional[str]:
    v = row.get(key)
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def stored_from_company_row(row: Dict[str, Any], m: SchemaMapping) -> StoredCompanyMetadata:
    cid = row.get(m.comp_col_id)
    return StoredCompanyMetadata(
        company_id=str(cid) if cid is not None else None,
        email_domain=_g(row, m.comp_col_domain),
        company_website_url=_g(row, m.comp_col_website),
        company_linkedin_url=_g(row, m.comp_col_linkedin),
        employee_count=_g(row, m.comp_col_employees),
        industry=_g(row, m.comp_col_industry),
        sub_industry=_g(row, m.comp_col_sub_industry),
        hq=HQLocation(
            country=_g(row, m.comp_col_hq_country),
            state=_g(row, m.comp_col_hq_state),
            city=_g(row, m.comp_col_hq_city),
        ),
    )


def target_record_from_row(row: Dict[str, Any], m: SchemaMapping) -> "TargetRecord":
    return TargetRecord(
        company_id=str(row[m.col_company_id]) if row.get(m.col_company_id) is not None else None,
        lead_id=str(row[m.col_lead_id]) if row.get(m.col_lead_id) is not None else None,
        email=_g(row, m.col_email),
        email_domain=_g(row, m.col_email_domain) or "",
        claimed=row_to_claimed(row, m),
        raw_row=row,
    )


@dataclass(slots=True)
class TargetRecord:
    company_id: Optional[str]
    lead_id: Optional[str]
    email: Optional[str]
    email_domain: str
    claimed: ClaimedFields
    raw_row: Dict[str, Any]
