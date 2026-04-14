"""
Company profile enrichment: **Elasticsearch first**, **PostgreSQL source of truth**.

Main API::

    from company_profile import get_company_profile

    profile = get_company_profile("acme.com")

See ``example_usage.py`` and ``.env.example``.
"""

from company_profile.company_contract import CompanyProfile, PROFILE_FIELD_NAMES
from company_profile.company_enrichment_service import (
    CompanyEnrichmentService,
    get_company_profile,
)

__all__ = (
    "CompanyProfile",
    "CompanyEnrichmentService",
    "PROFILE_FIELD_NAMES",
    "get_company_profile",
)
