"""Unit tests for company_profile (mocked ES / PG / Scrapingdog)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from company_profile.company_contract import CompanyProfile
from company_profile.company_enrichment_service import CompanyEnrichmentService
from company_profile.normalizer import normalize_domain, normalize_profile
from company_profile.sufficiency import is_sufficient


def test_normalize_domain_strips_www() -> None:
    assert normalize_domain("WWW.Example.COM/path") == "example.com"


def test_is_sufficient_requires_url_and_signal() -> None:
    assert not is_sufficient({})
    assert not is_sufficient({"company_website_url": "https://x.com"})
    assert is_sufficient(
        {
            "company_website_url": "https://x.com",
            "industry": "Software",
        }
    )
    assert is_sufficient(
        {
            "company_linkedin_url": "https://www.linkedin.com/company/foo",
            "employee_count": "100",
        }
    )


def test_normalize_profile_clears_state_for_non_us() -> None:
    p = CompanyProfile(
        company_domain="foo.de",
        hq_location_country="DE",
        hq_location_state="BY",
    )
    n = normalize_profile(p)
    assert n.hq_location_state is None


def test_get_company_profile_sufficient_from_pg() -> None:
    pg_row = {
        "id": "c1",
        "company_domain": "acme.com",
        "company_website_url": "https://acme.com",
        "industry": "Manufacturing",
        "sub_industry": None,
        "employee_count": None,
        "company_linkedin_url": None,
        "hq_location_country": "US",
        "hq_location_state": "CA",
        "hq_location_city": "SF",
        "source_url": "https://acme.com",
        "source_type": "company_website",
    }
    es = MagicMock()
    es.find_company_id_by_domain.return_value = "c1"
    pg = MagicMock()
    pg.fetch_by_id.return_value = pg_row

    svc = CompanyEnrichmentService(es=es, pg=pg, scrapingdog_api_key="")
    out = svc.get_company_profile("acme.com")

    assert out["company_domain"] == "acme.com"
    assert out.get("industry") == "manufacturing"
    pg.upsert_company.assert_not_called()
    es.index_company.assert_not_called()


def test_get_company_profile_scrape_when_es_miss() -> None:
    es = MagicMock()
    es.find_company_id_by_domain.return_value = None
    pg = MagicMock()
    pg.fetch_by_id.return_value = None

    fake_scrape = {
        "company_domain": "newco.io",
        "company_website_url": "https://newco.io/",
        "industry": "SaaS",
        "source_url": "https://newco.io/",
        "source_type": "company_website",
    }

    with patch("company_profile.company_enrichment_service.scrape_domain", return_value=fake_scrape):
        svc = CompanyEnrichmentService(es=es, pg=pg, scrapingdog_api_key="k")
        out = svc.get_company_profile("newco.io")

    assert out["company_domain"] == "newco.io"
    pg.upsert_company.assert_called_once()
    es.index_company.assert_called_once()


def test_get_company_profile_insufficient_pg_triggers_scrape() -> None:
    thin_row = {
        "id": "c2",
        "company_domain": "thin.com",
        "company_website_url": "https://thin.com",
        # no industry / employee / hq => insufficient
    }
    es = MagicMock()
    es.find_company_id_by_domain.return_value = "c2"
    pg = MagicMock()
    pg.fetch_by_id.return_value = thin_row

    fake_scrape = {
        "company_domain": "thin.com",
        "industry": "Tech",
        "source_url": "https://thin.com/",
        "source_type": "company_website",
    }
    with patch("company_profile.company_enrichment_service.scrape_domain", return_value=fake_scrape):
        svc = CompanyEnrichmentService(es=es, pg=pg, scrapingdog_api_key="k")
        out = svc.get_company_profile("thin.com")

    assert out.get("industry") == "tech"
    pg.upsert_company.assert_called_once()
    es.index_company.assert_called_once()
