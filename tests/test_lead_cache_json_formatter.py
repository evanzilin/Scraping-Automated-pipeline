"""Tests for lead_cache_json: industry pairing, employee buckets, validation, file output."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lead_cache_json.industry_normalizer import (
    classify_industry_sub_from_description_in_scope,
    coerce_industry_sub_to_taxonomy_scope,
    normalize_industry_pair,
    refine_raw_industry_sub_taxonomy_fallback,
    resolve_taxonomy_sub_key_scope,
)
from lead_cache_json.json_contract import K_EMAIL, SOURCE_UNCLEAR_VALUE
from lead_cache_json.json_formatter import format_cached_dataset_to_json
from lead_cache_json.record_normalizer import normalize_employee_count_bucket
from lead_cache_json.validators import validate_output_record

# ---------------------------------------------------------------------------
# Sample records (documentation + tests)
# ---------------------------------------------------------------------------

SAMPLE_CACHE_RECORD: dict = {
    "company": "Acme Analytics GmbH",
    "first_name": "Jane",
    "last_name": "Doe",
    "email": "jane.doe@acme-analytics.example",
    "role": "Chief Technology Officer",
    "website": "acme-analytics.example",
    "industry": "Software",
    "sub_industry": "SaaS",
    "country": "Germany",
    "city": "Berlin",
    "linkedin": "https://www.linkedin.com/in/janedoe",
    "company_linkedin": "https://www.linkedin.com/company/acme-analytics",
    "description": "B2B analytics platform for operations teams.",
    "employee_count": "51-200 employees",
    "hq_country": "Germany",
    "hq_city": "Berlin",
    "source_url": "https://acme-analytics.example/about",
    "source_type": "company_website",
    "phone_numbers": ["+49 30 1234567"],
    "socials": {"twitter": "https://twitter.com/acmeanalytics"},
}

SAMPLE_OUTPUT_RECORD: dict = {
    "business": "Acme Analytics GmbH",
    "full_name": "Jane Doe",
    "first": "Jane",
    "last": "Doe",
    "email": "jane.doe@acme-analytics.example",
    "role": "Chief Technology Officer",
    "website": "https://acme-analytics.example/",
    "industry": "Software",
    "sub_industry": "SaaS",
    "country": "Germany",
    "state": None,
    "city": "Berlin",
    "linkedin": "https://linkedin.com/in/janedoe",
    "company_linkedin": "https://linkedin.com/company/acme-analytics/",
    "source_url": "https://acme-analytics.example/about/",
    "description": "B2B analytics platform for operations teams.",
    "employee_count": "51-200",
    "hq_country": "Germany",
    "hq_state": None,
    "hq_city": "Berlin",
    "source_type": "company_website",
    "phone_numbers": ["+49 30 1234567"],
    "socials": {"twitter": "https://twitter.com/acmeanalytics/"},
}


def test_normalize_industry_pair_valid_software_saas() -> None:
    ind, sub, reason = normalize_industry_pair("Software", "SaaS")
    assert reason is None
    assert ind == "Software"
    assert sub == "SaaS"


def test_normalize_industry_pair_rejects_gibberish() -> None:
    ind, sub, reason = normalize_industry_pair("ZZZNotARealIndustry999", "ZZZNotASub888")
    assert reason is not None
    assert ind is None
    assert sub is None


def test_coerce_industry_sub_to_taxonomy_scope_canonical_parent_and_sub_key() -> None:
    ind, sub = coerce_industry_sub_to_taxonomy_scope("Computer Software", "saas")
    assert ind == "Software"
    assert sub == "SaaS"


def test_coerce_industry_sub_to_taxonomy_scope_linkedin_it_label() -> None:
    ind, sub = coerce_industry_sub_to_taxonomy_scope(
        "IT Services and IT Consulting", None
    )
    assert ind == "Information Technology"
    assert sub is None


def test_linkedin_technology_information_internet_maps_to_information_technology() -> None:
    ind, sub = refine_raw_industry_sub_taxonomy_fallback(
        "Technology, Information and Internet", None
    )
    assert ind == "Information Technology"
    assert sub is None


def test_classify_in_scope_parent_software_prefers_saas() -> None:
    desc = (
        "We develop a software product and sell it via a subscription over the internet "
        "rather than allowing users to download the application to their computers."
    )
    ind, sub, err = classify_industry_sub_from_description_in_scope(
        desc,
        allowed_parent_industries=["Software"],
        industry_hint="Software",
    )
    assert err is None
    assert ind == "Software"
    assert sub == "SaaS"


def test_classify_in_scope_sub_intersection_empty_fails() -> None:
    desc = (
        "We develop a software product and sell it via a subscription over the internet."
    )
    ind, sub, err = classify_industry_sub_from_description_in_scope(
        desc,
        allowed_sub_industries=["SaaS"],
        allowed_parent_industries=["Mining"],
    )
    assert ind is None and sub is None
    assert err == "empty_taxonomy_scope"


def test_resolve_taxonomy_scope_sub_only() -> None:
    full = resolve_taxonomy_sub_key_scope()
    assert "SaaS" in full
    sub_only = resolve_taxonomy_sub_key_scope(allowed_sub_industries=["SaaS"])
    assert sub_only == frozenset({"SaaS"})


def test_normalize_employee_count_bucket_variants() -> None:
    assert normalize_employee_count_bucket("11-50 employees") == "11-50"
    assert normalize_employee_count_bucket("1,001-5,000") == "1,001-5,000"
    assert normalize_employee_count_bucket(150) == "51-200"
    assert normalize_employee_count_bucket("10001+") == "10,001+"


def test_validate_output_record_ok_minimal_extra_fields() -> None:
    row = dict(SAMPLE_OUTPUT_RECORD)
    ok, reasons = validate_output_record(row)
    assert ok, reasons


def test_validate_output_record_fails_missing_email() -> None:
    row = dict(SAMPLE_OUTPUT_RECORD)
    row[K_EMAIL] = ""
    ok, reasons = validate_output_record(row)
    assert not ok
    assert any("email" in r for r in reasons)


def test_validate_us_requires_state() -> None:
    row = dict(SAMPLE_OUTPUT_RECORD)
    row["country"] = "United States"
    row["state"] = ""
    row["hq_country"] = "United States"
    row["hq_state"] = "California"
    ok, reasons = validate_output_record(row)
    assert not ok
    assert any("state_required" in r for r in reasons)


def test_validate_output_record_fails_name_email_mismatch() -> None:
    row = dict(SAMPLE_OUTPUT_RECORD)
    row["first"] = "Muhammad"
    row["last"] = "Arshad"
    row["full_name"] = "Muhammad Arshad"
    row["email"] = "waqas@twes.us"
    ok, reasons = validate_output_record(row)
    assert not ok
    assert "name_email_mismatch" in reasons


def test_validate_output_record_requires_proprietary_source_type_match() -> None:
    row = dict(SAMPLE_OUTPUT_RECORD)
    row["source_url"] = "proprietary_database"
    row["source_type"] = "company_website"
    ok, reasons = validate_output_record(row)
    assert not ok
    assert "source_type_must_match_proprietary_database" in reasons


def test_format_cached_dataset_to_json_writes_files(tmp_path: Path) -> None:
    out = tmp_path / "json-date.json"
    rej = tmp_path / "json-date.rejects.json"
    valid = format_cached_dataset_to_json(
        [SAMPLE_CACHE_RECORD],
        output_path=out,
        reject_path=rej,
        write_files=True,
    )
    assert len(valid) == 1
    data = json.loads(out.read_text(encoding="utf-8"))
    assert len(data) == 1
    assert data[0]["email"] == SAMPLE_CACHE_RECORD["email"]
    rejects = json.loads(rej.read_text(encoding="utf-8"))
    assert rejects == []


def test_sample_output_matches_formatter_shape() -> None:
    """Sanity: formatter output keys cover the documented sample (URLs may differ slightly)."""
    valid = format_cached_dataset_to_json([SAMPLE_CACHE_RECORD], write_files=False)
    assert set(valid[0].keys()) == set(SAMPLE_OUTPUT_RECORD.keys())
    assert valid[0]["industry"] == SAMPLE_OUTPUT_RECORD["industry"]
    assert valid[0]["sub_industry"] == SAMPLE_OUTPUT_RECORD["sub_industry"]
    assert valid[0]["employee_count"] == "51-200"


def test_source_type_inferred_from_website_when_no_explicit_source() -> None:
    thin = {
        "company": "X",
        "first": "A",
        "last": "B",
        "email": "a@x.com",
        "role": "CEO",
        "website": "https://x.com/",
        "industry": "Software",
        "sub_industry": "SaaS",
        "country": "Canada",
        "city": "Toronto",
        "linkedin": "https://www.linkedin.com/in/ab",
        "company_linkedin": "https://www.linkedin.com/company/x",
        "description": "Test",
        "employee_count": "2-10",
        "hq_country": "Canada",
    }
    valid = format_cached_dataset_to_json([thin], write_files=False)
    assert valid[0]["source_url"] != SOURCE_UNCLEAR_VALUE  # website fills source
    assert valid[0]["source_type"] == "company_website"
