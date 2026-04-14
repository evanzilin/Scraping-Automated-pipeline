"""PostgreSQL ``company_dataset`` — source of truth reads and upserts."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from company_profile.company_contract import PROFILE_FIELD_NAMES, CompanyProfile

logger = logging.getLogger(__name__)

TABLE = "company_dataset"


class PostgresCompanyRepository:
    """Access ``company_dataset`` with explicit column list matching ``company_contract``."""

    def __init__(self, dsn: str) -> None:
        import psycopg

        self._dsn = dsn
        self._connect = psycopg.connect

    @classmethod
    def from_env(cls) -> PostgresCompanyRepository:
        dsn = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_DSN")
        if not dsn:
            raise RuntimeError("DATABASE_URL or POSTGRES_DSN is required")
        return cls(dsn)

    def fetch_by_id(self, company_id: str) -> Optional[Dict[str, Any]]:
        """Load one row by primary key ``id``."""
        if not company_id:
            return None
        cols = ", ".join(PROFILE_FIELD_NAMES)
        sql = f"SELECT {cols} FROM {TABLE} WHERE id = %s"
        try:
            with self._connect(self._dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (company_id,))
                    row = cur.fetchone()
                    if not row:
                        logger.info("Postgres: no row id=%s", company_id)
                        return None
                    names = [d[0] for d in cur.description]
                    return dict(zip(names, row))
        except Exception as e:
            logger.exception("Postgres fetch_by_id failed: %s", e)
            raise

    def fetch_by_domain(self, company_domain: str) -> Optional[Dict[str, Any]]:
        """Fallback lookup by domain (not used in ES-first flow unless you call it explicitly)."""
        domain = (company_domain or "").strip().lower()
        if not domain:
            return None
        cols = ", ".join(PROFILE_FIELD_NAMES)
        sql = f"SELECT {cols} FROM {TABLE} WHERE lower(company_domain) = %s"
        try:
            with self._connect(self._dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (domain,))
                    row = cur.fetchone()
                    if not row:
                        return None
                    names = [d[0] for d in cur.description]
                    return dict(zip(names, row))
        except Exception as e:
            logger.exception("Postgres fetch_by_domain failed: %s", e)
            raise

    def upsert_company(self, profile: CompanyProfile) -> None:
        """
        Insert or update on ``company_domain`` unique constraint.

        Expects table::

            id, company_domain, company_website_url, employee_count, industry,
            sub_industry, company_linkedin_url,
            hq_location_country, hq_location_state, hq_location_city,
            source_url, source_type

        Adjust column types in your DB; ``id`` is TEXT.
        """
        p = profile
        if not p.company_domain:
            raise ValueError("company_domain required")
        pid = p.id or p.company_domain
        sql = f"""
        INSERT INTO {TABLE} (
            id, company_domain, company_website_url, employee_count, industry, sub_industry,
            company_linkedin_url, hq_location_country, hq_location_state, hq_location_city,
            source_url, source_type
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (company_domain) DO UPDATE SET
            company_website_url = EXCLUDED.company_website_url,
            employee_count = EXCLUDED.employee_count,
            industry = EXCLUDED.industry,
            sub_industry = EXCLUDED.sub_industry,
            company_linkedin_url = EXCLUDED.company_linkedin_url,
            hq_location_country = EXCLUDED.hq_location_country,
            hq_location_state = EXCLUDED.hq_location_state,
            hq_location_city = EXCLUDED.hq_location_city,
            source_url = EXCLUDED.source_url,
            source_type = EXCLUDED.source_type
        """
        try:
            with self._connect(self._dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        (
                            pid,
                            p.company_domain,
                            p.company_website_url,
                            p.employee_count,
                            p.industry,
                            p.sub_industry,
                            p.company_linkedin_url,
                            p.hq_location_country,
                            p.hq_location_state,
                            p.hq_location_city,
                            p.source_url,
                            p.source_type,
                        ),
                    )
                conn.commit()
            logger.info("Postgres: upsert domain=%s id=%s", p.company_domain, pid)
        except Exception as e:
            logger.exception("Postgres upsert failed: %s", e)
            raise
