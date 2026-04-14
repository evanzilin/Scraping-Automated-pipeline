"""PostgreSQL access via asyncpg pool — batched reads only."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.company_enrichment.models import SchemaMapping

logger = logging.getLogger(__name__)


class CompanyEnrichmentRepository:
    __slots__ = ("_pool", "_m")

    def __init__(self, pool: Any, schema: Optional[SchemaMapping] = None) -> None:
        self._pool = pool
        self._m = schema or SchemaMapping()

    def _leads_select_columns(self) -> str:
        m = self._m
        cols = [
            m.col_company_id,
            m.col_lead_id,
            m.col_email,
            m.col_email_domain,
            m.col_claimed_website,
            m.col_claimed_linkedin,
            m.col_claimed_employees,
            m.col_claimed_industry,
            m.col_claimed_sub_industry,
            m.col_claimed_hq_country,
            m.col_claimed_hq_state,
            m.col_claimed_hq_city,
        ]
        return ", ".join(cols)

    async def fetch_target_records(self, limit: int, offset: int = 0) -> List[Dict[str, Any]]:
        m = self._m
        sql = (
            f"SELECT {self._leads_select_columns()} "
            f"FROM {m.leads_table} "
            f"WHERE {m.leads_pending_predicate} "
            f"ORDER BY {m.leads_pk_column} "
            f"LIMIT $1 OFFSET $2"
        )
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, limit, offset)
        out = [dict(r) for r in rows]
        logger.info("fetch_target_records count=%s", len(out))
        return out

    async def fetch_existing_company_metadata_by_ids(
        self, company_ids: Sequence[str]
    ) -> Dict[str, Dict[str, Any]]:
        ids = [str(x) for x in company_ids if x is not None and str(x).strip()]
        if not ids:
            return {}
        m = self._m
        cols = [
            m.comp_col_id,
            m.comp_col_domain,
            m.comp_col_website,
            m.comp_col_linkedin,
            m.comp_col_employees,
            m.comp_col_industry,
            m.comp_col_sub_industry,
            m.comp_col_hq_country,
            m.comp_col_hq_state,
            m.comp_col_hq_city,
        ]
        col_sql = ", ".join(cols)
        sql = f"SELECT {col_sql} FROM {m.companies_table} WHERE {m.comp_col_id} = ANY($1::text[])"
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, ids)
        by_id: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            d = dict(r)
            key = str(d.get(m.comp_col_id))
            by_id[key] = d
        logger.info("fetch companies by id count=%s", len(by_id))
        return by_id

    async def fetch_existing_company_metadata_by_domains(
        self, domains: Sequence[str]
    ) -> Dict[str, Dict[str, Any]]:
        doms = sorted({d.strip().lower() for d in domains if d and d.strip()})
        if not doms:
            return {}
        m = self._m
        cols = [
            m.comp_col_id,
            m.comp_col_domain,
            m.comp_col_website,
            m.comp_col_linkedin,
            m.comp_col_employees,
            m.comp_col_industry,
            m.comp_col_sub_industry,
            m.comp_col_hq_country,
            m.comp_col_hq_state,
            m.comp_col_hq_city,
        ]
        col_sql = ", ".join(cols)
        sql = (
            f"SELECT {col_sql} FROM {m.companies_table} "
            f"WHERE lower(trim({m.comp_col_domain})) = ANY($1::text[])"
        )
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, doms)
        by_domain: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            d = dict(r)
            raw = d.get(m.comp_col_domain)
            key = str(raw).strip().lower() if raw is not None else ""
            if key:
                by_domain[key] = d
        logger.info("fetch companies by domain count=%s", len(by_domain))
        return by_domain


async def create_pool(dsn: str, *, min_size: int = 2, max_size: int = 16) -> Any:
    import asyncpg

    return await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size)
