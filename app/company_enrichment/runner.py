"""Entrypoint: wire pool, cache, Scrapingdog, and run ``process_company_enrichment_batch``."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

from app.company_enrichment.cache_service import MemoryCache, RedisCache
from app.company_enrichment.models import BatchSummary, SchemaMapping
from app.company_enrichment.repository import CompanyEnrichmentRepository, create_pool
from app.company_enrichment.scrapingdog_client import ScrapingdogEnrichmentClient
from app.company_enrichment.service import CompanyEnrichmentService
from scrape_api_key import get_scrape_api_key

logger = logging.getLogger(__name__)


async def process_company_enrichment_batch(
    limit: int = 500,
    *,
    offset: int = 0,
    database_url: Optional[str] = None,
    redis_url: Optional[str] = None,
    scrapingdog_api_key: Optional[str] = None,
    schema: Optional[SchemaMapping] = None,
    use_memory_cache: bool = False,
    scrape_concurrency: int = 12,
    try_dynamic_scrape: bool = False,
    pool: Any = None,
    close_pool_after: bool = True,
) -> Dict[str, Any]:
    """
    Run one enrichment batch.

    Environment (if args omitted):
    - ``DATABASE_URL`` / ``POSTGRES_DSN``
    - ``REDIS_URL`` (optional if ``use_memory_cache=True``)
    - ``SCRAPE_API_KEY`` (or legacy ``SCRAPINGDOG_API_KEY``)

    Returns a JSON-serializable dict with ``summary`` and ``results``.
    """
    dsn = database_url or os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("database_url or DATABASE_URL / POSTGRES_DSN is required")

    key = (scrapingdog_api_key or get_scrape_api_key() or "").strip()
    if not key:
        logger.warning("SCRAPE_API_KEY missing — enrichment scrape will return empty")

    own_pool = pool is None
    if pool is None:
        pool = await create_pool(dsn)

    try:
        if use_memory_cache or not (redis_url or os.environ.get("REDIS_URL")):
            cache = MemoryCache()
        else:
            import redis.asyncio as redis

            r = redis.from_url(redis_url or os.environ.get("REDIS_URL", ""), decode_responses=False)
            cache = RedisCache(r)

        sd = ScrapingdogEnrichmentClient(key, max_concurrent=scrape_concurrency)
        try:
            repo = CompanyEnrichmentRepository(pool, schema)
            svc = CompanyEnrichmentService(
                repo,
                cache,
                sd,
                schema=schema,
                try_dynamic_scrape=try_dynamic_scrape,
            )
            summary, results = await svc.process_batch(limit, offset)
        finally:
            await sd.aclose()

        out_summary: BatchSummary = summary
        return {
            "summary": out_summary.to_dict(),
            "results": [r.to_dict() for r in results],
        }
    finally:
        if own_pool and close_pool_after and pool is not None:
            await pool.close()


# Example (async context):
#
#   import asyncio
#   from app.company_enrichment.runner import process_company_enrichment_batch
#
#   async def main():
#       report = await process_company_enrichment_batch(
#           200,
#           use_memory_cache=True,
#       )
#       print(report["summary"])
#
#   asyncio.run(main())
