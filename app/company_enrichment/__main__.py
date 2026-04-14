"""``python -m app.company_enrichment --limit 100`` (requires DATABASE_URL + SCRAPE_API_KEY)."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

from app.company_enrichment.runner import process_company_enrichment_batch


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description="Run one company enrichment batch")
    p.add_argument("--limit", type=int, default=500)
    p.add_argument("--offset", type=int, default=0)
    p.add_argument("--memory-cache", action="store_true", help="Use in-process cache (no Redis)")
    p.add_argument("--dynamic-scrape", action="store_true", help="Second pass with Scrapingdog dynamic=true")
    args = p.parse_args()
    try:
        out = asyncio.run(
            process_company_enrichment_batch(
                args.limit,
                offset=args.offset,
                use_memory_cache=args.memory_cache,
                try_dynamic_scrape=args.dynamic_scrape,
            )
        )
    except Exception as e:
        print(e, file=sys.stderr)
        raise SystemExit(1) from e
    json.dump(out, sys.stdout, indent=2, default=str)


if __name__ == "__main__":
    main()
