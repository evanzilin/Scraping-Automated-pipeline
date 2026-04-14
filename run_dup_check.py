"""
Backward-compatible entry point: duplicate-check implementation lives in ``single_email_check``.

Prefer::

    python single_email_check.py -i workbook.xlsx --duplicate-check-only
    python single_email_check.py -i workbook.xlsx --duplicate-check-after --write-validation-columns

Or import ``run_duplicate_check_phase`` from ``single_email_check``.
"""

from __future__ import annotations

import asyncio

from single_email_check import (
    DUP_CHECK_BATCH_SIZE,
    DUP_CHECK_CONCURRENCY,
    DUP_CHECK_SAVE_INTERVAL,
    XLSX_PATH,
    SHEET_NAME,
    check_duplicate_batch as check_batch,
    is_duplicate_async,
    prepare_dataframe_for_duplicate_check,
    run_duplicate_check_phase,
)

# Legacy names (scripts that edited these paths).
XLSX = str(XLSX_PATH)
SHEET = SHEET_NAME
BATCH_SIZE = DUP_CHECK_BATCH_SIZE
CONCURRENCY = DUP_CHECK_CONCURRENCY
SAVE_INTERVAL = DUP_CHECK_SAVE_INTERVAL


def main() -> None:
    asyncio.run(
        run_duplicate_check_phase(
            XLSX,
            sheet_name=SHEET,
            batch_size=BATCH_SIZE,
            concurrency=CONCURRENCY,
        )
    )


if __name__ == "__main__":
    main()
