"""
python main.py setup-db
python main.py append-file --excel-path data.xlsx
python main.py start-workers --worker-count 30 --batch-size 1000
"""

import argparse
import json
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from pathlib import Path
from typing import Any

from openpyxl import load_workbook
from psycopg import connect
from psycopg.rows import dict_row


LOGGER = logging.getLogger("provider")
DEFAULT_DSN = "postgresql://postgres:12345678@localhost:5432/Leads"


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS provider_files (
    file_name TEXT PRIMARY KEY,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS data (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_row_number BIGINT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    batch_id UUID,
    claimed_by TEXT,
    claimed_at TIMESTAMPTZ,
    acked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT data_status_check CHECK (status IN ('pending', 'in_progress', 'done')),
    CONSTRAINT data_source_row_unique UNIQUE (source_file, source_row_number)
);

CREATE INDEX IF NOT EXISTS data_pending_idx
    ON data (status, id)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS data_batch_idx
    ON data (batch_id)
    WHERE batch_id IS NOT NULL;

CREATE OR REPLACE FUNCTION claim_batch(p_worker_id TEXT, p_batch_size INTEGER)
RETURNS TABLE (
    batch_id UUID,
    id BIGINT,
    source_file TEXT,
    source_row_number BIGINT,
    payload JSONB
) AS $$
DECLARE
    v_batch_id UUID := gen_random_uuid();
BEGIN
    RETURN QUERY
    WITH picked AS (
        SELECT d.id
        FROM data AS d
        WHERE d.status = 'pending'
        ORDER BY d.id
        FOR UPDATE SKIP LOCKED
        LIMIT p_batch_size
    ),
    updated AS (
        UPDATE data AS d
        SET status = 'in_progress',
            batch_id = v_batch_id,
            claimed_by = p_worker_id,
            claimed_at = NOW()
        FROM picked
        WHERE d.id = picked.id
        RETURNING d.id, d.source_file, d.source_row_number, d.payload
    )
    SELECT v_batch_id, u.id, u.source_file, u.source_row_number, u.payload
    FROM updated AS u
    ORDER BY u.id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ack_batch(p_worker_id TEXT, p_batch_id UUID)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    UPDATE data
    SET status = 'done',
        acked_at = NOW()
    WHERE batch_id = p_batch_id
      AND claimed_by = p_worker_id
      AND status = 'in_progress';

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

"""


def get_connection(dsn: str):
    return connect(dsn, autocommit=False, row_factory=dict_row)


def setup_database(dsn: str) -> None:
    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
            cur.execute(SCHEMA_SQL)
        conn.commit()


def load_excel_headers(excel_path: Path) -> list[str]:
    workbook = load_workbook(filename=excel_path, read_only=True, data_only=True)
    worksheet = workbook.active
    try:
        header_values = next(worksheet.iter_rows(min_row=1, max_row=1, values_only=True))
    finally:
        workbook.close()
    return ["" if value is None else str(value) for value in header_values]


def append_excel_file(dsn: str, excel_path: Path) -> int:
    excel_path = excel_path.resolve()
    if not excel_path.exists():
        raise FileNotFoundError(f"Excel file not found: {excel_path}")

    file_name = excel_path.name
    headers = load_excel_headers(excel_path)

    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM provider_files WHERE file_name = %s",
                (file_name,),
            )
            if cur.fetchone():
                conn.rollback()
                LOGGER.info("File %s was already loaded. Skipping.", file_name)
                return 0

        workbook = load_workbook(filename=excel_path, read_only=True, data_only=True)
        worksheet = workbook.active
        inserted = 0
        try:
            with conn.cursor() as cur:
                for row_number, row_values in enumerate(
                    worksheet.iter_rows(min_row=2, values_only=True),
                    start=2,
                ):
                    payload = {
                        headers[index]: row_values[index] if index < len(row_values) else None
                        for index in range(len(headers))
                    }
                    cur.execute(
                        """
                        INSERT INTO data (source_file, source_row_number, payload)
                        VALUES (%s, %s, %s::jsonb)
                        ON CONFLICT (source_file, source_row_number) DO NOTHING
                        """,
                        (file_name, row_number, json.dumps(payload, default=str)),
                    )
                    inserted += 1

                cur.execute(
                    """
                    INSERT INTO provider_files (file_name)
                    VALUES (%s)
                    ON CONFLICT (file_name) DO NOTHING
                    """,
                    (file_name,),
                )
            conn.commit()
        finally:
            workbook.close()

    LOGGER.info("Loaded %s rows from %s", inserted, file_name)
    return inserted


def claim_batch(dsn: str, worker_id: str, batch_size: int = 1000) -> dict[str, Any]:
    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM claim_batch(%s, %s)",
                (worker_id, batch_size),
            )
            rows = cur.fetchall()
        conn.commit()

    if not rows:
        return {"batch_id": None, "rows": []}

    batch_id = str(rows[0]["batch_id"])
    data_rows = [
        {
            "id": row["id"],
            "source_file": row["source_file"],
            "source_row_number": row["source_row_number"],
            "payload": row["payload"],
        }
        for row in rows
    ]
    return {"batch_id": batch_id, "rows": data_rows}


def ack_batch(dsn: str, worker_id: str, batch_id: str) -> int:
    with get_connection(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ack_batch(%s, %s::uuid) AS updated_count",
                (worker_id, batch_id),
            )
            result = cur.fetchone()
        conn.commit()
    return int(result["updated_count"])


def process_rows(rows: list[dict[str, Any]]) -> None:
    """Implement your worker logic here."""
    return None


def worker_loop(dsn: str, worker_id: str, batch_size: int = 1000) -> int:
    processed_total = 0

    while True:
        batch = claim_batch(dsn, worker_id, batch_size)
        batch_id = batch["batch_id"]
        rows = batch["rows"]
        if not batch_id:
            LOGGER.info("Worker %s found no more pending rows.", worker_id)
            return processed_total

        process_rows(rows)
        updated_count = ack_batch(dsn, worker_id, batch_id)
        processed_total += updated_count
        LOGGER.info(
            "Worker %s processed batch %s with %s rows.",
            worker_id,
            batch_id,
            updated_count,
        )


def start_workers(dsn: str, worker_count: int = 30, batch_size: int = 1000) -> int:
    total_processed = 0
    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(worker_loop, dsn, f"worker-{index:02d}", batch_size)
            for index in range(1, worker_count + 1)
        ]
        for future in as_completed(futures):
            total_processed += future.result()

    LOGGER.info(
        "All workers finished. Total acknowledged rows: %s",
        total_processed,
    )
    return total_processed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PostgreSQL-backed batch provider")
    parser.add_argument(
        "--dsn",
        default=DEFAULT_DSN,
        help="PostgreSQL DSN. Defaults to the built-in local Leads database DSN.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("setup-db", help="Create required tables and SQL functions.")

    append_parser = subparsers.add_parser("append-file", help="Append an Excel file into data.")
    append_parser.add_argument("--excel-path", required=True, help="Path to the Excel file.")
    workers_parser = subparsers.add_parser(
        "start-workers",
        help="Start worker processes that claim and acknowledge batches until data is exhausted.",
    )
    workers_parser.add_argument("--worker-count", type=int, default=30)
    workers_parser.add_argument("--batch-size", type=int, default=1000)

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.command == "setup-db":
        setup_database(args.dsn)
        return 0

    if args.command == "append-file":
        append_excel_file(args.dsn, Path(args.excel_path))
        return 0

    if args.command == "start-workers":
        start_workers(args.dsn, args.worker_count, args.batch_size)
        return 0

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
