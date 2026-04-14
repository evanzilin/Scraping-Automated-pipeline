#!/usr/bin/env python3
"""
Load leads from an Excel workbook, map them into the project's ``lead_format.json`` shape,
create PostgreSQL table ``Leads`` if needed, and insert the rows.

Behavior:
- Reads one worksheet from ``database.xlsx``-style files.
- Maps available Excel columns into the lead-format scope.
- Missing scalar fields become ``NULL`` in PostgreSQL.
- ``phone_numbers`` / ``socials`` stay JSON collections (``[]`` / ``{}`` when empty).

Connection:
- Uses ``--dsn`` if provided.
- Otherwise uses ``DATABASE_URL`` or ``POSTGRES_DSN`` from env / ``.env``.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

import pandas as pd

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


LEAD_COLUMNS = (
    "business",
    "full_name",
    "first",
    "last",
    "email",
    "role",
    "seniority",
    "website",
    "industry",
    "sub_industry",
    "country",
    "state",
    "city",
    "linkedin",
    "company_linkedin",
    "source_url",
    "description",
    "employee_count",
    "hq_country",
    "hq_state",
    "hq_city",
    "phone_numbers",
    "socials",
)

FIELD_ALIASES: Dict[str, tuple[str, ...]] = {
    "business": ("business", "company name"),
    "full_name": ("full_name", "full name", "name"),
    "first": ("first", "first name", "first_name"),
    "last": ("last", "last name", "last_name"),
    "email": ("email", "personal_email", "personal email", "work email"),
    "role": ("role", "title", "job title", "person_title"),
    "seniority": ("seniority",),
    "website": ("website", "company website", "url", "company url"),
    "industry": ("industry",),
    "sub_industry": ("sub_industry", "sub industry", "sub-industry"),
    "country": ("country",),
    "state": ("state",),
    "city": ("city",),
    "linkedin": ("linkedin", "person linkedin url", "linkedin url"),
    "company_linkedin": (
        "company_linkedin",
        "company linkedin",
        "company linkedin url",
        "linkedin company url",
    ),
    "source_url": ("source_url",),
    "description": ("description",),
    "employee_count": ("employee_count", "employee count"),
    "hq_country": ("hq_country", "company country", "hq country"),
    "hq_state": ("hq_state", "company state", "hq state"),
    "hq_city": ("hq_city", "company city", "hq city"),
}

PHONE_ALIASES = ("phone", "phone number", "mobile", "telephone")
SOCIALS_ALIASES = ("socials",)
SOURCE_URL_DEFAULT = "proprietary_database"


def _normalize_key(s: str) -> str:
    return " ".join(str(s).strip().lower().replace("_", " ").replace("-", " ").split())


def _none_if_blank(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def _absolute_http_url(url: Optional[str]) -> Optional[str]:
    s = _none_if_blank(url)
    if not s:
        return None
    if s.startswith("//"):
        return "https:" + s
    if not s.startswith(("http://", "https://")):
        return "https://" + s.lstrip("/")
    return s


def _host_like(s: Optional[str]) -> bool:
    t = _none_if_blank(s)
    if not t:
        return False
    if "://" in t:
        try:
            return bool(urlparse(t).hostname)
        except Exception:
            return False
    return "." in t and " " not in t


def _pick_value(row: Dict[str, Any], *aliases: str) -> Any:
    for alias in aliases:
        k = _normalize_key(alias)
        if k in row:
            return row[k]
    return None


def _parse_socials(value: Any) -> Dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {
            str(k): str(v).strip()
            for k, v in value.items()
            if _none_if_blank(v)
        }
    raw = _none_if_blank(value)
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    return {
        str(k): str(v).strip()
        for k, v in obj.items()
        if _none_if_blank(v)
    }


def _row_to_casefold_map(row: pd.Series) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in row.items():
        nk = _normalize_key(str(k))
        if nk and nk not in out:
            out[nk] = v
    return out


def map_excel_row_to_lead(row: pd.Series) -> Dict[str, Any]:
    src = _row_to_casefold_map(row)
    first = _none_if_blank(_pick_value(src, *FIELD_ALIASES["first"]))
    last = _none_if_blank(_pick_value(src, *FIELD_ALIASES["last"]))
    full_name = _none_if_blank(_pick_value(src, *FIELD_ALIASES["full_name"]))
    if not full_name:
        full_name = _none_if_blank(" ".join(x for x in (first or "", last or "") if x).strip())

    website_raw = _pick_value(src, *FIELD_ALIASES["website"])
    website = _absolute_http_url(_none_if_blank(website_raw))
    linkedin = _absolute_http_url(_none_if_blank(_pick_value(src, *FIELD_ALIASES["linkedin"])))
    company_linkedin = _absolute_http_url(
        _none_if_blank(_pick_value(src, *FIELD_ALIASES["company_linkedin"]))
    )

    phone_values: List[str] = []
    for alias in PHONE_ALIASES:
        val = _none_if_blank(_pick_value(src, alias))
        if val and val not in phone_values:
            phone_values.append(val)

    socials = _parse_socials(_pick_value(src, *SOCIALS_ALIASES))

    lead: Dict[str, Any] = {
        "business": _none_if_blank(_pick_value(src, *FIELD_ALIASES["business"])),
        "full_name": full_name,
        "first": first,
        "last": last,
        "email": _none_if_blank(_pick_value(src, *FIELD_ALIASES["email"])),
        "role": _none_if_blank(_pick_value(src, *FIELD_ALIASES["role"])),
        "seniority": _none_if_blank(_pick_value(src, *FIELD_ALIASES["seniority"])),
        "website": website,
        "industry": _none_if_blank(_pick_value(src, *FIELD_ALIASES["industry"])),
        "sub_industry": _none_if_blank(_pick_value(src, *FIELD_ALIASES["sub_industry"])),
        "country": _none_if_blank(_pick_value(src, *FIELD_ALIASES["country"])),
        "state": _none_if_blank(_pick_value(src, *FIELD_ALIASES["state"])),
        "city": _none_if_blank(_pick_value(src, *FIELD_ALIASES["city"])),
        "linkedin": linkedin,
        "company_linkedin": company_linkedin,
        "source_url": _none_if_blank(_pick_value(src, *FIELD_ALIASES["source_url"]))
        or SOURCE_URL_DEFAULT,
        "description": _none_if_blank(_pick_value(src, *FIELD_ALIASES["description"])),
        "employee_count": _none_if_blank(_pick_value(src, *FIELD_ALIASES["employee_count"])),
        "hq_country": _none_if_blank(_pick_value(src, *FIELD_ALIASES["hq_country"])),
        "hq_state": _none_if_blank(_pick_value(src, *FIELD_ALIASES["hq_state"])),
        "hq_city": _none_if_blank(_pick_value(src, *FIELD_ALIASES["hq_city"])),
        "phone_numbers": phone_values,
        "socials": socials,
    }

    if not lead["website"]:
        for candidate in (
            lead["company_linkedin"],
            lead["linkedin"],
            _pick_value(src, "website"),
        ):
            if _host_like(candidate):
                lead["website"] = _absolute_http_url(_none_if_blank(candidate))
                break

    for key in LEAD_COLUMNS:
        if key not in lead:
            lead[key] = None
    return lead


def load_excel_leads(path: Path, sheet_name: Optional[str]) -> List[Dict[str, Any]]:
    df = pd.read_excel(
        path,
        sheet_name=sheet_name if sheet_name is not None else 0,
        engine="openpyxl",
    )
    if df.empty:
        return []
    return [map_excel_row_to_lead(row) for _, row in df.iterrows()]


def _load_dsn(explicit: Optional[str]) -> str:
    dsn = (explicit or "").strip() or os.environ.get("DATABASE_URL", "").strip() or os.environ.get(
        "POSTGRES_DSN", ""
    ).strip()
    if not dsn:
        raise SystemExit("Set --dsn or DATABASE_URL / POSTGRES_DSN.")
    return dsn


def create_leads_table(cur: Any) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public."Leads" (
            id BIGSERIAL PRIMARY KEY,
            business TEXT NULL,
            full_name TEXT NULL,
            first TEXT NULL,
            last TEXT NULL,
            email TEXT NULL,
            role TEXT NULL,
            seniority TEXT NULL,
            website TEXT NULL,
            industry TEXT NULL,
            sub_industry TEXT NULL,
            country TEXT NULL,
            state TEXT NULL,
            city TEXT NULL,
            linkedin TEXT NULL,
            company_linkedin TEXT NULL,
            source_url TEXT NULL,
            description TEXT NULL,
            employee_count TEXT NULL,
            hq_country TEXT NULL,
            hq_state TEXT NULL,
            hq_city TEXT NULL,
            phone_numbers JSONB NOT NULL DEFAULT '[]'::jsonb,
            socials JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


def insert_leads(dsn: str, leads: Iterable[Dict[str, Any]]) -> int:
    try:
        psycopg = importlib.import_module("psycopg")
        Jsonb = importlib.import_module("psycopg.types.json").Jsonb
    except ImportError as e:  # pragma: no cover
        raise SystemExit("Install psycopg first: pip install psycopg[binary]") from e

    rows = list(leads)
    if not rows:
        return 0

    cols = list(LEAD_COLUMNS)
    placeholders = ", ".join(["%s"] * len(cols))
    quoted_cols = ", ".join(f'"{c}"' for c in cols)
    insert_sql = (
        f'INSERT INTO public."Leads" ({quoted_cols}) '
        f"VALUES ({placeholders})"
    )

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            create_leads_table(cur)
            params: List[tuple[Any, ...]] = []
            for lead in rows:
                params.append(
                    tuple(
                        Jsonb(lead[c]) if c in {"phone_numbers", "socials"} else lead[c]
                        for c in cols
                    )
                )
            cur.executemany(insert_sql, params)
        conn.commit()
    return len(rows)


def build_arg_parser(default_xlsx: Path) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Map Excel rows to lead_format scope and insert into PostgreSQL table Leads."
    )
    p.add_argument(
        "--xlsx",
        type=Path,
        default=default_xlsx,
        help=f"Excel workbook path (default: {default_xlsx.name})",
    )
    p.add_argument(
        "--sheet",
        default=None,
        help="Worksheet name (default: first sheet)",
    )
    p.add_argument(
        "--dsn",
        default="",
        help="PostgreSQL DSN (default: DATABASE_URL / POSTGRES_DSN from env)",
    )
    p.add_argument(
        "--preview",
        action="store_true",
        help="Print mapped lead JSON instead of inserting into PostgreSQL",
    )
    return p


def main() -> None:
    if load_dotenv is not None:
        load_dotenv()

    default_xlsx = Path(__file__).resolve().parent / "database.xlsx"
    cli = build_arg_parser(default_xlsx).parse_args()
    xlsx_path = cli.xlsx.resolve()
    if not xlsx_path.is_file():
        raise SystemExit(f"Excel file not found: {xlsx_path}")

    leads = load_excel_leads(xlsx_path, cli.sheet)
    if cli.preview:
        print(json.dumps(leads, indent=2, ensure_ascii=False))
        return

    dsn = _load_dsn(cli.dsn)
    inserted = insert_leads(dsn, leads)
    print(
        f'Inserted {inserted} row(s) into public."Leads" from {xlsx_path}',
        flush=True,
    )


if __name__ == "__main__":
    main()
