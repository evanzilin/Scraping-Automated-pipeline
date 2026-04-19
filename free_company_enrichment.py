"""
Company enrichment from an email domain: PostgreSQL first, then Google (via ScraperAPI).

Flow
----
1. Normalize the input (accepts ``user@domain.com`` or ``domain.com``).
2. Query table ``free_company_dataset`` (same DB as ``DATABASE_URL``) for rows whose
   domain column is similar to the input (exact match first, then fuzzy score).
3. If a good match exists and the row has a company website (or usable URL field),
   use that row as the source.
4. Otherwise fetch Google search HTML through ScraperAPI, parse results, and extract
   website, LinkedIn, employee hints, and HQ text heuristically (same downstream logic
   as before).
5. Print all fields to the terminal.
6. If ``DATABASE_URL`` / ``POSTGRES_DSN`` is set, **upsert** the ScraperAPI result into
   ``FREE_COMPANY_TABLE`` (UPDATE by domain key, else INSERT). Disable with
   ``SCRAPER_SAVE_TO_POSTGRES=0``.

Environment
-----------
- ``SCRAPE_API_KEY`` (or legacy ``SCRAPERAPI_*`` / ``SCRAPINGDOG_*`` / ``LINKEDIN_SCRAPER_*``) — required for ScraperAPI fallback
- ``DATABASE_URL`` or ``POSTGRES_DSN`` — PostgreSQL lookup; same DSN is used to **save**
  ScraperAPI enrichment (upsert). Without it, API results are only printed.
- ``SCRAPER_SAVE_TO_POSTGRES`` — optional, default on (`1`); set to `0` to skip DB writes after API.
- ``FREE_COMPANY_TABLE`` — optional, default ``free_company_dataset``. You may use ``schema.table``
  (e.g. ``Company_Dataset.free_company_dataset``). Otherwise set ``FREE_COMPANY_SCHEMA`` (default
  ``Company_Dataset``, not ``public``).
- ``FREE_COMPANY_DATABASE`` — optional; if set, replaces the database name in ``DATABASE_URL``
  (so you can point at a DB literally named ``free_company_dataset``)
- **pgvector (optional):** if the ``pgvector`` extension is enabled and the table has a column
  like ``domain_embedding vector(3)``, lookup runs **cosine-distance** search. Query vector:
  (1) hash-from-domain by default; (2) ``PGVECTOR_QUERY_VECTOR=0.1,0.2,0.3`` (or legacy
  ``DOMAIN_QUERY_VECTOR``); (3) ``PGVECTOR_USE_EXAMPLE_QUERY_VECTOR=1`` for the built-in
  example ``[0.1, 0.2, 0.3]`` (requires ``PGVECTOR_DIMS=3``). Match notes include
  **cosine_similarity** (= ``1 - cosine_distance`` for pgvector's ``<=>``). Tune
  ``PGVECTOR_MAX_COSINE_DISTANCE`` (default ``0.55``). Set ``PGVECTOR_DISABLED=1`` to skip.
  Column: ``PGVECTOR_DOMAIN_COLUMN`` (default ``domain_embedding``).

Expected columns (any subset; missing fields stay empty)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``company_domain`` or ``domain``, ``company_name`` or ``name``,
``company_website_url`` or ``website``,
``employee_count``, ``industry``, ``sub_industry``,
``company_linkedin_url`` or ``linkedin_company_url``,
``hq_location_country``, ``hq_location_state``, ``hq_location_city``
(or ``hq_country`` / ``hq_state`` / ``hq_city``).

Run
---
    python run_linkedin_enrichment.py stripe.com
    python run_linkedin_enrichment.py user@stripe.com
"""

from __future__ import annotations

import argparse
import hashlib
import html
import logging
import math
import os
import re
import sys
import uuid
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote, unquote, urlparse

import httpx
from dotenv import load_dotenv

from scrape_api_key import get_scrape_api_key

try:
    from psycopg import sql as psql
except ImportError:
    psql = None  # type: ignore[misc, assignment]

load_dotenv()

logger = logging.getLogger(__name__)

# ScraperAPI proxies a target URL; we use it to load Google SERP HTML.
SCRAPERAPI_BASE = "https://api.scraperapi.com"
DEFAULT_TABLE = "free_company_dataset"
# Default PostgreSQL schema for company tables (override with FREE_COMPANY_SCHEMA).
DEFAULT_FREE_COMPANY_SCHEMA = "Company_Dataset"
_IDENT_OK = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _parse_schema_and_table(table_env: str) -> Tuple[str, str]:
    """
    Resolve schema + table from ``FREE_COMPANY_TABLE`` / argument.

    ``myschema.my_table`` → ``("myschema", "my_table")``. Otherwise schema from
    ``FREE_COMPANY_SCHEMA`` (default ``Company_Dataset``).
    """
    raw = (table_env or "").strip() or DEFAULT_TABLE
    if "." in raw:
        sch, _, tbl = raw.partition(".")
        sch = (sch.strip() or DEFAULT_FREE_COMPANY_SCHEMA)
        tbl = tbl.strip() or DEFAULT_TABLE
        return sch, tbl
    sch = (os.environ.get("FREE_COMPANY_SCHEMA") or DEFAULT_FREE_COMPANY_SCHEMA).strip()
    sch = sch or DEFAULT_FREE_COMPANY_SCHEMA
    return sch, raw


def _safe_sql_ident(name: str, label: str) -> str:
    if not name or not _IDENT_OK.match(name):
        raise ValueError(f"Invalid {label} identifier: {name!r} (use letters, numbers, underscore only)")
    return name
# Minimum similarity (0–1) to accept a non-exact PostgreSQL domain match
PG_SIMILARITY_THRESHOLD = 0.72
# Row must have at least one of these (non-empty) to count as "found in PG"
PG_WEBSITE_KEYS = (
    "company_website_url",
    "website",
    "website_url",
    "url",
    "company_url",
)

DEFAULT_PGVECTOR_DOMAIN_COLUMN = "domain_embedding"
DEFAULT_PGVECTOR_DIMS = 3
DEFAULT_PGVECTOR_MAX_COSINE_DISTANCE = 0.55
# Built-in example query for cosine / pgvector tests (unit-normalized before use).
PGVECTOR_EXAMPLE_QUERY_VECTOR: Tuple[float, float, float] = (0.1, 0.2, 0.3)


def normalize_domain(value: str) -> str:
    """Lowercase host; strip path, port, www."""
    s = (value or "").strip().lower()
    if not s:
        return ""
    if "@" in s:
        s = s.split("@", 1)[1]
    if "://" in s:
        s = urlparse(s).netloc or s
    s = s.split("/")[0].split(":")[0]
    if s.startswith("www."):
        s = s[4:]
    return s


def normalize_stored_domain_value(value: Optional[str]) -> str:
    """
    Normalize a DB cell that may be a bare domain or a full URL (common in datasets).

    ``normalize_domain('https://www.acme.com/path')`` would not strip the scheme; this does.
    """
    if value is None:
        return ""
    s = str(value).strip().lower()
    if not s:
        return ""
    if "@" in s:
        s = s.split("@", 1)[1]
    if "://" not in s and "/" in s and "." in s.split("/")[0]:
        s = "https://" + s
    if "://" in s:
        try:
            p = urlparse(s)
            host = (p.netloc or p.path.split("/")[0] or "").strip()
        except Exception:
            host = s
        if host.startswith("www."):
            host = host[4:]
        return host.split(":")[0].split("/")[0]
    return normalize_domain(s)


def format_website_https_www(value: Optional[str]) -> str:
    """
    After a match, normalize website output to ``https://www.{hostname}/``.

    Accepts a bare domain (``example.com``), a full URL, or empty string.
    """
    if value is None or not str(value).strip():
        return ""
    host = normalize_stored_domain_value(str(value).strip())
    if not host:
        return ""
    return f"https://www.{host}/"


def cosine_similarity_vectors(v: Sequence[float], w: Sequence[float]) -> float:
    """
    Cosine similarity between two same-length vectors (general numeric check; not assumed unit length).

    Returns a value in ``[-1.0, 1.0]`` for real inputs; empty / length mismatch raises ``ValueError``.
    """
    va = [float(x) for x in v]
    wb = [float(x) for x in w]
    if len(va) != len(wb) or not va:
        raise ValueError("vectors must have the same non-zero length")
    dot = sum(a * b for a, b in zip(va, wb))
    ma = math.sqrt(sum(a * a for a in va))
    mb = math.sqrt(sum(b * b for b in wb))
    if ma == 0 or mb == 0:
        return 0.0
    sim = dot / (ma * mb)
    return max(-1.0, min(1.0, sim))


def pgvector_cosine_similarity_from_distance(cosine_distance: float) -> float:
    """
    pgvector's ``<=>`` operator returns **cosine distance**; for typical embeddings,
    **cosine similarity** = ``1 - cosine_distance`` (same as inner product on unit vectors).
    """
    return max(-1.0, min(1.0, 1.0 - float(cosine_distance)))


def _unit_vector(vec: Sequence[float]) -> List[float]:
    m = math.sqrt(sum(float(x) * float(x) for x in vec)) or 1.0
    return [round(float(x) / m, 6) for x in vec]


def embedding_from_domain(norm: str, dims: int = DEFAULT_PGVECTOR_DIMS) -> List[float]:
    """
    Deterministic unit vector (length ``dims``) from a normalized domain string.

    Use the **same function** when INSERT/UPDATE-ing ``domain_embedding`` so vector search
    aligns with stored rows. Example 3-D output shape (not fixed values): like ``[0.12, 0.58, 0.81]``.
    """
    if dims < 1:
        raise ValueError("dims must be >= 1")
    if not norm:
        return [0.0] * dims
    h = hashlib.sha256(norm.encode("utf-8")).digest()
    raw: List[float] = []
    for i in range(dims):
        j = (i * 2) % max(1, len(h) - 1)
        x = (h[j] << 8 | h[j + 1]) / 65535.0
        raw.append(x)
    mag = math.sqrt(sum(x * x for x in raw)) or 1.0
    return [round(x / mag, 6) for x in raw]


def _query_embedding_vector(norm: str, dims: int) -> List[float]:
    """
    Build the pgvector query embedding (always L2-unitized for stable cosine ops).

    Priority:

    1. ``PGVECTOR_USE_EXAMPLE_QUERY_VECTOR=1`` → fixed ``[0.1, 0.2, 0.3]`` (requires ``dims == 3``).
    2. ``PGVECTOR_QUERY_VECTOR`` or ``DOMAIN_QUERY_VECTOR`` → comma-separated floats (length ``dims``).
    3. Hash-from-domain via ``embedding_from_domain``.
    """
    ex_flag = os.environ.get("PGVECTOR_USE_EXAMPLE_QUERY_VECTOR", "").strip().lower()
    if ex_flag in ("1", "true", "yes"):
        if dims != len(PGVECTOR_EXAMPLE_QUERY_VECTOR):
            raise ValueError(
                f"PGVECTOR_USE_EXAMPLE_QUERY_VECTOR requires PGVECTOR_DIMS={len(PGVECTOR_EXAMPLE_QUERY_VECTOR)}"
            )
        return _unit_vector(PGVECTOR_EXAMPLE_QUERY_VECTOR)

    raw = (os.environ.get("PGVECTOR_QUERY_VECTOR") or os.environ.get("DOMAIN_QUERY_VECTOR", "")).strip()
    if not raw:
        return embedding_from_domain(norm, dims=dims)
    parts = [float(x.strip()) for x in raw.split(",") if x.strip() != ""]
    if len(parts) != dims:
        raise ValueError(
            f"PGVECTOR_QUERY_VECTOR / DOMAIN_QUERY_VECTOR must contain exactly {dims} comma-separated floats "
            f"(got {len(parts)})"
        )
    return _unit_vector(parts)


def _vector_sql_literal(vec: List[float]) -> str:
    return "[" + ",".join(str(x) for x in vec) + "]"


def domain_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    a, b = a.lower(), b.lower()
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def _registrable_domain_match(host: str, norm: str) -> bool:
    """
    True if ``host`` is the same registrable host as ``norm`` (e.g. ``shop.mastiok.de`` vs ``mastiok.de``).

    Avoids ``evilmastiok.de`` matching ``mastiok.de`` (requires boundary before ``norm`` suffix).
    """
    if not host or not norm:
        return False
    host, norm = host.lower(), norm.lower()
    if host == norm:
        return True
    if not host.endswith(norm):
        return False
    if len(host) == len(norm):
        return True
    return host[-len(norm) - 1] == "."


def _row_keys_lower(row: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).lower(): v for k, v in row.items()}


def _get_ci(row: Dict[str, Any], *names: str) -> Any:
    low = _row_keys_lower(row)
    for n in names:
        v = low.get(n.lower())
        if v is not None and str(v).strip():
            return v
    return None


def row_to_display(
    row: Dict[str, Any],
    source: str,
    *,
    search_domain: Optional[str] = None,
) -> Dict[str, Any]:
    """Map a PostgreSQL row (any column names) to the display shape."""
    r = row
    website = _get_ci(r, "company_website_url", "website", "website_url", "url", "company_url")
    cdom = _get_ci(r, "company_domain", "domain", "email_domain", "root_domain") or ""
    fb = normalize_domain(search_domain) if search_domain else ""
    raw_site = (str(website).strip() if website else "") or (str(cdom).strip() if cdom else "") or fb
    return {
        "source": source,
        "company_domain": normalize_domain(cdom) if cdom else fb,
        "company_name": str(
            _get_ci(r, "company_name", "name", "organization_name", "org_name", "business_name")
            or ""
        ).strip(),
        "website_url": format_website_https_www(raw_site),
        "employee_count": str(_get_ci(r, "employee_count", "employees", "company_size") or "").strip(),
        "industry": str(_get_ci(r, "industry") or "").strip(),
        "sub_industry": str(_get_ci(r, "sub_industry", "sub-industry", "subindustry") or "").strip(),
        "company_linkedin_url": str(
            _get_ci(r, "company_linkedin_url", "linkedin_company_url", "company_linkedin", "linkedin_url")
            or ""
        ).strip(),
        "hq_country": str(
            _get_ci(r, "hq_location_country", "hq_country", "headquarters_country", "country")
            or ""
        ).strip(),
        "hq_state": str(_get_ci(r, "hq_location_state", "hq_state", "headquarters_state", "state") or "").strip(),
        "hq_city": str(_get_ci(r, "hq_location_city", "hq_city", "headquarters_city", "city") or "").strip(),
    }


def _pick_existing_column(all_cols: List[str], candidates: Tuple[str, ...]) -> Optional[str]:
    acl = {c.lower(): c for c in all_cols}
    for cand in candidates:
        if cand.lower() in acl:
            return acl[cand.lower()]
    return None


def _enrichment_has_data_to_persist(enriched: Dict[str, Any]) -> bool:
    """True if ScraperAPI produced at least one non-domain field worth saving."""
    for k in (
        "company_name",
        "website_url",
        "company_linkedin_url",
        "industry",
        "sub_industry",
        "employee_count",
        "hq_country",
        "hq_state",
        "hq_city",
    ):
        v = enriched.get(k)
        if v is not None and str(v).strip():
            return True
    return False


def upsert_scrape_enrichment_to_postgres(
    dsn: str,
    table_spec: str,
    enriched: Dict[str, Any],
    *,
    search_domain: str,
) -> Tuple[bool, str]:
    """
    INSERT or UPDATE the company row after ScraperAPI enrichment.

    Maps fields to the first matching column name on the table. Domain key column must exist
    (``company_domain`` or ``domain``). Optional ``domain_embedding`` is filled when present
    and ``PGVECTOR_DIMS`` matches.

    Disable with ``SCRAPER_SAVE_TO_POSTGRES=0``. Skips when there is nothing to save beyond domain.
    """
    flag = os.environ.get("SCRAPER_SAVE_TO_POSTGRES", "1").strip().lower()
    if flag in ("0", "false", "no", "off"):
        return False, "scraper_save_disabled"

    norm = normalize_domain(search_domain) or normalize_domain(str(enriched.get("company_domain") or ""))
    if not norm:
        return False, "no_company_domain_for_upsert"

    if not _enrichment_has_data_to_persist(enriched):
        return False, "scrape_empty_nothing_to_save"

    try:
        import psycopg
    except ImportError:
        return False, "psycopg_not_installed"

    if psql is None:
        return False, "psycopg_sql_missing"

    schema, table_name = _parse_schema_and_table(table_spec)
    sch_ident = psql.Identifier(_safe_sql_ident(schema, "schema"))
    tbl_ident = psql.Identifier(_safe_sql_ident(table_name, "table"))
    qt_ident = psql.SQL("{}.{}" ).format(sch_ident, tbl_ident)

    values: Dict[str, Any] = {}
    vector_col: Optional[str] = None

    def _s(v: Any) -> Optional[str]:
        if v is None:
            return None
        t = str(v).strip()
        return t or None

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                all_cols = _list_table_columns(cur, schema, table_name)
                if not all_cols:
                    return False, f"no_columns_for_{schema}.{table_name}"

                key_col = _pick_existing_column(all_cols, ("company_domain", "domain", "email_domain"))
                if not key_col:
                    return False, "no_domain_key_column"

                values[key_col] = norm

                ncol = _pick_existing_column(
                    all_cols,
                    ("company_name", "name", "organization_name", "org_name", "business_name"),
                )
                if ncol and ncol != key_col:
                    v = _s(enriched.get("company_name"))
                    if v:
                        values[ncol] = v

                wcol = _pick_existing_column(
                    all_cols,
                    ("company_website_url", "website", "website_url", "url", "company_url"),
                )
                if wcol and wcol != key_col:
                    v = _s(enriched.get("website_url"))
                    if v:
                        values[wcol] = v

                ecol = _pick_existing_column(all_cols, ("employee_count", "employees", "company_size"))
                if ecol:
                    v = _s(enriched.get("employee_count"))
                    if v:
                        values[ecol] = v

                icol = _pick_existing_column(all_cols, ("industry",))
                if icol:
                    v = _s(enriched.get("industry"))
                    if v:
                        values[icol] = v

                scol = _pick_existing_column(all_cols, ("sub_industry", "sub-industry", "subindustry"))
                if scol:
                    v = _s(enriched.get("sub_industry"))
                    if v:
                        values[scol] = v

                lcol = _pick_existing_column(
                    all_cols,
                    ("company_linkedin_url", "linkedin_company_url", "company_linkedin", "linkedin_url"),
                )
                if lcol:
                    v = _s(enriched.get("company_linkedin_url"))
                    if v:
                        values[lcol] = v

                hq_c = _pick_existing_column(
                    all_cols,
                    ("hq_location_country", "hq_country", "headquarters_country", "country"),
                )
                if hq_c:
                    v = _s(enriched.get("hq_country"))
                    if v:
                        values[hq_c] = v

                hq_s = _pick_existing_column(
                    all_cols,
                    ("hq_location_state", "hq_state", "headquarters_state", "state"),
                )
                if hq_s:
                    v = _s(enriched.get("hq_state"))
                    if v:
                        values[hq_s] = v

                hq_city = _pick_existing_column(
                    all_cols,
                    ("hq_location_city", "hq_city", "headquarters_city", "city"),
                )
                if hq_city:
                    v = _s(enriched.get("hq_city"))
                    if v:
                        values[hq_city] = v

                stype = _pick_existing_column(all_cols, ("source_type",))
                if stype:
                    values[stype] = _s(enriched.get("source")) or "scraperapi"

                surl = _pick_existing_column(all_cols, ("source_url",))
                if surl:
                    values[surl] = _s(enriched.get("website_url")) or f"scraperapi:google_search:{norm}"

                vec_cand = os.environ.get("PGVECTOR_DOMAIN_COLUMN", DEFAULT_PGVECTOR_DOMAIN_COLUMN).strip()
                if vec_cand and vec_cand in all_cols:
                    try:
                        dims = int(os.environ.get("PGVECTOR_DIMS", str(DEFAULT_PGVECTOR_DIMS)))
                        vec_lit = _vector_sql_literal(embedding_from_domain(norm, dims=dims))
                        values[vec_cand] = vec_lit
                        vector_col = vec_cand
                    except Exception as ex:
                        logger.warning("Skip domain_embedding on upsert: %s", ex)

                id_col = _pick_existing_column(all_cols, ("id",))
                key_ident = psql.Identifier(_safe_sql_ident(key_col, "column"))
                non_key = {
                    k: v
                    for k, v in values.items()
                    if k != key_col and v is not None and (not id_col or k != id_col)
                }

                updated = False
                if non_key:
                    set_parts: List[Any] = []
                    params_u: List[Any] = []
                    for c, val in non_key.items():
                        c_ident = psql.Identifier(_safe_sql_ident(c, "column"))
                        if vector_col and c == vector_col:
                            set_parts.append(psql.SQL("{} = %s::vector").format(c_ident))
                        else:
                            set_parts.append(psql.SQL("{} = %s").format(c_ident))
                        params_u.append(val)

                    upd = psql.SQL("UPDATE {} SET {} WHERE {} = %s").format(
                        qt_ident,
                        psql.SQL(", ").join(set_parts),
                        key_ident,
                    )
                    params_u.append(norm)
                    cur.execute(upd, params_u)
                    updated = bool(cur.rowcount and cur.rowcount > 0)

                if updated:
                    conn.commit()
                    return True, f"updated_row ({key_col}={norm!r})"

                if not non_key:
                    cur.execute(
                        psql.SQL("SELECT 1 FROM {} WHERE {} = %s LIMIT 1").format(qt_ident, key_ident),
                        (norm,),
                    )
                    if cur.fetchone():
                        conn.commit()
                        return True, f"unchanged ({key_col}={norm!r}, no table columns mapped for API fields)"

                insert_vals = dict(values)
                if id_col and id_col not in insert_vals:
                    insert_vals[id_col] = str(uuid.uuid4())

                cols_order = list(insert_vals.keys())
                col_idents = [psql.Identifier(_safe_sql_ident(c, "column")) for c in cols_order]
                col_sql = psql.SQL(", ").join(col_idents)
                ph_parts: List[Any] = []
                params_i: List[Any] = []
                for c in cols_order:
                    val = insert_vals[c]
                    if vector_col and c == vector_col:
                        ph_parts.append(psql.SQL("%s::vector"))
                    else:
                        ph_parts.append(psql.SQL("%s"))
                    params_i.append(val)

                ins = psql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    qt_ident,
                    col_sql,
                    psql.SQL(", ").join(ph_parts),
                )
                cur.execute(ins, params_i)
                conn.commit()
                return True, f"inserted_row ({key_col}={norm!r})"

    except Exception as e:
        logger.exception("upsert_scrape_enrichment_to_postgres failed: %s", e)
        return False, f"upsert_error:{e}"


def _pg_dsn_with_database(dsn: str, database: Optional[str]) -> str:
    if not database or not database.strip():
        return dsn
    db = database.strip()
    # postgresql://user:pass@host:5432/olddb → replace path
    if "://" in dsn:
        base, _, rest = dsn.partition("://")
        if "/" in rest:
            auth_host, _, _old_db = rest.rpartition("/")
            return f"{base}://{auth_host}/{db}"
    return dsn


def _discover_domain_column(cur: Any, schema: str, table: str) -> Tuple[Optional[str], List[str]]:
    """
    Pick a column used for domain matching.

    Returns ``(column_name_or_None, all_column_names)``.
    Falls back to website-style columns if nothing is named *domain* (common in real datasets).
    """
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    cols = [r[0] for r in cur.fetchall()]
    preferred = (
        "company_domain",
        "domain",
        "email_domain",
        "root_domain",
        "website_domain",
        "web_domain",
        "site_domain",
    )
    for p in preferred:
        if p in cols:
            return p, cols
    for c in cols:
        if "domain" in c.lower():
            return c, cols
    # No *domain* column: use website URL column for host / LIKE matching (still works with scoring).
    for fallback in ("company_website_url", "website", "website_url", "url", "company_url"):
        if fallback in cols:
            return fallback, cols
    return None, cols


def _diagnose_missing_table_or_columns(
    cur: Any, schema: str, table: str, cols: List[str]
) -> None:
    """Print hints when discovery fails (wrong schema/name is the usual cause)."""
    if cols:
        print(
            f"PostgreSQL: table {schema!r}.{table!r} has no domain/website column we recognize. "
            f"Columns: {cols!r}",
            file=sys.stderr,
        )
        return
    print(
        f"PostgreSQL: no columns found for {schema!r}.{table!r} (wrong schema, DB, or table name?).",
        file=sys.stderr,
    )
    cur.execute(
        """
        SELECT table_schema, table_name FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' AND lower(table_name) = lower(%s)
        ORDER BY table_schema, table_name
        """,
        (table,),
    )
    matches = cur.fetchall()
    if matches:
        print(
            "  Tables with this name (use FREE_COMPANY_TABLE=schema.table or FREE_COMPANY_SCHEMA): "
            f"{matches}",
            file=sys.stderr,
        )


def _has_website_in_row(row: Dict[str, Any]) -> bool:
    """Check if the row has a company website field filled."""
    low = _row_keys_lower(row)
    for k in PG_WEBSITE_KEYS:
        v = low.get(k.lower())
        if v and str(v).strip():
            return True
    return False


def _has_enrichment_in_row(row: Dict[str, Any]) -> bool:
    """
    True if the row has any field we can treat as a successful DB hit.

    Previously only ``website`` counted, so rows with LinkedIn / industry / headcount only
    were discarded even when the domain matched.
    """
    if _has_website_in_row(row):
        return True
    low = _row_keys_lower(row)
    for k in (
        "company_name",
        "name",
        "organization_name",
        "org_name",
        "business_name",
        "company_linkedin_url",
        "linkedin_company_url",
        "company_linkedin",
        "linkedin_url",
        "industry",
        "employee_count",
        "employees",
        "company_size",
    ):
        v = low.get(k.lower())
        if v is not None and str(v).strip():
            return True
    return False


def _list_table_columns(cur: Any, schema: str, table: str) -> List[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]


def _website_columns_present(all_cols: List[str]) -> List[str]:
    allowed = {k.lower() for k in PG_WEBSITE_KEYS}
    return [c for c in all_cols if c.lower() in allowed]


def _pgvector_extension_available(cur: Any) -> bool:
    cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
    return cur.fetchone() is not None


def _try_pgvector_best_row(
    cur: Any,
    *,
    qt_ident: Any,
    vec_col_name: str,
    norm: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[float], str]:
    """
    Return ``(row, cosine_distance, note)``. Row is None if no acceptable vector neighbor.
    """
    if os.environ.get("PGVECTOR_DISABLED", "").strip().lower() in ("1", "true", "yes"):
        return None, None, "pgvector_disabled"

    if not _pgvector_extension_available(cur):
        return None, None, "pgvector_extension_missing"

    dims = int(os.environ.get("PGVECTOR_DIMS", str(DEFAULT_PGVECTOR_DIMS)))
    max_dist = float(os.environ.get("PGVECTOR_MAX_COSINE_DISTANCE", str(DEFAULT_PGVECTOR_MAX_COSINE_DISTANCE)))

    try:
        vec = _query_embedding_vector(norm, dims)
    except ValueError as e:
        return None, None, f"pgvector_bad_query_vector:{e}"

    lit = _vector_sql_literal(vec)
    v_ident = psql.Identifier(_safe_sql_ident(vec_col_name, "column"))

    q = psql.SQL(
        """
        SELECT *, ({} <=> %s::vector) AS _pgv_dist
        FROM {}
        WHERE {} IS NOT NULL
        ORDER BY {} <=> %s::vector
        LIMIT 15
        """
    ).format(v_ident, qt_ident, v_ident, v_ident)

    cur.execute(q, (lit, lit))
    names = [d[0] for d in cur.description] if cur.description else []
    for row in cur.fetchall():
        d = dict(zip(names, row))
        dist = d.pop("_pgv_dist", None)
        if dist is None:
            continue
        try:
            dist_f = float(dist)
        except (TypeError, ValueError):
            continue
        if dist_f > max_dist:
            continue
        if not _has_enrichment_in_row(d):
            continue
        cos_sim = pgvector_cosine_similarity_from_distance(dist_f)
        qvec_lit = lit
        note = (
            f"postgres_pgvector (query_vec={qvec_lit}; cosine_distance={dist_f:.4f}; "
            f"cosine_similarity={cos_sim:.4f})"
        )
        if os.environ.get("PGVECTOR_DEBUG_COSINE", "").strip().lower() in ("1", "true", "yes"):
            print(
                f"[pgvector] query_vec={qvec_lit} cosine_distance={dist_f:.6f} cosine_similarity={cos_sim:.6f}",
                file=sys.stderr,
                flush=True,
            )
        return d, dist_f, note

    return None, None, "pgvector_no_row_under_threshold"


def _row_domain_match_score(norm: str, row: Dict[str, Any], dom_col: str) -> float:
    """How well this row's domain column (and optional website host) matches ``norm``."""
    cell = str(row.get(dom_col) or "").strip()
    best = 0.0
    if cell:
        cn = normalize_stored_domain_value(cell)
        if cn:
            best = max(best, domain_similarity(norm, cn))
            if cn == norm:
                best = 1.0
            elif _registrable_domain_match(cn, norm):
                best = max(best, 0.97)
    low = _row_keys_lower(row)
    for k in PG_WEBSITE_KEYS:
        w = low.get(k.lower())
        if not w:
            continue
        hn = normalize_stored_domain_value(str(w))
        if not hn:
            continue
        best = max(best, domain_similarity(norm, hn))
        if hn == norm:
            best = max(best, 1.0)
        elif _registrable_domain_match(hn, norm):
            best = max(best, 0.97)
        elif norm in hn or hn.endswith("." + norm):
            best = max(best, 0.96)
    return best


def fetch_best_row_from_postgres(
    domain: str,
    dsn: str,
    table: str,
) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Return ``(row_dict, note)`` where note describes match quality.
    ``row_dict`` uses original column names from the DB.
    """
    try:
        import psycopg
    except ImportError:
        print("ERROR: install psycopg: pip install psycopg[binary]", file=sys.stderr)
        return None, "psycopg_not_installed"

    if psql is None:
        print("ERROR: psycopg.sql unavailable", file=sys.stderr)
        return None, "psycopg_import_error"

    norm = normalize_domain(domain)
    if not norm:
        return None, "empty_domain"

    schema, table_name = _parse_schema_and_table(table)

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                dom_col, discovered_cols = _discover_domain_column(cur, schema, table_name)
                if not dom_col:
                    _diagnose_missing_table_or_columns(cur, schema, table_name, discovered_cols)
                    safe_tbl = f"{schema}.{table_name}".replace(".", "_")
                    return None, f"no_domain_column_in_{safe_tbl}"

                sch_ident = psql.Identifier(_safe_sql_ident(schema, "schema"))
                tbl_ident = psql.Identifier(_safe_sql_ident(table_name, "table"))
                qt_ident = psql.SQL("{}.{}" ).format(sch_ident, tbl_ident)
                c_ident = psql.Identifier(_safe_sql_ident(dom_col, "column"))

                all_cols = _list_table_columns(cur, schema, table_name)
                vec_col = os.environ.get("PGVECTOR_DOMAIN_COLUMN", DEFAULT_PGVECTOR_DOMAIN_COLUMN).strip()
                if vec_col and vec_col in all_cols:
                    vrow, vdist, vnote = _try_pgvector_best_row(
                        cur, qt_ident=qt_ident, vec_col_name=vec_col, norm=norm
                    )
                    if vrow is not None:
                        t_sim = _row_domain_match_score(norm, vrow, dom_col)
                        return vrow, f"{vnote}; text_similarity={t_sim:.2f}"
                    # else fall through to text/LIKE discovery (SequenceMatcher on candidates)

                site_cols = _website_columns_present(all_cols)

                # Pull candidates: domain column OR any website column contains the domain string.
                # (Rows often store ``https://company.com`` in the domain column — old SQL equality missed those.)
                like_full = f"%{norm}%"
                like_prefix = f"{norm}%"
                or_parts: List[Any] = [
                    psql.SQL("lower({}::text) LIKE %s").format(c_ident),
                    psql.SQL("lower({}::text) LIKE %s").format(c_ident),
                ]
                params: List[Any] = [like_full, like_prefix]
                for wc in site_cols:
                    if wc == dom_col:
                        continue
                    or_parts.append(
                        psql.SQL("lower({}::text) LIKE %s").format(
                            psql.Identifier(_safe_sql_ident(wc, "column"))
                        )
                    )
                    params.append(like_full)

                where_sql = psql.SQL(" OR ").join(or_parts)
                cur.execute(
                    psql.SQL("SELECT * FROM {} WHERE {} LIMIT 150").format(qt_ident, where_sql),
                    params,
                )
                names = [d[0] for d in cur.description] if cur.description else []
                seen: set[str] = set()
                candidates: List[Dict[str, Any]] = []
                for row in cur.fetchall():
                    d = dict(zip(names, row))
                    key = "|".join(str(d.get(k, "")) for k in sorted(d.keys()))
                    if key in seen:
                        continue
                    seen.add(key)
                    candidates.append(d)

                # If nothing matched ``%mastiok.de%`` (e.g. DB only has ``mastiok`` or odd casing),
                # widen with the first DNS label (``mastiok`` from ``mastiok.de``).
                if not candidates and "." in norm:
                    apex = norm.split(".", 1)[0]
                    if len(apex) >= 3:
                        wide_parts = list(or_parts)
                        wide_parts.append(psql.SQL("lower({}::text) LIKE %s").format(c_ident))
                        wide_params = list(params) + [f"%{apex}%"]
                        for wc in site_cols:
                            if wc == dom_col:
                                continue
                            wide_parts.append(
                                psql.SQL("lower({}::text) LIKE %s").format(
                                    psql.Identifier(_safe_sql_ident(wc, "column"))
                                )
                            )
                            wide_params.append(f"%{apex}%")
                        wide_where = psql.SQL(" OR ").join(wide_parts)
                        cur.execute(
                            psql.SQL("SELECT * FROM {} WHERE {} LIMIT 150").format(qt_ident, wide_where),
                            wide_params,
                        )
                        for row in cur.fetchall():
                            d = dict(zip(names, row))
                            key = "|".join(str(d.get(k, "")) for k in sorted(d.keys()))
                            if key in seen:
                                continue
                            seen.add(key)
                            candidates.append(d)

        best: Optional[Dict[str, Any]] = None
        best_score = 0.0
        for row in candidates:
            sc = _row_domain_match_score(norm, row, dom_col)
            if sc > best_score:
                best_score = sc
                best = row

        if best is None or best_score < PG_SIMILARITY_THRESHOLD:
            return None, f"postgres_no_similar_match (best={best_score:.2f})"

        if not _has_enrichment_in_row(best):
            return None, f"postgres_domain_match_but_no_company_fields (score={best_score:.2f})"

        note = (
            "postgres_exact_domain_with_website"
            if best_score >= 0.99 and _has_website_in_row(best)
            else f"postgres_similarity (score={best_score:.2f})"
        )
        return best, note
    except Exception as e:
        print(f"PostgreSQL error: {e}", file=sys.stderr)
        return None, f"postgres_error:{e}"


def _google_search_url(query: str, num: int = 10) -> str:
    return "https://www.google.com/search?q=" + quote(query, safe="") + f"&hl=en&gl=us&num={num}"


def _fetch_google_via_scraperapi(client: httpx.Client, scraper_api_key: str, q: str, num: int = 10) -> str:
    """Return raw Google SERP HTML fetched through ScraperAPI."""
    target = _google_search_url(q, num=num)
    api_url = (
        f"{SCRAPERAPI_BASE}?api_key={quote(scraper_api_key, safe='')}"
        f"&url={quote(target, safe='')}"
    )
    r = client.get(api_url, timeout=90.0, follow_redirects=True)
    r.raise_for_status()
    return r.text


def _html_to_plain_text(html: str, max_len: int = 120_000) -> str:
    """Strip tags/scripts for regex mining (ScraperAPI returns HTML, not JSON like Serper)."""
    if not html:
        return ""
    s = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html)
    s = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", s)
    s = re.sub(r"(?is)<noscript[^>]*>.*?</noscript>", " ", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:max_len] if len(s) > max_len else s


def _parse_google_serp_html(html: str) -> Dict[str, Any]:
    """
    Best-effort parse of Google HTML into the same shape Serper used:
    ``organic`` list of ``{title, link, snippet}``; empty ``knowledgeGraph`` / ``answerBox``.

    Note: Serper populated ``knowledgeGraph`` (industry type, HQ). Raw HTML does not unless
    we mine ``_page_plain`` downstream.
    """
    organic: List[Dict[str, Any]] = []
    seen: set[str] = set()
    # Main results: <a href=...><h3>title</h3></a>
    pattern = re.compile(
        r'<a[^>]+href="([^"]+)"[^>]*>\s*<h3[^>]*>(.*?)</h3>',
        re.DOTALL | re.IGNORECASE,
    )
    for m in pattern.finditer(html):
        href = (m.group(1) or "").replace("&amp;", "&")
        title_html = m.group(2) or ""
        title = re.sub(r"<[^>]+>", "", title_html).strip()
        url: Optional[str] = None
        if "url?" in href and "q=" in href:
            qm = re.search(r"[?&]q=([^&]+)", href)
            if qm:
                try:
                    url = unquote(qm.group(1))
                except Exception:
                    continue
        elif href.startswith("http"):
            url = href
        if not url or not url.startswith("http"):
            continue
        low = url.lower()
        if any(x in low for x in ("google.", "gstatic.", "youtube.com", "googleusercontent.")):
            continue
        url = url.split("#")[0]
        if url in seen:
            continue
        seen.add(url)
        snippet = ""
        window = html[m.end() : m.end() + 2000]
        for sm in re.finditer(r">([^<>]{25,320})<", window):
            text = sm.group(1).strip()
            if len(text) > 20 and not text.lower().startswith("http"):
                snippet = text
                break
        organic.append({"title": title, "link": url, "snippet": snippet})
        if len(organic) >= 12:
            break

    return {"organic": organic, "knowledgeGraph": {}, "answerBox": {}}


def _search_payload_via_scraperapi(client: httpx.Client, scraper_api_key: str, q: str, num: int = 10) -> Dict[str, Any]:
    html = _fetch_google_via_scraperapi(client, scraper_api_key, q, num=num)
    payload = _parse_google_serp_html(html)
    payload["_page_plain"] = _html_to_plain_text(html)
    return payload


_HQ_LINE_RE = re.compile(
    r"(?:Headquarters|Head office|HQ|Located in|Based in)\s*[:\s]\s*"
    r"([A-Za-z0-9À-ÿ'’\-\s,.]{8,140}?)(?:\.|$|\||Follow|Employees|\d{4})",
    re.I,
)
_INDUSTRY_LINE_RE = re.compile(
    r"(?:Industry|Sector|Type of company)\s*[:\s]\s*"
    r"([A-Za-z0-9\s&\-,/]{3,90}?)(?:\.|$|\||Headquarters|Employees|Founded)",
    re.I,
)


_LI_COMPANY_RE = re.compile(
    r"https?://(?:[\w-]+\.)?linkedin\.com/company/[\w\-_%]+/?",
    re.I,
)
_EMPLOYEE_RE = re.compile(
    r"(\d{1,3}(?:,\d{3})*)\s*[-–—]\s*(\d{1,3}(?:,\d{3})*)\s*employees?",
    re.I,
)
_EMPLOYEE_PLUS_RE = re.compile(r"(\d{1,3}(?:,\d{3})*)\+\s*employees?", re.I)
_SIMPLE_RANGE = re.compile(r"(\d+)\s*[-–—]\s*(\d+)\s*employees?", re.I)


def _extract_linkedin_urls(text: str) -> List[str]:
    return list(dict.fromkeys(_LI_COMPANY_RE.findall(text or "")))


def _pick_website_from_organic(organic: List[Dict[str, Any]], domain: str) -> str:
    norm = domain.lower()
    skip_hosts = ("linkedin.com", "facebook.com", "twitter.com", "x.com", "instagram.com", "crunchbase.com")
    for o in organic:
        link = (o.get("link") or "").strip()
        if not link:
            continue
        try:
            host = urlparse(link).netloc.lower()
            if host.startswith("www."):
                host = host[4:]
        except Exception:
            continue
        if any(s in host for s in skip_hosts):
            continue
        if norm in host or host.endswith("." + norm) or host == norm:
            return link.split("?")[0].rstrip("/")
    for o in organic:
        link = (o.get("link") or "").strip()
        if not link:
            continue
        host = urlparse(link).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        if not any(s in host for s in skip_hosts):
            return link.split("?")[0].rstrip("/")
    return ""


def _snippet_employee(snippet: str) -> str:
    if not snippet:
        return ""
    for rx in (_EMPLOYEE_RE, _SIMPLE_RANGE, _EMPLOYEE_PLUS_RE):
        m = rx.search(snippet)
        if m:
            return m.group(0).replace(",", "").strip()
    return ""


def _clean_company_display_name(title: str) -> str:
    """Strip tags/entities and common SERP suffixes from a title string."""
    t = html.unescape(re.sub(r"<[^>]+>", "", title or "")).strip()
    if not t:
        return ""
    lower = t.lower()
    for suf in (
        " - google search",
        " - google",
        " | linkedin",
        " :: linkedin",
        " - wikipedia",
        " — wikipedia",
    ):
        if lower.endswith(suf):
            t = t[: -len(suf)].strip()
            lower = t.lower()
    if "|" in t:
        left = t.split("|", 1)[0].strip()
        if len(left) >= 2:
            t = left
    return t.strip()


def enrich_from_serper(domain: str, api_key: str) -> Dict[str, Any]:
    """Fetch Google SERPs via ScraperAPI (same merge logic as the former Serper path)."""
    norm = normalize_domain(domain)
    out: Dict[str, Any] = {
        "source": "scraperapi",
        "company_domain": norm,
        "company_name": "",
        "website_url": "",
        "employee_count": "",
        "industry": "",
        "sub_industry": "",
        "company_linkedin_url": "",
        "hq_country": "",
        "hq_state": "",
        "hq_city": "",
    }

    queries = [
        f"{norm} official company website",
        f"{norm} LinkedIn company employees",
        f"{norm} headquarters location",
    ]

    all_text: List[str] = []
    organic_all: List[Dict[str, Any]] = []

    with httpx.Client() as client:
        for q in queries:
            try:
                data = _search_payload_via_scraperapi(client, api_key, q, num=10)
            except Exception as e:
                print(f"ScraperAPI/Google request failed ({q!r}): {e}", file=sys.stderr)
                continue

            kg = data.get("knowledgeGraph") or {}
            if isinstance(kg, dict):
                if kg.get("website"):
                    out["website_url"] = str(kg["website"]).strip()
                title = kg.get("title") or ""
                if title and not out["company_name"]:
                    cn = _clean_company_display_name(str(title))
                    if cn:
                        out["company_name"] = cn
                typ = kg.get("type") or ""
                if typ and not out["industry"]:
                    out["industry"] = str(typ).strip()
                attrs = kg.get("attributes") or {}
                if isinstance(attrs, dict):
                    hq = attrs.get("Headquarters") or attrs.get("headquarters") or ""
                    if hq:
                        parts = [p.strip() for p in re.split(r",", str(hq)) if p.strip()]
                        if len(parts) >= 1:
                            out["hq_city"] = parts[0]
                        if len(parts) >= 2:
                            out["hq_state"] = parts[1]
                        if len(parts) >= 3:
                            out["hq_country"] = parts[2]
                    for k, v in attrs.items():
                        all_text.append(f"{k} {v}")

            for block in data.get("organic") or []:
                if isinstance(block, dict):
                    organic_all.append(block)
                    all_text.append(block.get("title") or "")
                    all_text.append(block.get("snippet") or "")

            answer = data.get("answerBox") or {}
            if isinstance(answer, dict):
                all_text.append(str(answer.get("answer") or ""))
                all_text.append(str(answer.get("snippet") or ""))

            # Raw HTML has no JSON knowledge panel — mine visible text (same fields Serper had).
            plain = (data.get("_page_plain") or "").strip()
            if plain:
                all_text.append(plain)

    blob = "\n".join(all_text)
    if not out["company_name"]:
        for o in organic_all:
            link = (o.get("link") or "").strip()
            if not link:
                continue
            try:
                host = urlparse(link).netloc.lower()
                if host.startswith("www."):
                    host = host[4:]
            except Exception:
                continue
            if host == norm or _registrable_domain_match(host, norm):
                cn = _clean_company_display_name(o.get("title") or "")
                if cn:
                    out["company_name"] = cn
                    break
    if not out["website_url"]:
        out["website_url"] = _pick_website_from_organic(organic_all, norm)

    li_urls = _extract_linkedin_urls(blob)
    if li_urls:
        out["company_linkedin_url"] = li_urls[0].split("?")[0].rstrip("/")

    if not out["employee_count"]:
        for o in organic_all:
            ec = _snippet_employee(o.get("snippet") or "") or _snippet_employee(o.get("title") or "")
            if ec:
                out["employee_count"] = ec
                break
        if not out["employee_count"]:
            out["employee_count"] = _snippet_employee(blob)

    # Heuristic HQ / industry from page text (knowledge panel or snippets).
    if not (out["hq_city"] or out["hq_state"] or out["hq_country"]):
        hm = _HQ_LINE_RE.search(blob)
        if hm:
            line = re.sub(r"\s+", " ", hm.group(1)).strip(" ,.-")
            parts = [p.strip() for p in line.split(",") if p.strip()]
            if len(parts) >= 3:
                out["hq_city"], out["hq_state"], out["hq_country"] = parts[0], parts[1], parts[2]
            elif len(parts) == 2:
                out["hq_city"], out["hq_country"] = parts[0], parts[1]
            elif len(parts) == 1:
                out["hq_city"] = parts[0]

    if not out["industry"]:
        im = _INDUSTRY_LINE_RE.search(blob)
        if im:
            out["industry"] = re.sub(r"\s+", " ", im.group(1)).strip(" ,.-")

    # sub_industry is not provided by Google SERP JSON or a stable HTML field here.
    # Fill only when industry line clearly contains "Parent · Child" style (rare).
    if not out["sub_industry"] and out["industry"] and "·" in out["industry"]:
        bits = [b.strip() for b in out["industry"].split("·") if b.strip()]
        if len(bits) >= 2:
            out["industry"], out["sub_industry"] = bits[0], bits[-1]

    out["website_url"] = format_website_https_www(out["website_url"] or norm)

    return out


def print_enrichment(d: Dict[str, Any]) -> None:
    print()
    print("========== Company enrichment ==========")
    print(f"  source               : {d.get('source', '')}")
    print(f"  company_domain       : {d.get('company_domain', '')}")
    print(f"  company_name         : {d.get('company_name', '')}")
    print(f"  website_url          : {d.get('website_url', '')}")
    print(f"  employee_count       : {d.get('employee_count', '')}")
    print(f"  industry             : {d.get('industry', '')}")
    print(f"  sub_industry         : {d.get('sub_industry', '')}")
    print(f"  company_linkedin_url : {d.get('company_linkedin_url', '')}")
    print(f"  hq_country           : {d.get('hq_country', '')}")
    print(f"  hq_state             : {d.get('hq_state', '')}")
    print(f"  hq_city              : {d.get('hq_city', '')}")
    print("========================================")
    print()


def run_for_domain(domain_input: str) -> Dict[str, Any]:
    domain = normalize_domain(domain_input)
    if not domain:
        raise SystemExit("Invalid or empty domain.")

    dsn = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_DSN") or ""
    db_override = os.environ.get("FREE_COMPANY_DATABASE", "").strip()
    table = os.environ.get("FREE_COMPANY_TABLE", DEFAULT_TABLE).strip() or DEFAULT_TABLE
    scraper_key = get_scrape_api_key()

    if dsn:
        dsn = _pg_dsn_with_database(dsn, db_override or None)

    result: Dict[str, Any]

    if dsn:
        row, note = fetch_best_row_from_postgres(domain, dsn, table)
        print(f"[lookup] PostgreSQL: {note}", flush=True)
        # Use Postgres when domain matched and the row has website and/or other company fields
        if row and _has_enrichment_in_row(row) and (
            note.startswith("postgres_exact_domain")
            or note.startswith("postgres_similarity")
            or note.startswith("postgres_pgvector")
        ):
            result = row_to_display(row, "postgresql", search_domain=domain)
            print_enrichment(result)
            return result

    if not scraper_key:
        print(
            "ERROR: SCRAPE_API_KEY (or legacy SCRAPERAPI_*/SCRAPINGDOG_* keys) not set — cannot run search fallback.",
            file=sys.stderr,
        )
        if not dsn:
            print("ERROR: DATABASE_URL / POSTGRES_DSN also not set.", file=sys.stderr)
        sys.exit(1)

    print("[lookup] Using ScraperAPI (Google search)...", flush=True)
    result = enrich_from_serper(domain, scraper_key)
    print_enrichment(result)
    if dsn:
        ok, save_msg = upsert_scrape_enrichment_to_postgres(
            dsn, table, result, search_domain=domain
        )
        if ok:
            print(f"[save] PostgreSQL: {save_msg}", flush=True)
        else:
            print(f"[save] PostgreSQL: skipped ({save_msg})", flush=True)
    else:
        print(
            "[save] PostgreSQL: skipped (set DATABASE_URL or POSTGRES_DSN to upsert ScraperAPI results)",
            flush=True,
        )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich company data from email domain (PG + ScraperAPI).")
    parser.add_argument(
        "domain",
        nargs="?",
        default="",
        help="Email domain or full email (e.g. stripe.com or user@stripe.com)",
    )
    args = parser.parse_args()
    raw = (args.domain or "").strip()
    if not raw:
        print(__doc__)
        print("Usage: python run_linkedin_enrichment.py <domain_or_email>")
        sys.exit(1)
    run_for_domain(raw)


if __name__ == "__main__":
    main()
