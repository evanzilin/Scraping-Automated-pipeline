"""
Orchestration: cache records → normalized / validated JSON written to ``temp/json-date.json``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from lead_cache_json.industry_normalizer import normalize_industry_pair
from lead_cache_json.json_contract import DEFAULT_OUTPUT_JSON, DEFAULT_REJECT_JSON
from lead_cache_json.record_normalizer import (
    get_alias,
    map_flat_to_contract,
    normalize_record_for_contract,
)
from lead_cache_json.validators import apply_empty_string_to_null, validate_output_record

logger = logging.getLogger(__name__)


def format_cached_dataset_to_json(
    cache_records: List[Dict[str, Any]],
    *,
    output_path: Optional[Path] = None,
    reject_path: Optional[Path] = None,
    write_files: bool = True,
) -> List[Dict[str, Any]]:
    """
    Normalize industry/sub-industry, map to the strict JSON contract, validate, and optionally write files.

    Parameters
    ----------
    cache_records:
        Raw list of dicts from cache (flat leads or wrapped ``enriched`` / ``final_resolved`` payloads).
    output_path:
        Defaults to ``temp/json-date.json`` (relative to CWD).
    reject_path:
        Defaults to ``temp/json-date.rejects.json``.
    write_files:
        When True, writes valid rows to ``output_path`` and reject details to ``reject_path``.

    Returns
    -------
    list[dict]
        Only **valid** formatted records (invalid entries are skipped and logged).
    """
    out_path = output_path or DEFAULT_OUTPUT_JSON
    rej_path = reject_path or DEFAULT_REJECT_JSON

    valid_rows: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []

    for idx, raw in enumerate(cache_records):
        if not isinstance(raw, dict):
            logger.warning("Skipping non-dict record index=%s", idx)
            rejected.append({"index": idx, "reasons": ["not_a_dict"], "snapshot": raw})
            continue

        try:
            flat, _top = normalize_record_for_contract(raw)
            ind, sub, ind_reason = normalize_industry_pair(
                get_alias(flat, "industry"),
                get_alias(flat, "sub_industry"),
            )
            if ind_reason:
                logger.info("Record index=%s skipped industry: %s", idx, ind_reason)
                rejected.append(
                    {
                        "index": idx,
                        "reasons": [f"industry:{ind_reason}"],
                        "snapshot": {
                            "industry": get_alias(flat, "industry"),
                            "sub_industry": get_alias(flat, "sub_industry"),
                        },
                    }
                )
                continue

            assert ind is not None and sub is not None
            row = map_flat_to_contract(flat, industry=ind, sub_industry=sub)
            row = apply_empty_string_to_null(row)
            ok, reasons = validate_output_record(row)
            if not ok:
                logger.info("Record index=%s skipped validation: %s", idx, reasons)
                rejected.append({"index": idx, "reasons": reasons, "snapshot": row})
                continue

            valid_rows.append(row)
        except Exception as e:
            logger.exception("Record index=%s failed unexpectedly: %s", idx, e)
            rejected.append({"index": idx, "reasons": [f"exception:{e!s}"], "snapshot": raw})

    logger.info(
        "Format complete: valid=%s invalid=%s total=%s",
        len(valid_rows),
        len(rejected),
        len(cache_records),
    )

    if write_files:
        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            rej_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(valid_rows, indent=2, ensure_ascii=False), encoding="utf-8")
            rej_path.write_text(json.dumps(rejected, indent=2, ensure_ascii=False), encoding="utf-8")
            logger.info("Wrote %s (%s rows)", out_path, len(valid_rows))
            logger.info("Wrote %s (%s rejects)", rej_path, len(rejected))
        except OSError as e:
            logger.error("Failed writing output files: %s", e)
            raise

    return valid_rows


def load_cache_json_file(path: Path) -> List[Dict[str, Any]]:
    """
    Load a JSON file containing either a list of records or a single object (wrapped in a list).

    This is a thin helper for CLI / scripts; callers may use Redis or other cache sources instead.
    """
    text = path.read_text(encoding="utf-8")
    data = json.loads(text)
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        return [data]
    raise ValueError(f"Expected list or dict in {path}, got {type(data)}")
