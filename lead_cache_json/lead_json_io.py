"""
Write a single lead dict to a pretty-printed UTF-8 JSON file under a temp directory.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Mapping, Union

__all__ = [
    "InvalidFilenameError",
    "InvalidLeadJsonError",
    "InvalidTempDirError",
    "LeadJsonWriteError",
    "write_lead_json_to_temp_file",
]

JsonObject = dict[str, Any]


class InvalidLeadJsonError(ValueError):
    """``lead`` is not a single JSON object or is not JSON-serializable."""


class InvalidTempDirError(ValueError):
    """``temp_dir`` is missing, empty, or not usable as a directory path."""


class InvalidFilenameError(ValueError):
    """Output file name is empty, unsafe, or not a simple basename."""


class LeadJsonWriteError(OSError):
    """Failed to write the JSON file (permissions, disk full, etc.)."""


_SAFE_FILENAME_RE = re.compile(r"^[\w.\-]+$")


def _validate_temp_dir(temp_dir: Union[str, Path, None]) -> Path:
    if temp_dir is None:
        raise InvalidTempDirError("temp_dir is required (got None).")
    raw = str(temp_dir).strip()
    if not raw:
        raise InvalidTempDirError("temp_dir is empty or whitespace-only.")
    p = Path(os.path.expanduser(raw))
    if p.parts and p.parts[-1] in (".", ".."):
        raise InvalidTempDirError(f"temp_dir must resolve to a directory path, not {p.parts[-1]!r}.")
    return p


def _normalize_filename(name: str | None) -> str:
    if not name or not str(name).strip():
        return "lead.json"
    base = Path(str(name).strip()).name
    if not base or base in (".", ".."):
        raise InvalidFilenameError(f"Invalid filename: {name!r}.")
    if not _SAFE_FILENAME_RE.match(base):
        raise InvalidFilenameError(
            f"Filename must be a simple name (letters, digits, ._- only): {name!r}."
        )
    if not base.lower().endswith(".json"):
        base = f"{base}.json"
    return base


def write_lead_json_to_temp_file(
    lead: Any,
    temp_dir: Union[str, Path],
    *,
    filename: str | None = None,
) -> Path:
    """
    Validate ``lead`` as a JSON object, ensure ``temp_dir`` exists, and write pretty UTF-8 JSON.

    Parameters
    ----------
    lead:
        A single mapping (JSON object). Lists and scalars at the top level are rejected.
    temp_dir:
        Directory to create if missing; file is ``temp_dir / <filename>``.
    filename:
        Optional file name (default ``lead.json``). Only a base name is allowed (no paths).

    Returns
    -------
    pathlib.Path
        Absolute path to the written file.

    Raises
    ------
    InvalidLeadJsonError
        If ``lead`` is not a ``dict``, or is not JSON-serializable.
    InvalidTempDirError
        If ``temp_dir`` is invalid.
    InvalidFilenameError
        If ``filename`` is not a safe base name.
    LeadJsonWriteError
        If the directory cannot be created or the file cannot be written.
    """
    if not isinstance(lead, Mapping) or isinstance(lead, (str, bytes, bytearray)):
        raise InvalidLeadJsonError(
            "lead must be a single JSON object (mapping/dict), not an array, string, or scalar."
        )

    # Shallow-normalize to a plain dict for a stable JSON object (not array/tuple keys).
    try:
        clean: JsonObject = dict(lead)
    except (TypeError, ValueError) as e:
        raise InvalidLeadJsonError(f"lead cannot be normalized to a dict: {e}") from e

    try:
        payload = json.dumps(clean, ensure_ascii=False, indent=2, sort_keys=False)
    except TypeError as e:
        raise InvalidLeadJsonError(f"lead is not JSON-serializable: {e}") from e

    root = _validate_temp_dir(temp_dir)
    out_name = _normalize_filename(filename)
    out_path = (root / out_name).resolve()

    try:
        root.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise LeadJsonWriteError(f"Cannot create temp_dir {root}: {e}") from e

    try:
        out_path.write_text(payload + "\n", encoding="utf-8", newline="\n")
    except OSError as e:
        raise LeadJsonWriteError(f"Cannot write {out_path}: {e}") from e

    return out_path.resolve()


# --- Usage example (doc / copy-paste) ---
if __name__ == "__main__":
    import tempfile

    example_lead: JsonObject = {
        "email": "contact@例え.jp",
        "company": "株式会社テスト",
        "score": 42,
    }
    tmp = Path(tempfile.mkdtemp(prefix="lead_json_"))
    saved = write_lead_json_to_temp_file(example_lead, tmp)
    print("Wrote:", saved)
    # File is UTF-8 with unescaped non-ASCII; open in an editor to inspect.
