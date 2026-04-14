"""Enumerations and lightweight type aliases."""

from __future__ import annotations

from enum import Enum
from typing import Literal

SourceUsed = Literal["db", "cache", "scrapingdog", "mixed", "none"]


class EnrichmentStatus(str, Enum):
    MATCHED = "matched"
    PARTIALLY_MATCHED = "partially_matched"
    ENRICHED = "enriched"
    INSUFFICIENT_DATA = "insufficient_data"
    FAILED = "failed"
