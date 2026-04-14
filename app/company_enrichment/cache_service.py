"""Redis (async) cache with optional in-memory fallback for tests."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

CACHE_PREFIX = "company_enrichment:"


def cache_key_for_domain(normalized_domain: str) -> str:
    return f"{CACHE_PREFIX}{normalized_domain.lower().strip()}"


@runtime_checkable
class CacheProtocol(Protocol):
    async def mget(self, keys: List[str]) -> List[Optional[bytes]]: ...

    async def mset_json(self, mapping: Dict[str, Dict[str, Any]], ttl_seconds: int) -> None: ...


class MemoryCache:
    """Process-local async cache (tests / no Redis)."""

    __slots__ = ("_data", "_ttl")

    def __init__(self, default_ttl_seconds: int = 86_400) -> None:
        self._data: Dict[str, tuple[float, str]] = {}
        self._ttl = default_ttl_seconds

    async def mget(self, keys: List[str]) -> List[Optional[bytes]]:
        now = time.time()
        out: List[Optional[bytes]] = []
        for k in keys:
            ent = self._data.get(k)
            if not ent:
                out.append(None)
                continue
            exp, payload = ent
            if exp < now:
                del self._data[k]
                out.append(None)
            else:
                out.append(payload.encode("utf-8"))
        return out

    async def mset_json(self, mapping: Dict[str, Dict[str, Any]], ttl_seconds: int) -> None:
        now = time.time()
        exp = now + ttl_seconds
        for k, v in mapping.items():
            self._data[k] = (exp, json.dumps(v, separators=(",", ":"), default=str))


class RedisCache:
    """redis.asyncio with pipeline MSET where possible."""

    __slots__ = ("_r", "_default_ttl")

    def __init__(self, redis_client: Any, default_ttl_seconds: int = 86_400) -> None:
        self._r = redis_client
        self._default_ttl = default_ttl_seconds

    async def mget(self, keys: List[str]) -> List[Optional[bytes]]:
        if not keys:
            return []
        return list(await self._r.mget(keys))

    async def mset_json(self, mapping: Dict[str, Dict[str, Any]], ttl_seconds: int) -> None:
        if not mapping:
            return
        ttl = ttl_seconds or self._default_ttl
        # Use pipeline: SET ... EX for each (MSET cannot set per-key TTL in one call)
        pipe = self._r.pipeline(transaction=False)
        for k, obj in mapping.items():
            payload = json.dumps(obj, separators=(",", ":"), default=str)
            pipe.set(k, payload, ex=ttl)
        await pipe.execute()
        logger.debug("cache mset %s keys ttl=%s", len(mapping), ttl)


def decode_cache_entry(raw: Optional[bytes]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None


def build_cache_payload(
    enriched: Dict[str, Any],
    final_resolved: Dict[str, Any],
    status: str,
) -> Dict[str, Any]:
    return {
        "enriched": enriched,
        "final_resolved": final_resolved,
        "status": status,
        "ts": time.time(),
    }
