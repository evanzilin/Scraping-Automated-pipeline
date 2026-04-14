import hashlib
import httpx

def is_duplicate(email: str) -> bool:
    lead_id = hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()

    resp = httpx.get(
        "https://subnet71.com/api/lead-search",
        params={"leadId": lead_id, "limit": 1000},
        timeout=20.0,
    )
    resp.raise_for_status()
    data = resp.json()

    if isinstance(data, list):
        return len(data) > 0

    if isinstance(data, dict):
        if isinstance(data.get("results"), list):
            return len(data["results"]) > 0
        if isinstance(data.get("data"), list):
            return len(data["data"]) > 0
        if isinstance(data.get("items"), list):
            return len(data["items"]) > 0
        if isinstance(data.get("count"), int):
            return data["count"] > 0
        if isinstance(data.get("total"), int):
            return data["total"] > 0

    return False