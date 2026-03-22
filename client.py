"""StatFin PxWeb API client — all HTTP and parsing logic."""

import asyncio
import time
from collections import deque
from itertools import product as iterproduct

import httpx

BASE_URL = "https://statfin.stat.fi/PXWeb/api/v1"
DEFAULT_LANG = "fi"
MAX_CELLS = 100_000
API_CELL_LIMIT = 120_000
RATE_WINDOW = 10  # seconds
RATE_MAX = 30

# Module-level request tracker
_request_timestamps: deque[float] = deque(maxlen=RATE_MAX)


async def _rate_limit():
    """Enforce max 30 requests per 10 seconds."""
    now = time.monotonic()
    if len(_request_timestamps) == RATE_MAX:
        oldest = _request_timestamps[0]
        elapsed = now - oldest
        if elapsed < RATE_WINDOW:
            wait = RATE_WINDOW - elapsed + 0.05
            await asyncio.sleep(wait)
    _request_timestamps.append(time.monotonic())


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=30.0)


async def _get(url: str, params: dict | None = None) -> dict | list:
    """GET with rate limiting and retry on 429."""
    await _rate_limit()
    async with _client() as c:
        r = await c.get(url, params=params)
        if r.status_code == 429:
            await asyncio.sleep(10)
            await _rate_limit()
            r = await c.get(url, params=params)
        r.raise_for_status()
        return r.json()


async def _post(url: str, body: dict) -> dict:
    """POST with rate limiting and retry on 429."""
    await _rate_limit()
    async with _client() as c:
        r = await c.post(url, json=body)
        if r.status_code == 429:
            await asyncio.sleep(10)
            await _rate_limit()
            r = await c.post(url, json=body)
        if r.status_code == 400:
            return {"error": "Query too large or invalid", "status": 400}
        r.raise_for_status()
        return r.json()


def parse_jsonstat2(data: dict) -> list[dict]:
    """Convert json-stat2 response to list of flat dicts."""
    # The top-level key may be the dataset id or "dataset"
    if "id" in data and "dimension" in data:
        dataset = data
    elif "dataset" in data:
        dataset = data["dataset"]
    else:
        # Try first key
        first_key = next(iter(data))
        dataset = data[first_key]

    dims = dataset["dimension"]
    dim_ids = dataset["id"]
    dim_sizes = dataset["size"]
    values = dataset["value"]

    categories: dict[str, dict[int, str]] = {}
    for dim_id in dim_ids:
        cats = dims[dim_id]["category"]
        idx_map = cats.get("index", {})
        label_map = cats.get("label", {})
        # Build int-index → label
        if idx_map:
            categories[dim_id] = {v: label_map.get(k, k) for k, v in idx_map.items()}
        else:
            categories[dim_id] = {int(k): v for k, v in label_map.items()}

    results = []
    ranges = [range(s) for s in dim_sizes]
    for i, combo in enumerate(iterproduct(*ranges)):
        row = {}
        for dim_id, idx in zip(dim_ids, combo):
            row[dim_id] = categories[dim_id].get(idx, str(idx))
        row["value"] = values[i] if i < len(values) else None
        results.append(row)
    return results


# ---------------------------------------------------------------------------
# Public API functions called by the MCP tools
# ---------------------------------------------------------------------------


async def get_api_status() -> dict:
    url = f"{BASE_URL}/{DEFAULT_LANG}/?config"
    return await _get(url)


async def browse_folders(path: str = "") -> list:
    path = path.strip("/")
    url = f"{BASE_URL}/{DEFAULT_LANG}/StatFin/{path}" if path else f"{BASE_URL}/{DEFAULT_LANG}/StatFin"
    return await _get(url)


async def search_tables(query: str, path: str = "") -> list:
    path = path.strip("/")
    url = f"{BASE_URL}/{DEFAULT_LANG}/StatFin/{path}" if path else f"{BASE_URL}/{DEFAULT_LANG}/StatFin"
    return await _get(url, params={"query": query})


async def get_table_info(table_url: str) -> dict:
    return await _get(table_url)


async def get_table_variables(table_url: str) -> list:
    info = await _get(table_url)
    return info.get("variables", [])


async def search_regions(query: str) -> list:
    url = f"{BASE_URL}/{DEFAULT_LANG}/StatFin"
    results = await _get(url, params={"query": query, "filter": "codes"})
    return results


async def find_region_code(name: str, table_url: str) -> list[dict]:
    info = await _get(table_url)
    variables = info.get("variables", [])
    # Find the region variable (typically "Alue")
    region_var = None
    for v in variables:
        if v["code"].lower() in ("alue", "kunta", "maakunta", "region"):
            region_var = v
            break
    if region_var is None:
        # Fallback: first variable
        region_var = variables[0] if variables else None
    if region_var is None:
        return []

    name_lower = name.lower()
    matches = []
    for code, text in zip(region_var["values"], region_var["valueTexts"]):
        if name_lower in text.lower() or name_lower in code.lower():
            matches.append({"code": code, "text": text})
    return matches


async def test_selection(table_url: str, selections: dict) -> dict:
    info = await _get(table_url)
    variables = info.get("variables", [])

    total = 1
    checked = []
    for v in variables:
        code = v["code"]
        if code in selections:
            sel = selections[code]
            if sel == ["*"]:
                count = len(v["values"])
            else:
                count = len(sel)
        elif v.get("elimination", False):
            count = 1  # can be omitted
        else:
            count = len(v["values"])  # all values selected by default
        total *= count
        checked.append({"variable": code, "selected": count})

    return {
        "estimated_cells": total,
        "within_limit": total <= MAX_CELLS,
        "limit": MAX_CELLS,
        "api_hard_limit": API_CELL_LIMIT,
        "variables_checked": checked,
    }


async def preview_data(table_url: str) -> list[dict]:
    info = await _get(table_url)
    variables = info.get("variables", [])

    query_parts = []
    for v in variables:
        query_parts.append({
            "code": v["code"],
            "selection": {"filter": "top", "values": ["2"]},
        })

    body = {"query": query_parts, "response": {"format": "json-stat2"}}
    result = await _post(table_url, body)
    if "error" in result:
        return [result]
    rows = parse_jsonstat2(result)
    return rows[:20]


async def get_table_data(table_url: str, selections: dict, lang: str = DEFAULT_LANG) -> list[dict]:
    # Use the requested language in the URL
    actual_url = table_url
    if lang != DEFAULT_LANG:
        actual_url = table_url.replace(f"/{DEFAULT_LANG}/", f"/{lang}/")

    # Fetch metadata for validation
    info = await _get(actual_url)
    variables = info.get("variables", [])

    var_map = {v["code"]: v for v in variables}

    # Validate codes
    for sel_code, sel_values in selections.items():
        if sel_code not in var_map:
            return [{"error": f"Variable '{sel_code}' not found in table. Available: {list(var_map.keys())}"}]
        if sel_values != ["*"]:
            valid = set(var_map[sel_code]["values"])
            invalid = [v for v in sel_values if v not in valid]
            if invalid:
                return [{"error": f"Invalid values for '{sel_code}': {invalid}. Valid: {var_map[sel_code]['values'][:20]}..."}]

    # Cell count check
    total = 1
    for v in variables:
        code = v["code"]
        if code in selections:
            sel = selections[code]
            if sel == ["*"]:
                total *= len(v["values"])
            else:
                total *= len(sel)
        elif v.get("elimination", False):
            pass  # omitted
        else:
            total *= len(v["values"])

    if total > MAX_CELLS:
        return [{
            "error": f"Query would return {total:,} cells, exceeding limit of {MAX_CELLS:,}. "
                     f"Reduce selections or split the query."
        }]

    # Build query
    query_parts = []
    for v in variables:
        code = v["code"]
        if code in selections:
            sel = selections[code]
            if sel == ["*"]:
                query_parts.append({
                    "code": code,
                    "selection": {"filter": "all", "values": ["*"]},
                })
            else:
                query_parts.append({
                    "code": code,
                    "selection": {"filter": "item", "values": sel},
                })
        elif not v.get("elimination", False):
            # Must include non-eliminable variables with all values
            query_parts.append({
                "code": code,
                "selection": {"filter": "all", "values": ["*"]},
            })

    body = {"query": query_parts, "response": {"format": "json-stat2"}}
    result = await _post(actual_url, body)

    if "error" in result:
        return [result]

    rows = parse_jsonstat2(result)
    for row in rows:
        row["source"] = "Statistics Finland / Tilastokeskus, StatFin"
    return rows


def check_usage() -> dict:
    now = time.monotonic()
    recent = [t for t in _request_timestamps if now - t < RATE_WINDOW]
    return {
        "requests_last_10s": len(recent),
        "max_requests_per_10s": RATE_MAX,
        "approaching_limit": len(recent) >= RATE_MAX - 5,
        "max_values_per_query": API_CELL_LIMIT,
        "soft_limit": MAX_CELLS,
    }
