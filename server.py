"""StatFin MCP Server — Statistics Finland's StatFin database via PxWeb API."""

from fastmcp import FastMCP
from mcp.server.fastmcp import Icon

import client

# ---------------------------------------------------------------------------
# Server metadata
# ---------------------------------------------------------------------------

icon = Icon(
    src="https://raw.githubusercontent.com/SimonBerg255/statfin-mcp/main/statfin_logo.png",
)

INSTRUCTION_STRING = (
    "This server provides access to Statistics Finland's StatFin database. "
    "It lets you browse the folder structure, search for tables, inspect table metadata, "
    "and fetch statistical data. All data is sourced from the free PxWeb API at "
    "https://statfin.stat.fi/PXWeb/api/v1 and licensed under CC BY 4.0. "
    "Always call statfin_test_selection before statfin_get_table_data on large tables "
    "to avoid exceeding the 100,000 cell limit."
)

mcp = FastMCP(
    name="StatFin MCP Server",
    instructions=INSTRUCTION_STRING,
    version="1.0.0",
    website_url="https://statfin.stat.fi/",
    icons=[icon],
)


# ---------------------------------------------------------------------------
# Tools — all with requires_permission: False for automatic Intric execution
# ---------------------------------------------------------------------------


@mcp.tool(meta={"requires_permission": False})
async def statfin_get_api_status() -> dict:
    """Get StatFin API configuration and rate limit information.

    Returns the API's configuration including rate limits and supported features.
    Use this to verify the API is reachable.

    args: (none)

    returns:
        API configuration dict with rate limits and settings.
    """
    try:
        return await client.get_api_status()
    except Exception as e:
        return {"error": str(e)}


@mcp.tool(meta={"requires_permission": False})
async def statfin_browse_folders(path: str = "") -> list | dict:
    """Browse the StatFin database folder structure.

    Navigate the hierarchical subject-area structure of StatFin.
    Each item has type "l" (folder) or "t" (table).

    args:
        path: Folder path relative to StatFin root, e.g. "vrm" or "vrm/vaerak".
              Empty string returns the root level subject areas.

    returns:
        List of {id, type, text} items. type "l" = folder, "t" = table.
    """
    try:
        return await client.browse_folders(path)
    except Exception as e:
        return {"error": str(e)}


@mcp.tool(meta={"requires_permission": False})
async def statfin_search_tables(query: str, path: str = "") -> list | dict:
    """Search for tables in StatFin by free text.

    Searches table names and descriptions. Optionally scope to a subfolder.

    args:
        query: Search term, e.g. "väestö" (population), "työllisyys" (employment).
        path: Optional folder path to scope the search, e.g. "vrm".

    returns:
        List of matching tables with id, text, and path information.
    """
    try:
        return await client.search_tables(query, path)
    except Exception as e:
        return {"error": str(e)}


@mcp.tool(meta={"requires_permission": False})
async def statfin_get_table_info(table_url: str) -> dict:
    """Get full metadata for a StatFin table.

    Fetches the table title and all variables with their codes and value lists.
    This is the essential step before building a data query.

    args:
        table_url: Full URL to the table including .px extension,
                   e.g. "https://statfin.stat.fi/PXWeb/api/v1/fi/StatFin/vrm/vaerak/statfin_vaerak_pxt_11ra.px"

    returns:
        Dict with title and variables list containing codes, texts, values, valueTexts.
    """
    try:
        return await client.get_table_info(table_url)
    except Exception as e:
        return {"error": str(e)}


@mcp.tool(meta={"requires_permission": False})
async def statfin_get_table_variables(table_url: str) -> list | dict:
    """Get just the variables (dimensions) of a StatFin table.

    Compact view showing what dimensions exist and their possible values.
    Useful for quickly understanding table structure before querying.

    args:
        table_url: Full URL to the table including .px extension.

    returns:
        List of variable dicts with code, text, values, valueTexts.
    """
    try:
        return await client.get_table_variables(table_url)
    except Exception as e:
        return {"error": str(e)}


@mcp.tool(meta={"requires_permission": False})
async def statfin_search_regions(query: str) -> list | dict:
    """Search for Finnish municipality/region codes.

    Searches by name or code across StatFin. Useful for finding KU-codes
    (e.g. KU091 = Helsinki) before building data queries.

    args:
        query: Region name or code to search for, e.g. "Helsinki" or "KU091".

    returns:
        List of matching items with code and text.
    """
    try:
        return await client.search_regions(query)
    except Exception as e:
        return {"error": str(e)}


@mcp.tool(meta={"requires_permission": False})
async def statfin_find_region_code(name: str, table_url: str) -> list | dict:
    """Find region codes within a specific table.

    More reliable than statfin_search_regions because it searches the actual
    region variable of the target table. Some tables use non-standard region sets.

    args:
        name: Region name to search for (case-insensitive substring match),
              e.g. "Helsinki", "Tampere", "Espoo".
        table_url: Full URL to the table including .px extension.

    returns:
        List of matching {code, text} pairs from the table's region variable.
    """
    try:
        return await client.find_region_code(name, table_url)
    except Exception as e:
        return {"error": str(e)}


@mcp.tool(meta={"requires_permission": False})
async def statfin_test_selection(table_url: str, selections: dict) -> dict:
    """Calculate the cell count for a proposed query without fetching data.

    ALWAYS run this before statfin_get_table_data on tables with many regions or years
    to avoid exceeding the 100,000 cell limit.

    args:
        table_url: Full URL to the table including .px extension.
        selections: Dict mapping variable codes to lists of value codes,
                    e.g. {"Alue": ["KU091"], "Vuosi": ["2023", "2024"], "Tiedot": ["vaesto"]}.
                    Use ["*"] to select all values for a variable.

    returns:
        Dict with estimated_cells, within_limit (bool), limit, and variables_checked.
    """
    try:
        return await client.test_selection(table_url, selections)
    except Exception as e:
        return {"error": str(e)}


@mcp.tool(meta={"requires_permission": False})
async def statfin_preview_data(table_url: str) -> list | dict:
    """Quick preview of a table's data using top-2 values per variable.

    Fetches a small sample to see what the data looks like before
    constructing a full query. Returns at most 20 rows.

    args:
        table_url: Full URL to the table including .px extension.

    returns:
        List of flat dicts with dimension labels and data values (max 20 rows).
    """
    try:
        return await client.preview_data(table_url)
    except Exception as e:
        return {"error": str(e)}


@mcp.tool(meta={"requires_permission": False})
async def statfin_get_table_data(table_url: str, selections: dict, lang: str = "fi") -> list | dict:
    """Fetch statistical data from a StatFin table.

    The main data retrieval tool. Validates all codes, checks cell count,
    and returns parsed data rows.

    args:
        table_url: Full URL to the table including .px extension.
        selections: Dict mapping variable codes to lists of value codes.
                    Example: {"Alue": ["KU091", "KU837"], "Vuosi": ["2022", "2023"], "Tiedot": ["vaesto"]}.
                    Use ["*"] to select all values for a variable.
        lang: Language code: "fi" (Finnish, default), "sv" (Swedish), or "en" (English).

    returns:
        List of flat dicts with dimension labels, data values, and source attribution.
    """
    try:
        return await client.get_table_data(table_url, selections, lang)
    except Exception as e:
        return {"error": str(e)}


@mcp.tool(meta={"requires_permission": False})
async def statfin_check_usage() -> dict:
    """Check current API usage and rate limit status.

    Shows how many requests have been made in the last 10 seconds
    and whether you're approaching the rate limit.

    args: (none)

    returns:
        Dict with requests_last_10s, max_requests_per_10s, approaching_limit, and limits.
    """
    try:
        return client.check_usage()
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# ASGI app for uvicorn (Intric-compatible HTTP transport)
# ---------------------------------------------------------------------------

app = mcp.http_app()
