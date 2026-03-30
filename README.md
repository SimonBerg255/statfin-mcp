# StatFin MCP Server

An MCP server that gives AI assistants direct access to Statistics Finland's [StatFin](https://statfin.stat.fi/) database — thousands of official Finnish statistical tables covering population, employment, economy, housing, education, health, and more.

Built on the PxWeb REST API. No API key needed. All data is open and licensed under CC BY 4.0.

The server handles the full workflow: browsing the folder tree, searching for tables, inspecting variable codes, validating query size against rate limits, and fetching parsed data — so the AI never has to guess codes or risk oversized requests.

## Tools

| Tool | Description |
|------|-------------|
| `statfin_get_api_status` | Check API configuration and rate limits |
| `statfin_browse_folders` | Navigate the StatFin folder hierarchy |
| `statfin_search_tables` | Free-text search for tables |
| `statfin_get_table_info` | Get full metadata for a table (variables, codes, values) |
| `statfin_get_table_variables` | Compact view of a table's dimensions |
| `statfin_search_regions` | Search for Finnish municipality/region codes |
| `statfin_find_region_code` | Find region codes within a specific table |
| `statfin_test_selection` | Calculate cell count before fetching (prevents 400 errors) |
| `statfin_preview_data` | Quick data preview using top-2 values per variable |
| `statfin_get_table_data` | Fetch statistical data with full validation |
| `statfin_check_usage` | Monitor rate limit status |

## How it works

1. **Browse or search** to find a table (`statfin_browse_folders`, `statfin_search_tables`)
2. **Inspect** the table to see its variables and value codes (`statfin_get_table_info`)
3. **Test** the selection size to stay under the 100,000 cell limit (`statfin_test_selection`)
4. **Fetch** the data (`statfin_get_table_data`)

## Quick start

```bash
git clone https://github.com/SimonBerg255/statfin-mcp.git
cd statfin-mcp
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn server:app --host 0.0.0.0 --port 8000
```

## API details

- **Base URL:** `https://statfin.stat.fi/PXWeb/api/v1`
- **Protocol:** PxWeb REST API
- **Auth:** None (fully public)
- **Rate limits:** 30 requests per 10 seconds, 120,000 cells per POST query
- **Response format:** json-stat2
- **License:** CC BY 4.0

## Finnish region codes

StatFin uses `KU` + 3-digit municipal codes:

| Code | Municipality |
|------|-------------|
| KU091 | Helsinki |
| KU049 | Espoo |
| KU092 | Vantaa |
| KU837 | Tampere |
| KU853 | Turku |
| KU564 | Oulu |

Use `statfin_find_region_code` to look up codes for any municipality.

## Validation

```bash
python3 validate.py
```

Runs 6 live tests against the StatFin API: API status, folder browsing, table search, region lookup, cell count check, and data fetch with sanity check on Helsinki population figures.

## License

MIT
