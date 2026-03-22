#!/usr/bin/env python3
"""Validation script for the StatFin MCP server — tests core API functions."""

import asyncio
import sys

import client

POPULATION_TABLE = (
    "https://statfin.stat.fi/PXWeb/api/v1/fi/StatFin/vaerak/statfin_vaerak_pxt_11ra.px"
)

passed = 0
failed = 0


def report(name: str, ok: bool, detail: str = ""):
    global passed, failed
    status = "PASS" if ok else "FAIL"
    if ok:
        passed += 1
    else:
        failed += 1
    print(f"  [{status}] {name}" + (f"  — {detail}" if detail else ""))


async def main():
    print("StatFin MCP Validation")
    print("=" * 50)

    # 1. API status
    print("\n1. statfin_get_api_status()")
    try:
        result = await client.get_api_status()
        ok = isinstance(result, dict) and len(result) > 0
        report("Returns config dict", ok, f"keys: {list(result.keys())[:5]}")
    except Exception as e:
        report("Returns config dict", False, str(e))

    # 2. Browse folders
    print("\n2. statfin_browse_folders('')")
    try:
        result = await client.browse_folders("")
        ok = isinstance(result, list) and len(result) >= 10
        report(f"Returns >=10 folders", ok, f"got {len(result)} items")
    except Exception as e:
        report("Returns >=10 folders", False, str(e))

    # 3. Search tables
    print("\n3. statfin_search_tables('väestö')")
    try:
        result = await client.search_tables("väestö")
        ok = isinstance(result, list) and len(result) >= 5
        report(f"Returns >=5 results", ok, f"got {len(result)} results")
    except Exception as e:
        report("Returns >=5 results", False, str(e))

    # 4. Find region code
    print(f"\n4. statfin_find_region_code('Helsinki', ...)")
    try:
        result = await client.find_region_code("Helsinki", POPULATION_TABLE)
        codes = [r["code"] for r in result]
        ok = "KU091" in codes
        report("Finds KU091 for Helsinki", ok, f"matches: {result[:3]}")
    except Exception as e:
        report("Finds KU091 for Helsinki", False, str(e))

    # 5. Test selection
    print("\n5. statfin_test_selection(...)")
    try:
        result = await client.test_selection(POPULATION_TABLE, {
            "Alue": ["KU091"],
            "Vuosi": ["2024"],
            "Tiedot": ["vaesto"],
        })
        ok = result.get("within_limit") is True
        report("within_limit is True", ok, f"cells: {result.get('estimated_cells')}")
    except Exception as e:
        report("within_limit is True", False, str(e))

    # 6. Get table data
    print("\n6. statfin_get_table_data(...)")
    try:
        result = await client.get_table_data(POPULATION_TABLE, {
            "Alue": ["KU091"],
            "Vuosi": ["2022", "2023", "2024"],
            "Tiedot": ["vaesto"],
        })
        ok_rows = isinstance(result, list) and len(result) == 3
        report(f"Returns 3 rows", ok_rows, f"got {len(result)} rows")

        if ok_rows:
            values = [r.get("value") for r in result]
            all_numeric = all(isinstance(v, (int, float)) and v is not None for v in values)
            report("Values are numeric", all_numeric, f"values: {values}")

            # Helsinki population sanity check (roughly 600k-700k)
            latest = values[-1] if values else 0
            ok_range = 600_000 <= latest <= 700_000 if latest else False
            report(f"Helsinki pop ~600k-700k", ok_range, f"latest: {latest}")
    except Exception as e:
        report("Returns 3 rows", False, str(e))

    # Summary
    print("\n" + "=" * 50)
    total = passed + failed
    print(f"Results: {passed}/{total} passed, {failed} failed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    code = asyncio.run(main())
    sys.exit(code)
