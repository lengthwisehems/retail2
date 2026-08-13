# -*- coding: utf-8 -*-
"""
Export all denim_analytics rows for ONE brand to two files:

  1. [BRAND]_DB_Values_YYYY-MM-DD_HH-MM-SS.json  - compact, lossless, meant to
     be handed to Claude so it can load real data and test database-fix code.
  2. [BRAND]_DB_Values_YYYY-MM-DD_HH-MM-SS.xlsx  - one worksheet per table for
     you to review by eye.

Both files land in the SAME folder as this script. Timestamp is Central time.

CONFIGURE below: set BRAND, then flip each table True (pull it) / False (skip).

RUN (Windows Command Prompt - the black "cmd" window):
    pip install pymssql openpyxl
    set SQL_USERNAME=carrieboromisa
    set SQL_PASSWORD=your-password
    python "C:\\path\\to\\export_db_values.py"
(or fill HARDCODED_USERNAME / HARDCODED_PASSWORD below to skip the set lines)
"""
from __future__ import annotations

import json
import os
import sys
import datetime as dt
from decimal import Decimal
from pathlib import Path

# ===========================================================================
# CONFIGURE
# ===========================================================================
BRAND = "TRIARCHY"

# True  = include this table in the export
# False = skip it
TABLES = {
    "dbo.lookup":            True,
    "dbo.style_info":        True,
    "dbo.style_metrics":     True,
    "dbo.variant_metrics":   True,
    "dbo.image_url_history": False,   # flip True if you want image history too
}

WRITE_JSON = True    # the Claude-readable file
WRITE_XLSX = True    # the Excel review file
MAX_ROWS_PER_TABLE = None   # e.g. 5000 to cap; None = no cap (pull everything)

# Database connection --------------------------------------------------------
SQL_SERVER   = os.environ.get("SQL_SERVER",   "denim-sql.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "denim_analytics")
HARDCODED_USERNAME = ""
HARDCODED_PASSWORD = ""
SQL_USERNAME = HARDCODED_USERNAME or os.environ.get("SQL_USERNAME", "")
SQL_PASSWORD = HARDCODED_PASSWORD or os.environ.get("SQL_PASSWORD", "")

SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


# ===========================================================================
# Helpers
# ===========================================================================
def central_now() -> dt.datetime:
    """Current time in US Central (CST/CDT). Falls back to a fixed -6:00
    offset if the IANA tz database isn't installed (pip install tzdata)."""
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo("America/Chicago"))
    except Exception:
        print("   (note: tzdata not found - using fixed UTC-6 for the timestamp; "
              "pip install tzdata for exact Central time)")
        return dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=6)


def json_safe(v):
    if v is None:
        return None
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return v.decode("latin-1", errors="replace")
    return v


def xlsx_safe(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (bytes, bytearray)):
        return v.decode("latin-1", errors="replace")
    if isinstance(v, str) and len(v) > 32767:   # Excel hard cell limit
        return v[:32764] + "..."
    return v   # None, int, float, str, datetime/date are fine for openpyxl


def table_short(name: str) -> str:
    """'dbo.style_info' -> 'style_info'."""
    return name.split(".")[-1].strip().strip("[]")


# ===========================================================================
# Main
# ===========================================================================
def main() -> None:
    selected = [t for t, on in TABLES.items() if on]
    if not selected:
        sys.exit("Nothing to do - every table in TABLES is False.")
    if not SQL_USERNAME or not SQL_PASSWORD:
        sys.exit("ERROR: set SQL_USERNAME and SQL_PASSWORD (env vars) or fill "
                 "HARDCODED_USERNAME/PASSWORD near the top of this file.")

    stamp = central_now().strftime("%Y-%m-%d_%H-%M-%S")
    base = f"{BRAND}_DB_Values_{stamp}"
    print("=" * 64)
    print(f"EXPORT {BRAND} DB VALUES  ({stamp} CST)")
    print("Tables:", ", ".join(table_short(t) for t in selected))
    print("=" * 64)

    import pymssql
    conn = pymssql.connect(server=SQL_SERVER, user=SQL_USERNAME,
                           password=SQL_PASSWORD, database=SQL_DATABASE,
                           timeout=600, login_timeout=60)
    cur = conn.cursor()

    export: dict = {}
    for t in selected:
        tbl = table_short(t)
        top = f"TOP {int(MAX_ROWS_PER_TABLE)} " if MAX_ROWS_PER_TABLE else ""
        try:
            cur.execute(f"SELECT {top}* FROM [dbo].[{tbl}] WHERE brand = %s "
                        f"ORDER BY 1", (BRAND,))
        except Exception as exc:
            print(f"   {tbl:<20} SKIPPED - query failed: {exc}")
            continue
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        export[tbl] = {"columns": cols, "row_count": len(rows), "rows": rows}
        print(f"   {tbl:<20} {len(rows):>7} rows, {len(cols)} columns")

    conn.close()

    if not export:
        sys.exit("No data pulled - nothing written.")

    # ---- 1) Claude-readable JSON ------------------------------------------
    json_path = SCRIPT_DIR / f"{base}.json"
    if WRITE_JSON:
        payload = {
            "brand": BRAND,
            "generated_at_central": stamp,
            "database": SQL_DATABASE,
            "tables": {
                tbl: {
                    "columns": d["columns"],
                    "row_count": d["row_count"],
                    "rows": [[json_safe(v) for v in row] for row in d["rows"]],
                }
                for tbl, d in export.items()
            },
        }
        with json_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=1)
        print(f"\nClaude JSON : {json_path}")

    # ---- 2) Excel review workbook -----------------------------------------
    if WRITE_XLSX:
        try:
            from openpyxl import Workbook
            from openpyxl.utils import get_column_letter
        except ImportError:
            print("\n(xlsx skipped - run 'pip install openpyxl' to enable the "
                  "Excel file; the JSON was still written.)")
        else:
            wb = Workbook()
            wb.remove(wb.active)
            for tbl, d in export.items():
                ws = wb.create_sheet(title=tbl[:31])
                ws.append(d["columns"])
                for row in d["rows"]:
                    ws.append([xlsx_safe(v) for v in row])
                # freeze header + reasonable column widths
                ws.freeze_panes = "A2"
                for i, cname in enumerate(d["columns"], start=1):
                    ws.column_dimensions[get_column_letter(i)].width = \
                        min(max(len(str(cname)) + 2, 12), 60)
            xlsx_path = SCRIPT_DIR / f"{base}.xlsx"
            wb.save(xlsx_path)
            print(f"Excel file  : {xlsx_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
