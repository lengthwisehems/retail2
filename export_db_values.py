# -*- coding: utf-8 -*-
"""
Export all denim_analytics rows for ONE brand to two files:

  1. [BRAND]_DB_Values_YYYY-MM-DD_HH-MM-SS.json  - compact, lossless, meant to
     be handed to Claude so it can load real data and test database-fix code.
     Tables listed in JSON_DEDUPE are de-duplicated on the way out (see below)
     to keep this file small.
  2. [BRAND]_DB_Values_YYYY-MM-DD_HH-MM-SS.xlsx  - one worksheet per table for
     you to review by eye. Always written with EVERY row (no de-duplication).

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
BRAND = "AMO"

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

# Tables that get 10%-progress logging while they download, e.g.
# {"variant_metrics"}. Empty = every table just logs start and end, and is
# pulled in one plain fetch. Progress logging costs an extra SELECT COUNT(*)
# per table and downloads in chunks, so leave this empty unless you want the
# percentages; it does not change what ends up in either file.
PROGRESS_TABLES: set = set()

# --- JSON de-duplication ---------------------------------------------------
# This is Excel's Data > Remove Duplicates, applied to the JSON file only.
# For each table below, only the FIRST row of each distinct combination of the
# listed columns is kept; every later row with the same combination is dropped.
# Surviving rows keep all of their columns (same as Excel), unless you flip
# JSON_DEDUPE_KEEP_ONLY_KEY_COLUMNS to True.
#
# Column names are matched case-insensitively, and any name that isn't actually
# a column on the table is ignored (with a log line). Values compare the way
# Excel compares them: blanks match blanks, text matches ignoring case and
# leading/trailing spaces.
#
# Remove a table from this dict (or set it to []) to write that table's rows
# to the JSON untouched.
JSON_DEDUPE = {
    "variant_metrics": [
        "Action", "Brand", "sku_shopify", "sku_brand",
        "barcode", "variant_title", "size",
    ],
}

# False = keep every column on the surviving rows (what Excel does).
# True  = also throw away the non-key columns, for an even smaller JSON.
JSON_DEDUPE_KEEP_ONLY_KEY_COLUMNS = False

# Database connection --------------------------------------------------------
SQL_SERVER   = os.environ.get("SQL_SERVER",   "denim-sql.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "denim_analytics")
HARDCODED_USERNAME = "carrieboromisa"
HARDCODED_PASSWORD = ""          # fill this in locally, or use SQL_PASSWORD env var
SQL_USERNAME = HARDCODED_USERNAME or os.environ.get("SQL_USERNAME", "")
SQL_PASSWORD = HARDCODED_PASSWORD or os.environ.get("SQL_PASSWORD", "")

SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
FETCH_CHUNK = 2000   # rows per fetchmany batch (for progress reporting)


# ===========================================================================
# Central-time logging (every line is timestamped)
# ===========================================================================
def _resolve_tz():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/Chicago"), False
    except Exception:
        return dt.timezone(dt.timedelta(hours=-6)), True


_TZ, _TZ_FALLBACK = _resolve_tz()


def now_central() -> dt.datetime:
    return dt.datetime.now(_TZ)


def log(msg: str = "") -> None:
    print(f"[{now_central().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ===========================================================================
# Value serialization
# ===========================================================================
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


class Decile:
    """Emit a log line each time progress crosses the next 10% boundary."""
    def __init__(self, total: int, label: str, enabled: bool):
        self.total = total
        self.label = label
        self.enabled = enabled and total > 0
        self.next = 10

    def update(self, done: int) -> None:
        if not self.enabled:
            return
        while self.next <= 100 and done >= self.total * self.next / 100:
            log(f"   {self.label}: {self.next}% ({min(done, self.total)}/{self.total})")
            self.next += 10


# ===========================================================================
# JSON de-duplication (Excel's Data > Remove Duplicates)
# ===========================================================================
def _dedupe_cell(v):
    """Normalize one cell for duplicate comparison, the way Excel compares:
    blanks match blanks, text matches ignoring case and surrounding spaces."""
    v = json_safe(v)          # Decimal/date/bytes -> hashable, stable form
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip().casefold()
    return v


def dedupe_key_columns(tbl: str, cols: list) -> list:
    """Indexes of the columns JSON_DEDUPE wants for `tbl`, [] if it isn't set up."""
    wanted = {t.lower(): c for t, c in JSON_DEDUPE.items()}.get(tbl.lower())
    if not wanted:
        return []
    pos = {c.lower(): i for i, c in enumerate(cols)}
    missing = [w for w in wanted if w.lower() not in pos]
    if missing:
        log(f"   {tbl}: dedupe ignoring column(s) not on this table: "
            + ", ".join(missing))
    return [pos[w.lower()] for w in wanted if w.lower() in pos]


def dedupe_rows(tbl: str, cols: list, rows: list):
    """Return (cols, rows, key_column_names) with duplicates removed per JSON_DEDUPE.

    Keeps the first row of each distinct key combination and drops the rest -
    exactly what Excel's Data > Remove Duplicates does with those columns ticked.
    key_column_names comes back empty when this table isn't set up for dedupe.
    """
    key_idx = dedupe_key_columns(tbl, cols)
    if not key_idx:
        return cols, rows, []
    key_names = [cols[i] for i in key_idx]

    seen = set()
    kept = []
    for row in rows:
        key = tuple(_dedupe_cell(row[i]) for i in key_idx)
        if key in seen:
            continue
        seen.add(key)
        kept.append(row)

    log(f"   {tbl}: dedupe on [" + ", ".join(key_names) + "] - "
        f"{len(rows)} rows -> {len(kept)} ({len(rows) - len(kept)} duplicates removed)")

    if JSON_DEDUPE_KEEP_ONLY_KEY_COLUMNS:
        cols = key_names
        kept = [[row[i] for i in key_idx] for row in kept]
    return cols, kept, key_names


# ===========================================================================
# Fetch one table (chunked, with optional 10% progress)
# ===========================================================================
def fetch_table(cur, tbl: str, progress: bool):
    top = f"TOP {int(MAX_ROWS_PER_TABLE)} " if MAX_ROWS_PER_TABLE else ""

    total = None
    if progress:
        # Only used to turn row counts into percentages. If it fails (a big
        # table can time out on COUNT alone) fall back to a plain fetch - a
        # progress nicety must never cost us the table itself.
        try:
            cur.execute(f"SELECT COUNT(*) FROM [dbo].[{tbl}] WHERE brand = %s", (BRAND,))
            total = cur.fetchone()[0]
            if MAX_ROWS_PER_TABLE:
                total = min(total, int(MAX_ROWS_PER_TABLE))
            log(f"{tbl}: fetching {total} rows...")
        except Exception as exc:
            log(f"{tbl}: row-count for progress failed ({exc}); "
                "fetching without progress...")
            progress = False
    if not progress:
        log(f"{tbl}: fetching...")

    cur.execute(f"SELECT {top}* FROM [dbo].[{tbl}] WHERE brand = %s ORDER BY 1", (BRAND,))
    cols = [d[0] for d in cur.description]

    if not progress:
        rows = cur.fetchall()
    else:
        rows = []
        dec = Decile(total or 0, f"{tbl} fetch", True)
        while True:
            chunk = cur.fetchmany(FETCH_CHUNK)
            if not chunk:
                break
            rows.extend(chunk)
            dec.update(len(rows))
    return cols, rows


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

    stamp = now_central().strftime("%Y-%m-%d_%H-%M-%S")
    base = f"{BRAND}_DB_Values_{stamp}"
    log("=" * 58)
    log(f"EXPORT {BRAND} DB VALUES")
    log("Tables: " + ", ".join(table_short(t) for t in selected))
    if _TZ_FALLBACK:
        log("(tzdata not found - timestamps use a fixed UTC-6; "
            "pip install tzdata for exact Central time)")
    log("=" * 58)

    import pymssql
    log("Connecting to database...")
    conn = pymssql.connect(server=SQL_SERVER, user=SQL_USERNAME,
                           password=SQL_PASSWORD, database=SQL_DATABASE,
                           timeout=600, login_timeout=60)
    cur = conn.cursor()

    export: dict = {}
    failed: dict = {}
    for t in selected:
        tbl = table_short(t)
        progress = tbl.lower() in {p.lower() for p in PROGRESS_TABLES}
        try:
            cols, rows = fetch_table(cur, tbl, progress)
        except Exception as exc:
            log("*" * 58)
            log(f"*** {tbl}: SKIPPED - query failed: {exc}")
            log(f"*** {tbl} will be MISSING from both output files.")
            log("*" * 58)
            failed[tbl] = str(exc)
            continue
        export[tbl] = {"columns": cols, "row_count": len(rows), "rows": rows}
        log(f"{tbl}: done - {len(rows)} rows, {len(cols)} columns")

    conn.close()

    if not export:
        sys.exit("No data pulled - nothing written.")

    # ---- 1) Claude-readable JSON (de-duplicated per JSON_DEDUPE) -----------
    if WRITE_JSON:
        log("Writing Claude JSON...")
        tables_out = {}
        for tbl, d in export.items():
            cols, rows, key_names = dedupe_rows(tbl, d["columns"], d["rows"])
            entry = {
                "columns": cols,
                "row_count": len(rows),
                "rows": [[json_safe(v) for v in row] for row in rows],
            }
            if key_names:
                # so whoever reads this file knows it isn't the raw table
                entry["deduped_on"] = key_names
                entry["row_count_before_dedupe"] = d["row_count"]
            tables_out[tbl] = entry

        payload = {
            "brand": BRAND,
            "generated_at_central": stamp,
            "database": SQL_DATABASE,
            "tables": tables_out,
        }
        if failed:
            payload["tables_that_failed"] = failed
        json_path = SCRIPT_DIR / f"{base}.json"
        with json_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=1)
        log(f"Claude JSON written: {json_path}")

    # ---- 2) Excel review workbook (always the full, un-deduped rows) -------
    if WRITE_XLSX:
        try:
            from openpyxl import Workbook
            from openpyxl.utils import get_column_letter
        except ImportError:
            log("Excel skipped - run 'pip install openpyxl' to enable it "
                "(the JSON was still written).")
        else:
            log("Writing Excel workbook...")
            wb = Workbook()
            wb.remove(wb.active)
            for tbl, d in export.items():
                ws = wb.create_sheet(title=tbl[:31])
                ws.append(d["columns"])
                progress = tbl.lower() in {p.lower() for p in PROGRESS_TABLES}
                dec = Decile(d["row_count"], f"{tbl} xlsx", progress)
                for i, row in enumerate(d["rows"], start=1):
                    ws.append([xlsx_safe(v) for v in row])
                    dec.update(i)
                ws.freeze_panes = "A2"
                for i, cname in enumerate(d["columns"], start=1):
                    ws.column_dimensions[get_column_letter(i)].width = \
                        min(max(len(str(cname)) + 2, 12), 60)
                log(f"{tbl}: sheet written ({d['row_count']} rows)")
            xlsx_path = SCRIPT_DIR / f"{base}.xlsx"
            log("Saving Excel file (this can take a moment for large tables)...")
            wb.save(xlsx_path)
            log(f"Excel file written: {xlsx_path}")

    log("Tables exported: " + ", ".join(f"{t} ({d['row_count']} rows)"
                                        for t, d in export.items()))
    if failed:
        log("!" * 58)
        log("!!! TABLES MISSING FROM BOTH FILES: " + ", ".join(failed))
        for t, exc in failed.items():
            log(f"!!!   {t}: {exc}")
        log("!" * 58)
    log("Done.")


if __name__ == "__main__":
    main()
