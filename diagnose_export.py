# -*- coding: utf-8 -*-
"""
Diagnose WHY export_db_values.py can't pull variant_metrics. READ-ONLY.

Round 1 showed every access to variant_metrics is pathologically slow: a plain
COUNT of 417k AMO rows took 213s and a 20k-row pull took 227s (~88 rows/s). So
the SORT was never the real problem - just READING the table is this slow. This
round uses BOUNDED, fast probes (small clustered-PK windows + metadata) to pin
down the exact cause and the fix:

  1. Table size from metadata (instant) - is variant_metrics huge across all
     brands? (a non-selective brand => scans dominate)
  2. PK bounds (instant).
  3. A small PK-range window (fast clustered seek), timed three ways:
       a. COUNT of the window            -> raw server scan rate (rows/s)
       b. SELECT *  of the window         -> WIDE-row stream rate (rows/s)
       c. SELECT one column of the window -> NARROW stream rate (rows/s)
     b<<a  => stream-bound (shipping wide rows is the wall).
     a slow => scan-bound / throttled tier.
  4. Force the brand index for a COUNT - if that's fast, the optimizer was
     picking a full scan and a hint/rewrite fixes everything.

Each probe is capped by a short query timeout so nothing hangs 10 minutes.
Run it like the export (SQL_USERNAME / SQL_PASSWORD env vars).
"""
from __future__ import annotations

import os
import sys
import time
import datetime as dt

TABLE = "variant_metrics"
PK = "variant_metric_id"
BRAND = "AMO"
WINDOW = 40000          # PK-range width for the bounded probes
BRAND_INDEX = "ix_vm_brand_dt"   # (brand, captured_datetime) - forced-index test
QUERY_TIMEOUT = 120     # short: a "fast" probe that hits this is itself a finding

SQL_SERVER   = os.environ.get("SQL_SERVER",   "denim-sql.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "denim_analytics")
SQL_USERNAME = os.environ.get("SQL_USERNAME", "")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")
VC = "CAST(%s AS varchar(255))"


def log(msg: str = "") -> None:
    print(f"[{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def section(title: str) -> None:
    log(""); log("=" * 60); log(title); log("=" * 60)


def run(cur, label, sql, params=None, stream=False):
    """Execute; if stream, drain all rows. Returns (rowcount_or_val, seconds, exc)."""
    t0 = time.time()
    try:
        cur.execute(sql, params or ())
        val = None
        if stream:
            n = 0
            while True:
                chunk = cur.fetchmany(2000)
                if not chunk:
                    break
                n += len(chunk)
            val = n
        elif cur.description:
            row = cur.fetchone()
            val = row[0] if row else None
        s = time.time() - t0
        log(f"{label}: OK in {s:.1f}s" + (f"  ({val} rows/val)" if val is not None else ""))
        return val, s, None
    except Exception as exc:
        s = time.time() - t0
        log(f"{label}: FAILED after {s:.1f}s -> {type(exc).__name__}: {exc}")
        return None, s, exc


def rate(n, s):
    return f"{(n/s):,.0f} rows/s" if (n and s) else "n/a"


def main():
    if not SQL_USERNAME or not SQL_PASSWORD:
        sys.exit("ERROR: set SQL_USERNAME and SQL_PASSWORD env vars first.")
    import pymssql

    section("1) CONNECT + table size (metadata, instant)")
    conn = pymssql.connect(server=SQL_SERVER, user=SQL_USERNAME, password=SQL_PASSWORD,
                           database=SQL_DATABASE, timeout=QUERY_TIMEOUT, login_timeout=60)
    cur = conn.cursor()
    log(f"Connected (query timeout {QUERY_TIMEOUT}s).")
    total_tbl, _, _ = run(cur, "table row count (all brands, DMV)",
        "SELECT SUM(ps.row_count) FROM sys.dm_db_partition_stats ps "
        "WHERE ps.object_id = OBJECT_ID('dbo.%s') AND ps.index_id IN (0,1)" % TABLE)
    if total_tbl is not None:
        log(f"   -> {TABLE} holds {total_tbl:,} rows across ALL brands "
            f"(AMO was ~417,859 of them).")

    section("2) PK bounds (instant, clustered)")
    lo, _, _ = run(cur, "MIN pk", f"SELECT MIN([{PK}]) FROM [dbo].[{TABLE}]")
    hi, _, _ = run(cur, "MAX pk", f"SELECT MAX([{PK}]) FROM [dbo].[{TABLE}]")
    if lo is None or hi is None:
        conn.close(); sys.exit("could not read PK bounds")
    a, b = hi - WINDOW, hi
    log(f"   pk range {lo}..{hi}; probing window {a}..{b}")

    section(f"3) BOUNDED window probes (pk {a}..{b}) - scan vs stream")
    rng = f"[{PK}] BETWEEN {a} AND {b}"
    cnt, cnt_s, _ = run(cur, "3a COUNT window (server scan)",
        f"SELECT COUNT(*) FROM [dbo].[{TABLE}] WHERE {rng}")
    if cnt:
        log(f"   -> scan rate ~ {rate(cnt, cnt_s)}")
    wide, wide_s, _ = run(cur, "3b SELECT * window (WIDE stream)",
        f"SELECT * FROM [dbo].[{TABLE}] WHERE {rng}", stream=True)
    if wide:
        log(f"   -> wide-row stream rate ~ {rate(wide, wide_s)}")
    narrow, narrow_s, _ = run(cur, "3c SELECT one col window (NARROW stream)",
        f"SELECT [{PK}] FROM [dbo].[{TABLE}] WHERE {rng}", stream=True)
    if narrow:
        log(f"   -> narrow stream rate ~ {rate(narrow, narrow_s)}")
    amo, amo_s, _ = run(cur, "3d COUNT window WHERE brand=AMO",
        f"SELECT COUNT(*) FROM [dbo].[{TABLE}] WHERE {rng} AND [brand]={VC}", (BRAND,))
    if cnt:
        log(f"   -> {amo}/{cnt} rows in this window are AMO")

    section("4) FORCE the brand index for a full AMO count")
    log(f"   (if this is fast, the optimizer was choosing a full scan and a hint")
    log(f"    or index rebuild fixes the export; may still take a while - capped)")
    run(cur, f"4 COUNT AMO WITH(INDEX({BRAND_INDEX}))",
        f"SELECT COUNT(*) FROM [dbo].[{TABLE}] WITH (INDEX({BRAND_INDEX})) "
        f"WHERE [brand]={VC}", (BRAND,))

    conn.close()
    section("READING GUIDE")
    log("* 3b (wide) MUCH slower than 3c (narrow) and 3a -> STREAM-bound: the wall")
    log("  is shipping wide rows. Fix = dedupe server-side to a tiny result, or")
    log("  select only needed columns; a full 417k-row pull is not viable.")
    log("* 3a/3c also slow (few thousand rows/s) -> throttled tier / scan-bound.")
    log("  Fix = keyset-paginate by pk in bounded pages and dedupe in Python.")
    log("* Step 4 fast while plain COUNT was 213s -> bad plan; an index hint fixes")
    log("  the export dedupe directly.")
    section("DONE - paste this whole output back.")


if __name__ == "__main__":
    main()
