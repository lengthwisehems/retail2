# -*- coding: utf-8 -*-
"""
Diagnose WHY export_db_values.py times out pulling variant_metrics (the SQL-side
dedupe). READ-ONLY: it only SELECTs / COUNTs, never writes.

The deduped fetch is:
    WITH ranked AS (
      SELECT <all cols>, ROW_NUMBER() OVER (
        PARTITION BY <6 key cols> ORDER BY captured_datetime, pk) AS _rn
      FROM variant_metrics WHERE brand = <cast>)
    SELECT <all cols> FROM ranked WHERE _rn = 1

Now that the brand filter is cast to varchar (so it seeks), the remaining
suspect is the ROW_NUMBER SORT over ~475k full-width rows in tempdb. This script
separates the costs so we know the fix:

  A) how fast can it even COUNT AMO rows           (baseline seek)
  B) how fast is a plain streaming pull of N rows  (the pre-dedupe path)
  C) how long the NARROW survivor-pk ROW_NUMBER    (sort of pk+keys only)
  D) how many rows survive the dedupe              (how small the result is)

Decision tree from the output:
  * B fast, C fast   -> switch dedupe to two-step: compute survivor pks (narrow
                        sort), then fetch those rows by pk. Cheap.
  * B fast, C slow   -> the sort itself is too heavy for this tier -> dedupe in
                        Python via keyset-paginated streaming (no server sort).
  * B slow           -> even the raw pull is throttled; page it and be patient.

Run it like the export:
    set SQL_USERNAME=carrieboromisa
    set SQL_PASSWORD=your-password
    python "C:\\...\\diagnose_export.py"
"""
from __future__ import annotations

import os
import sys
import time
import datetime as dt

BRAND = "AMO"
TABLE = "variant_metrics"
KEY_COLS = ["brand", "sku_shopify", "sku_brand", "barcode", "variant_title", "size"]
DATE_COL = "captured_datetime"
PK = "variant_metric_id"

PULL_SAMPLE = 20000    # rows for the streaming-pull speed test
QUERY_TIMEOUT = 300    # short-ish so a heavy step fails without a 10-min hang
LOCK_TIMEOUT_MS = 15000

SQL_SERVER   = os.environ.get("SQL_SERVER",   "denim-sql.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "denim_analytics")
SQL_USERNAME = os.environ.get("SQL_USERNAME", "")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")
VC = "CAST(%s AS varchar(255))"


def log(msg: str = "") -> None:
    print(f"[{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def section(title: str) -> None:
    log("")
    log("=" * 60)
    log(title)
    log("=" * 60)


def timed(cur, label, sql, params=None, fetch=True):
    t0 = time.time()
    try:
        cur.execute(sql, params or ())
        rows = cur.fetchall() if (fetch and cur.description) else None
        dt_s = time.time() - t0
        log(f"{label}: OK in {dt_s:.1f}s")
        return rows, dt_s, None
    except Exception as exc:
        dt_s = time.time() - t0
        log(f"{label}: FAILED after {dt_s:.1f}s -> {type(exc).__name__}: {exc}")
        return None, dt_s, exc


def main():
    if not SQL_USERNAME or not SQL_PASSWORD:
        sys.exit("ERROR: set SQL_USERNAME and SQL_PASSWORD env vars first.")
    import pymssql

    section("1) CONNECT")
    t0 = time.time()
    conn = pymssql.connect(server=SQL_SERVER, user=SQL_USERNAME, password=SQL_PASSWORD,
                           database=SQL_DATABASE, timeout=QUERY_TIMEOUT, login_timeout=60)
    cur = conn.cursor(as_dict=True)
    log(f"Connected in {time.time()-t0:.1f}s (query timeout {QUERY_TIMEOUT}s).")
    try:
        cur.execute(f"SET LOCK_TIMEOUT {LOCK_TIMEOUT_MS}")
    except Exception as exc:
        log(f"(could not set LOCK_TIMEOUT: {exc})")

    partition = ", ".join(f"[{c}]" for c in KEY_COLS)

    # -- A) baseline count --------------------------------------------------
    section("A) COUNT of AMO rows (baseline brand seek)")
    rows, cnt_s, _ = timed(cur, f"COUNT {TABLE}",
        f"SELECT COUNT(*) AS n FROM [dbo].[{TABLE}] WHERE [brand] = {VC}", (BRAND,))
    total = rows[0]["n"] if rows else None
    if total is not None:
        log(f"   -> {total} {BRAND} rows in {TABLE}; COUNT took {cnt_s:.1f}s")

    # -- B) plain streaming pull speed --------------------------------------
    section(f"B) PLAIN streaming pull of up to {PULL_SAMPLE} rows (pre-dedupe path)")
    t0 = time.time()
    try:
        cur.execute(f"SELECT TOP ({PULL_SAMPLE}) * FROM [dbo].[{TABLE}] "
                    f"WHERE [brand] = {VC} ORDER BY [{PK}]", (BRAND,))
        got = 0
        while True:
            chunk = cur.fetchmany(2000)
            if not chunk:
                break
            got += len(chunk)
        pull_s = time.time() - t0
        rate = got / pull_s if pull_s else 0
        log(f"   pulled {got} rows in {pull_s:.1f}s ({rate:,.0f} rows/s)")
        if total and rate:
            log(f"   -> full {total} rows would stream in ~{total/rate:,.0f}s")
    except Exception as exc:
        log(f"   PLAIN pull FAILED after {time.time()-t0:.1f}s -> {exc}")

    # -- C) narrow survivor-pk ROW_NUMBER (the sort, but pk+keys only) ------
    section("C) NARROW survivor-pk dedupe (sort of pk + key cols only)")
    rows, narrow_s, narrow_exc = timed(cur, "narrow ROW_NUMBER survivors",
        f"SELECT COUNT(*) AS n FROM ("
        f"  SELECT [{PK}] AS pk, ROW_NUMBER() OVER ("
        f"    PARTITION BY {partition} "
        f"    ORDER BY [{DATE_COL}] ASC, [{PK}] ASC) AS _rn "
        f"  FROM [dbo].[{TABLE}] WHERE [brand] = {VC}) t WHERE t._rn = 1", (BRAND,))
    survivors = rows[0]["n"] if rows else None
    if survivors is not None:
        log(f"   -> {survivors} survivors; narrow dedupe sort took {narrow_s:.1f}s")
        if total:
            log(f"   -> dedupe would shrink {total} rows to {survivors}")

    # -- D) distinct key groups (sanity vs survivors) -----------------------
    section("D) DISTINCT dedupe-key groups (should equal survivors)")
    timed(cur, "distinct groups",
        f"SELECT COUNT(*) AS n FROM (SELECT {partition} FROM [dbo].[{TABLE}] "
        f"WHERE [brand] = {VC} GROUP BY {partition}) g", (BRAND,))

    conn.close()
    section("VERDICT HINTS")
    log("If B streamed fast but C was slow/failed -> the ROW_NUMBER SORT is the")
    log("   bottleneck; fix = dedupe in Python via keyset pagination (no sort).")
    log("If B and C were both OK -> fix = two-step SQL: get survivor pks (C),")
    log("   then fetch those rows by pk.")
    log("If B itself was slow -> the tier is throttling raw reads; page + wait.")
    section("DONE - paste this whole output back.")


if __name__ == "__main__":
    main()
