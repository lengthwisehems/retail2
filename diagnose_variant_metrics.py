# -*- coding: utf-8 -*-
"""
Diagnose WHY deletes on variant_metrics time out (error 20003/20047).

This is READ-MOSTLY and NON-DESTRUCTIVE: the only write it does is a single
DELETE TOP (100) wrapped in a transaction that is ALWAYS rolled back, purely to
time the delete path and reveal blocking. Nothing is permanently changed.

It answers, in order:
  1. Does auth/connect work?  (it always has - included for completeness)
  2. What TYPE is sku_shopify / brand / captured_datetime?  (type mismatch ->
     the index can't seek -> full 475k-row scan -> timeout)
  3. What indexes exist on variant_metrics, and is there a CLUSTERED one?
     (a heap makes big deletes pathologically slow)
  4. Is anything BLOCKING right now? (a zombie transaction from a Ctrl-C'd run
     still holding locks makes every new delete wait until the query timeout)
  5. How many variant_metrics rows does ONE target sku actually have, and how
     long does COUNTing them take? (fast COUNT + slow DELETE => locking/log;
     slow COUNT => scan/throttle)
  6. How long does a real DELETE TOP (100) for that sku take, then ROLLBACK.

Run it the same way you run the cleanup:
    set SQL_USERNAME=carrieboromisa
    set SQL_PASSWORD=your-password
    python "C:\\...\\diagnose_variant_metrics.py"

Query timeout is deliberately short (90s) and LOCK_TIMEOUT is set to 15s, so if
something is blocking you get a fast, clear answer instead of a 10-minute hang.
"""
from __future__ import annotations

import os
import sys
import time
import datetime as dt

BRAND = "AMO"
SQL_SERVER   = os.environ.get("SQL_SERVER",   "denim-sql.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "denim_analytics")
SQL_USERNAME = os.environ.get("SQL_USERNAME", "")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")

# Optionally test specific skus (the ones the cleanup tries to delete). Leave the
# list empty to let the script pick real AMO skus from the table itself.
TARGET_SKUS: list = []

QUERY_TIMEOUT = 90     # seconds - short on purpose so a hang fails fast
LOCK_TIMEOUT_MS = 15000


def log(msg: str = "") -> None:
    stamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {msg}")


def section(title: str) -> None:
    log("")
    log("=" * 60)
    log(title)
    log("=" * 60)


def timed(cur, label, sql, params=None):
    """Run one statement, print how long it took (or how it failed)."""
    t0 = time.time()
    try:
        cur.execute(sql, params or ())
        rows = cur.fetchall() if cur.description else None
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
    log(f"Connected in {time.time()-t0:.1f}s as {SQL_USERNAME} "
        f"(query timeout {QUERY_TIMEOUT}s).")
    # Surface blocking fast instead of waiting the whole query timeout.
    try:
        cur.execute(f"SET LOCK_TIMEOUT {LOCK_TIMEOUT_MS}")
        log(f"LOCK_TIMEOUT set to {LOCK_TIMEOUT_MS} ms "
            f"(a blocked statement will error 1222 within {LOCK_TIMEOUT_MS//1000}s).")
    except Exception as exc:
        log(f"(could not set LOCK_TIMEOUT: {exc})")

    # -- 2) column types ----------------------------------------------------
    section("2) COLUMN TYPES on variant_metrics")
    rows, _, _ = timed(cur, "columns",
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH AS len, "
        "NUMERIC_PRECISION AS prec, NUMERIC_SCALE AS scale, COLLATION_NAME "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_NAME='variant_metrics' AND TABLE_SCHEMA='dbo' "
        "AND COLUMN_NAME IN ('variant_metric_id','brand','sku_shopify',"
        "'captured_datetime','variant_title','handle')")
    for r in rows or []:
        log(f"   {r['COLUMN_NAME']:20} {r['DATA_TYPE']:12} "
            f"len={r['len']} prec={r['prec']} scale={r['scale']} "
            f"coll={r['COLLATION_NAME']}")
    log("   -> If sku_shopify is a NUMERIC/BIGINT type, passing string skus can")
    log("      force a scan. If it's varchar, equality should seek.")

    # -- 3) indexes ---------------------------------------------------------
    section("3) INDEXES on variant_metrics (is there a CLUSTERED one?)")
    rows, _, _ = timed(cur, "indexes", """
        SELECT i.name AS idx, i.type_desc AS kind, i.is_unique AS uniq,
               STUFF((SELECT ', ' + c.name
                      FROM sys.index_columns ic2
                      JOIN sys.columns c ON c.object_id=ic2.object_id AND c.column_id=ic2.column_id
                      WHERE ic2.object_id=i.object_id AND ic2.index_id=i.index_id
                        AND ic2.is_included_column=0
                      ORDER BY ic2.key_ordinal
                      FOR XML PATH('')),1,2,'') AS keycols
        FROM sys.indexes i
        JOIN sys.objects o ON o.object_id=i.object_id
        WHERE o.name='variant_metrics' AND i.type>0
        ORDER BY i.index_id""")
    has_clustered = False
    for r in rows or []:
        if r["kind"] == "CLUSTERED":
            has_clustered = True
        log(f"   {str(r['idx']):32} {r['kind']:16} uniq={r['uniq']}  [{r['keycols']}]")
    log(f"   -> CLUSTERED index present: {has_clustered}  "
        f"(False = HEAP, which makes big deletes very slow)")

    # -- 4) blocking / open transactions ------------------------------------
    section("4) BLOCKING and open transactions RIGHT NOW")
    rows, _, _ = timed(cur, "requests", """
        SELECT r.session_id, r.blocking_session_id, r.status, r.command,
               r.wait_type, r.wait_time, DB_NAME(r.database_id) AS db
        FROM sys.dm_exec_requests r
        WHERE r.session_id <> @@SPID AND (r.blocking_session_id <> 0
              OR r.database_id = DB_ID())""")
    if rows:
        for r in rows:
            log(f"   spid {r['session_id']} status={r['status']} cmd={r['command']} "
                f"blocked_by={r['blocking_session_id']} wait={r['wait_type']} "
                f"({r['wait_time']}ms) db={r['db']}")
    else:
        log("   No blocking/other active requests seen from this login's view.")
    rows, _, _ = timed(cur, "open_tran", """
        SELECT s.session_id, s.login_name, s.host_name, s.program_name,
               s.last_request_end_time, t.transaction_id
        FROM sys.dm_tran_session_transactions t
        JOIN sys.dm_exec_sessions s ON s.session_id=t.session_id
        WHERE s.session_id <> @@SPID""")
    if rows:
        for r in rows:
            log(f"   OPEN TRAN spid {r['session_id']} login={r['login_name']} "
                f"host={r['host_name']} prog={r['program_name']} "
                f"last_req_end={r['last_request_end_time']}")
        log("   -> An open transaction from a killed run can hold locks and block "
            "your delete. If one is yours, it must be killed/reaped.")
    else:
        log("   No other sessions hold an open transaction.")

    # -- 5) prove the nvarchar-vs-varchar cause -----------------------------
    # brand/sku_shopify are varchar. pymssql sends str params as NVARCHAR, which
    # makes SQL Server convert the COLUMN (non-SARGable) and scan. Casting the
    # param to varchar restores the index seek. Get a real target sku with the
    # CAST form (fast), then compare a CAST count to a non-cast count.
    section("5) CONFIRM: cast (varchar) vs non-cast (nvarchar) seek")
    VC = "CAST(%s AS varchar(255))"
    skus = [str(x) for x in TARGET_SKUS if str(x).strip()]
    if not skus:
        rows, _, _ = timed(cur, "one sku (cast)",
            f"SELECT TOP 1 sku_shopify FROM variant_metrics WHERE brand={VC}", (BRAND,))
        skus = [str(r["sku_shopify"]) for r in (rows or [])]
    target = skus[0] if skus else None
    if target:
        log(f"target sku = {target}")
        rows, cast_s, _ = timed(cur, "COUNT (CAST varchar)",
            f"SELECT COUNT(*) AS n FROM variant_metrics "
            f"WHERE brand={VC} AND sku_shopify={VC}", (BRAND, target))
        if rows:
            log(f"   -> {rows[0]['n']} rows, {cast_s:.1f}s  (should be ~instant)")
        # The non-cast form is what was timing out; bounded by the 90s timeout.
        _, raw_s, raw_exc = timed(cur, "COUNT (raw nvarchar)",
            "SELECT COUNT(*) AS n FROM variant_metrics "
            "WHERE brand=%s AND sku_shopify=%s", (BRAND, target))
        if raw_exc is not None:
            log(f"   -> raw nvarchar form FAILED/timed out ({raw_s:.1f}s) = the bug")
        else:
            log(f"   -> raw nvarchar form took {raw_s:.1f}s")

    # -- 6) time a real DELETE (CAST), then ROLLBACK (non-destructive) ------
    section("6) TIMED DELETE TOP (100) with CAST, then ROLLBACK (nothing kept)")
    if target:
        _, del_s, exc = timed(cur, "DELETE TOP(100) (CAST)",
            f"DELETE TOP (100) FROM variant_metrics "
            f"WHERE brand={VC} AND sku_shopify={VC}", (BRAND, target))
        if exc is None:
            log(f"   DELETE affected {cur.rowcount} rows in {del_s:.1f}s")
        try:
            conn.rollback()
            log("   ROLLED BACK - no rows were actually removed.")
        except Exception as e:
            log(f"   (rollback issue: {e})")
    conn.close()
    section("DONE - paste this whole output back.")


if __name__ == "__main__":
    main()
