#!/usr/bin/env python3
"""
Fill blank style_id / style_name / style_name_grouping / sku_shopify / barcode
for brand = GOODAMERICAN, without losing variant_metrics history.

BACKGROUND
----------
Early GOODAMERICAN scrapes did not capture style_id, style_name,
style_name_grouping, sku_shopify, or barcode. Those variants were still
written to dbo.lookup (keyed by sku_brand, which was always captured), so a
lot of rows in dbo.lookup for GOODAMERICAN have those five fields NULL.
A later, fuller scrape re-captured the same variants -- this time with every
field populated -- as brand-new dbo.lookup rows (new lookup_id, same
sku_brand/variant).

So for many of the "blank" legacy rows, a fully-populated twin row for the
exact same variant already exists elsewhere in dbo.lookup. If we simply wrote
the correct values into the legacy row, it would become a byte-for-byte
duplicate of its twin -- identical brand/style_id/product_name/handle/
sku_url/color/style_name/style_name_grouping/variant_title/size/sku_shopify/
sku_brand/barcode, differing only by lookup_id. That collides with the
uq_lookup_sku unique index on (brand, sku_shopify), so the raw UPDATE would
either fail outright or (if run row by row) silently leave two rows aliasing
one SKU.

This script resolves that per row, decided live against the database:

  * If a twin with the same (brand, sku_shopify) already exists elsewhere in
    dbo.lookup, the legacy row is redundant -- it is deleted. Nothing is lost:
    dbo.variant_metrics/style_metrics/style_info are captured-history tables
    with no foreign key to lookup_id, so they are untouched either way.
  * Otherwise the row is genuinely unique history -- its blank fields are
    filled in from the supplied data (existing non-NULL values are never
    overwritten).

Every row touched (deleted or updated) is copied to a backup table first,
following this database's existing backup_<brand>_... convention (see
backup_rudes_lookup_5duplicates / backup_rudes_lookup_5products for
precedent).

As a second pass, once dbo.lookup has no more blank sku_shopify/barcode for
GOODAMERICAN, the same sku_brand match is used to backfill blank sku_shopify/
barcode in dbo.variant_metrics (safe: variant_metrics' uniqueness is keyed on
variant_title + captured_datetime, not sku_shopify, so this cannot create a
duplicate there).

A third, best-effort pass backfills blank style_id/style_name/
style_name_grouping in dbo.style_info and dbo.style_metrics by matching on
(brand, handle) against dbo.lookup, but only where every matching lookup row
agrees on a single value -- ambiguous handles are left alone and reported for
manual review, never guessed.

INPUT
-----
A CSV (or the original .xlsx) with one row per dbo.lookup.lookup_id that
currently has a blank field, and the value to fill each blank field with:

    lookup_id, sku_brand, fill_style_id, fill_style_name,
    fill_style_name_grouping, fill_sku_shopify, fill_barcode

Default: db_fixes/data/goodamerican_lookup_fill_values.csv (checked into this
repo, derived from the "GA skus that need numbers" worksheet).

USAGE
-----
    export SQL_SERVER=denim-sql.database.windows.net
    export SQL_DATABASE=denim_analytics
    export SQL_USERNAME=...
    export SQL_PASSWORD=...

    # 1. Always preview first -- makes no changes, just reports what would happen.
    python3 fill_goodamerican_blank_lookup_fields.py --dry-run

    # 2. Apply for real, inside one transaction. Rolls back automatically on
    #    any error.
    python3 fill_goodamerican_blank_lookup_fields.py --execute

Requires `pymssql` (`pip install pymssql`) -- no local ODBC driver needed.

Credentials are read only from the SQL_SERVER / SQL_DATABASE / SQL_USERNAME /
SQL_PASSWORD environment variables; they are intentionally never hardcoded
here or written to any file this script produces.
"""
import argparse
import csv
import os
import sys
from datetime import datetime

BRAND = "GOODAMERICAN"

BACKUP_TABLE = "backup_goodamerican_lookup_fillblanks"

CREATE_BACKUP_TABLE_SQL = f"""
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '{BACKUP_TABLE}')
BEGIN
    CREATE TABLE dbo.{BACKUP_TABLE} (
        backup_row_id       INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        lookup_id            INT           NOT NULL,
        brand                VARCHAR(255)  NOT NULL,
        style_id              VARCHAR(100)  NULL,
        product_name          NVARCHAR(255) NULL,
        handle                VARCHAR(255)  NULL,
        sku_url               NVARCHAR(255) NULL,
        color                 NVARCHAR(100) NULL,
        style_name            VARCHAR(255)  NULL,
        style_name_grouping   VARCHAR(100)  NULL,
        variant_title         NVARCHAR(255) NULL,
        size                  VARCHAR(255)  NULL,
        sku_shopify           VARCHAR(100)  NULL,
        sku_brand             VARCHAR(255)  NULL,
        barcode               VARCHAR(100)  NULL,
        backup_action         VARCHAR(20)   NOT NULL,  -- 'removed_duplicate' | 'duplicate_twin_kept' | 'filled_blanks'
        matched_lookup_id     INT           NULL,       -- for removed_duplicate/duplicate_twin_kept: the other side of the pair
        backup_at             DATETIME2(7)  NOT NULL
    );
END
"""

CREATE_STAGING_TABLE_SQL = """
IF OBJECT_ID('tempdb..#ga_fill_staging') IS NOT NULL DROP TABLE #ga_fill_staging;
CREATE TABLE #ga_fill_staging (
    lookup_id                  INT NOT NULL PRIMARY KEY,
    sku_brand                  VARCHAR(255) NULL,
    fill_style_id               VARCHAR(100) NOT NULL,
    fill_style_name             VARCHAR(255) NOT NULL,
    fill_style_name_grouping    VARCHAR(100) NOT NULL,
    fill_sku_shopify             VARCHAR(100) NOT NULL,
    fill_barcode                 VARCHAR(100) NOT NULL
);
"""

INSERT_STAGING_SQL = """
INSERT INTO #ga_fill_staging
    (lookup_id, sku_brand, fill_style_id, fill_style_name, fill_style_name_grouping, fill_sku_shopify, fill_barcode)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# NOTE: BRAND is interpolated as a literal (not passed as a bound parameter)
# in every statement that CREATEs a temp table. pyodbc/ODBC parameterized
# execs run dynamic SQL via sp_executesql, whose nested scope silently drops
# any #temp table created inside it the moment the call returns -- the next
# statement then fails with "Invalid object name '#...'" We sidestep that
# whole class of bug by never parameterizing a CREATE ... INTO #temp
# statement. BRAND is a hardcoded constant, not user input, so this is safe.

CREATE_DUPES_TABLE_SQL = f"""
IF OBJECT_ID('tempdb..#ga_dupes') IS NOT NULL DROP TABLE #ga_dupes;
SELECT s.lookup_id AS legacy_id, twin.lookup_id AS twin_id
INTO #ga_dupes
FROM #ga_fill_staging s
JOIN dbo.lookup twin
  ON twin.brand = '{BRAND}'
 AND twin.sku_shopify = s.fill_sku_shopify
 AND twin.lookup_id <> s.lookup_id;
"""

PREVIEW_COUNTS_SQL = """
SELECT
    (SELECT COUNT(*) FROM #ga_fill_staging) AS staged_rows,
    (SELECT COUNT(*) FROM #ga_dupes) AS duplicate_rows_to_remove,
    (SELECT COUNT(*) FROM #ga_fill_staging s WHERE NOT EXISTS (SELECT 1 FROM #ga_dupes d WHERE d.legacy_id = s.lookup_id)) AS rows_to_fill,
    (SELECT COUNT(*) FROM #ga_fill_staging s LEFT JOIN dbo.lookup l ON l.lookup_id = s.lookup_id WHERE l.lookup_id IS NULL) AS lookup_ids_not_found
"""

PREVIEW_MANUAL_OVERRIDE_HITS_SQL = """
SELECT COUNT(*) FROM dbo.manual_overrides
WHERE table_name = 'lookup' AND record_id IN (SELECT legacy_id FROM #ga_dupes)
"""

BACKUP_REMOVED_SQL = f"""
INSERT INTO dbo.{BACKUP_TABLE}
    (lookup_id, brand, style_id, product_name, handle, sku_url, color, style_name,
     style_name_grouping, variant_title, size, sku_shopify, sku_brand, barcode,
     backup_action, matched_lookup_id, backup_at)
SELECT l.lookup_id, l.brand, l.style_id, l.product_name, l.handle, l.sku_url, l.color, l.style_name,
       l.style_name_grouping, l.variant_title, l.size, l.sku_shopify, l.sku_brand, l.barcode,
       'removed_duplicate', d.twin_id, SYSDATETIME()
FROM dbo.lookup l
JOIN #ga_dupes d ON d.legacy_id = l.lookup_id;
"""

BACKUP_TWIN_SQL = f"""
INSERT INTO dbo.{BACKUP_TABLE}
    (lookup_id, brand, style_id, product_name, handle, sku_url, color, style_name,
     style_name_grouping, variant_title, size, sku_shopify, sku_brand, barcode,
     backup_action, matched_lookup_id, backup_at)
SELECT DISTINCT l.lookup_id, l.brand, l.style_id, l.product_name, l.handle, l.sku_url, l.color, l.style_name,
       l.style_name_grouping, l.variant_title, l.size, l.sku_shopify, l.sku_brand, l.barcode,
       'duplicate_twin_kept', d.legacy_id, SYSDATETIME()
FROM dbo.lookup l
JOIN #ga_dupes d ON d.twin_id = l.lookup_id;
"""

BACKUP_FILLED_SQL = f"""
INSERT INTO dbo.{BACKUP_TABLE}
    (lookup_id, brand, style_id, product_name, handle, sku_url, color, style_name,
     style_name_grouping, variant_title, size, sku_shopify, sku_brand, barcode,
     backup_action, matched_lookup_id, backup_at)
SELECT l.lookup_id, l.brand, l.style_id, l.product_name, l.handle, l.sku_url, l.color, l.style_name,
       l.style_name_grouping, l.variant_title, l.size, l.sku_shopify, l.sku_brand, l.barcode,
       'filled_blanks', NULL, SYSDATETIME()
FROM dbo.lookup l
JOIN #ga_fill_staging s ON s.lookup_id = l.lookup_id
WHERE NOT EXISTS (SELECT 1 FROM #ga_dupes d WHERE d.legacy_id = l.lookup_id);
"""

DELETE_DUPES_SQL = """
DELETE l
FROM dbo.lookup l
JOIN #ga_dupes d ON d.legacy_id = l.lookup_id;
"""

UPDATE_FILL_SQL = """
UPDATE l
SET style_id = COALESCE(l.style_id, s.fill_style_id),
    style_name = COALESCE(l.style_name, s.fill_style_name),
    style_name_grouping = COALESCE(l.style_name_grouping, s.fill_style_name_grouping),
    sku_shopify = COALESCE(l.sku_shopify, s.fill_sku_shopify),
    barcode = COALESCE(l.barcode, s.fill_barcode)
FROM dbo.lookup l
JOIN #ga_fill_staging s ON s.lookup_id = l.lookup_id
WHERE NOT EXISTS (SELECT 1 FROM #ga_dupes d WHERE d.legacy_id = l.lookup_id);
"""

# --- Step 2: variant_metrics backfill (safe -- uniqueness there is keyed on
# variant_title + captured_datetime, never on sku_shopify/barcode) ---------

CHECK_SKUBRAND_UNIQUE_SQL = f"""
SELECT sku_brand, COUNT(*) AS n
FROM dbo.lookup
WHERE brand = '{BRAND}' AND sku_brand IS NOT NULL
GROUP BY sku_brand
HAVING COUNT(*) > 1
"""

PREVIEW_VARIANT_METRICS_SQL = f"""
SELECT COUNT(*)
FROM dbo.variant_metrics vm
JOIN dbo.lookup l ON l.brand = vm.brand AND l.sku_brand = vm.sku_brand
WHERE vm.brand = '{BRAND}'
  AND (vm.sku_shopify IS NULL OR vm.barcode IS NULL)
  AND vm.sku_brand IS NOT NULL
  AND (l.sku_shopify IS NOT NULL OR l.barcode IS NOT NULL)
"""

UPDATE_VARIANT_METRICS_SQL = f"""
UPDATE vm
SET sku_shopify = COALESCE(vm.sku_shopify, l.sku_shopify),
    barcode = COALESCE(vm.barcode, l.barcode)
FROM dbo.variant_metrics vm
JOIN dbo.lookup l ON l.brand = vm.brand AND l.sku_brand = vm.sku_brand
WHERE vm.brand = '{BRAND}'
  AND (vm.sku_shopify IS NULL OR vm.barcode IS NULL)
  AND vm.sku_brand IS NOT NULL;
"""

# --- Step 3: style_info / style_metrics backfill (best effort, only where
# every dbo.lookup row for that handle agrees on a single value) -----------

BUILD_HANDLE_AGG_SQL = f"""
IF OBJECT_ID('tempdb..#ga_handle_agg') IS NOT NULL DROP TABLE #ga_handle_agg;
SELECT handle,
       COUNT(DISTINCT style_id) AS n_style_id, MAX(style_id) AS one_style_id,
       COUNT(DISTINCT style_name) AS n_style_name, MAX(style_name) AS one_style_name,
       COUNT(DISTINCT style_name_grouping) AS n_style_name_grouping, MAX(style_name_grouping) AS one_style_name_grouping
INTO #ga_handle_agg
FROM dbo.lookup
WHERE brand = '{BRAND}' AND handle IS NOT NULL
GROUP BY handle;
"""

def preview_style_table_sql(table):
    return f"""
    SELECT
        SUM(CASE WHEN t.style_id IS NULL AND a.n_style_id = 1 THEN 1 ELSE 0 END) AS style_id_fillable,
        SUM(CASE WHEN t.style_id IS NULL AND a.n_style_id > 1 THEN 1 ELSE 0 END) AS style_id_ambiguous,
        SUM(CASE WHEN t.style_name IS NULL AND a.n_style_name = 1 THEN 1 ELSE 0 END) AS style_name_fillable,
        SUM(CASE WHEN t.style_name_grouping IS NULL AND a.n_style_name_grouping = 1 THEN 1 ELSE 0 END) AS style_name_grouping_fillable
    FROM dbo.{table} t
    JOIN #ga_handle_agg a ON a.handle = t.handle
    WHERE t.brand = '{BRAND}'
      AND (t.style_id IS NULL OR t.style_name IS NULL OR t.style_name_grouping IS NULL)
    """

def update_style_table_sql(table):
    return f"""
    UPDATE t
    SET style_id = CASE WHEN t.style_id IS NULL AND a.n_style_id = 1 THEN a.one_style_id ELSE t.style_id END,
        style_name = CASE WHEN t.style_name IS NULL AND a.n_style_name = 1 THEN a.one_style_name ELSE t.style_name END,
        style_name_grouping = CASE WHEN t.style_name_grouping IS NULL AND a.n_style_name_grouping = 1 THEN a.one_style_name_grouping ELSE t.style_name_grouping END
    FROM dbo.{table} t
    JOIN #ga_handle_agg a ON a.handle = t.handle
    WHERE t.brand = '{BRAND}'
      AND (
            (t.style_id IS NULL AND a.n_style_id = 1)
         OR (t.style_name IS NULL AND a.n_style_name = 1)
         OR (t.style_name_grouping IS NULL AND a.n_style_name_grouping = 1)
      );
    """

AMBIGUOUS_HANDLES_SQL = """
SELECT handle, n_style_id, n_style_name, n_style_name_grouping
FROM #ga_handle_agg
WHERE n_style_id > 1 OR n_style_name > 1 OR n_style_name_grouping > 1
ORDER BY handle;
"""


def load_rows(path):
    rows = []
    if path.lower().endswith(".xlsx"):
        import pandas as pd
        df = pd.read_excel(path, sheet_name=0, dtype=str)
        df.columns = [c.strip() for c in df.columns]
        rename = {
            "If style_id is blank, fill with": "fill_style_id",
            "If style_name is blank, fill with": "fill_style_name",
            "If style_name_grouping is blank, fill with": "fill_style_name_grouping",
            "If sku_shopify is blank, fill with": "fill_sku_shopify",
            "If barcode is blank, fill with": "fill_barcode",
        }
        df = df.rename(columns=rename)
        needed = ["lookup_id", "sku_brand", "fill_style_id", "fill_style_name",
                  "fill_style_name_grouping", "fill_sku_shopify", "fill_barcode"]
        for r in df[needed].itertuples(index=False):
            rows.append(dict(zip(needed, r)))
    else:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rows.append(row)

    for r in rows:
        r["lookup_id"] = int(r["lookup_id"])
        for k in ("fill_style_id", "fill_style_name", "fill_style_name_grouping", "fill_sku_shopify", "fill_barcode"):
            if r.get(k) in (None, "", "nan"):
                raise ValueError(f"lookup_id {r['lookup_id']}: missing required fill value for {k}")

    seen = set()
    for r in rows:
        if r["lookup_id"] in seen:
            raise ValueError(f"duplicate lookup_id {r['lookup_id']} in input file")
        seen.add(r["lookup_id"])

    return rows


def connect():
    import pymssql
    server = os.environ.get("SQL_SERVER")
    database = os.environ.get("SQL_DATABASE")
    user = os.environ.get("SQL_USERNAME")
    password = os.environ.get("SQL_PASSWORD")
    missing = [n for n, v in [("SQL_SERVER", server), ("SQL_DATABASE", database),
                               ("SQL_USERNAME", user), ("SQL_PASSWORD", password)] if not v]
    if missing:
        sys.exit(f"Missing required environment variable(s): {', '.join(missing)}")
    return pymssql.connect(server=server, database=database, user=user, password=password,
                            login_timeout=30, timeout=120, autocommit=False)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", default=os.path.join(os.path.dirname(__file__), "data", "goodamerican_lookup_fill_values.csv"),
                     help="CSV or .xlsx with lookup_id + fill_* columns (default: bundled CSV)")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Report what would happen; makes no database changes.")
    mode.add_argument("--execute", action="store_true", help="Apply the changes for real, inside one transaction.")
    ap.add_argument("--skip-variant-metrics", action="store_true", help="Skip the variant_metrics backfill pass.")
    ap.add_argument("--skip-style-tables", action="store_true", help="Skip the style_info/style_metrics backfill pass.")
    args = ap.parse_args()

    print(f"Loading fill data from {args.input} ...")
    rows = load_rows(args.input)
    print(f"  {len(rows)} lookup_id rows loaded.")

    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(CREATE_BACKUP_TABLE_SQL)

        cur.execute(CREATE_STAGING_TABLE_SQL)
        cur.executemany(INSERT_STAGING_SQL, [
            (r["lookup_id"], r["sku_brand"], r["fill_style_id"], r["fill_style_name"],
             r["fill_style_name_grouping"], r["fill_sku_shopify"], r["fill_barcode"])
            for r in rows
        ])
        cur.execute(CREATE_DUPES_TABLE_SQL)

        cur.execute(PREVIEW_COUNTS_SQL)
        staged, dupe_count, fill_count, missing_count = cur.fetchone()
        print("\n=== dbo.lookup ===")
        print(f"  staged rows (from input file):        {staged}")
        print(f"  lookup_id not found in dbo.lookup:      {missing_count}  (already handled previously?)")
        print(f"  will be DELETED as duplicate of a twin: {dupe_count}")
        print(f"  will be UPDATED (blanks filled in):     {fill_count}")

        cur.execute(PREVIEW_MANUAL_OVERRIDE_HITS_SQL)
        (mo_count,) = cur.fetchone()
        if mo_count:
            print(f"  NOTE: {mo_count} row(s) in dbo.manual_overrides reference a lookup_id "
                  f"that will be deleted as a duplicate. Their table_name/record_id pointer "
                  f"will become stale (no FK enforces it either way) -- review dbo.manual_overrides "
                  f"WHERE table_name='lookup' after running this.")

        if args.dry_run:
            # These two passes depend on dbo.lookup already being fixed (Step 1),
            # so in a dry run they can only report today's (pre-fix) numbers as a
            # lower bound -- say so explicitly rather than imply they're final.
            print("\n--- estimates below are computed against dbo.lookup's CURRENT (pre-fix) state ---")
            print("--- actual --execute numbers for these two passes will be different (usually higher) ---")

            if not args.skip_variant_metrics:
                cur.execute(PREVIEW_VARIANT_METRICS_SQL)
                (vm_count,) = cur.fetchone()
                print(f"\n=== dbo.variant_metrics (estimate) ===")
                print(f"  rows with blank sku_shopify/barcode fillable from dbo.lookup via sku_brand: {vm_count}")

            if not args.skip_style_tables:
                cur.execute(BUILD_HANDLE_AGG_SQL)
                for table in ("style_info", "style_metrics"):
                    cur.execute(preview_style_table_sql(table))
                    sid_fill, sid_ambig, sname_fill, sgroup_fill = cur.fetchone()
                    print(f"\n=== dbo.{table} (estimate) ===")
                    print(f"  style_id fillable (unambiguous by handle):          {sid_fill or 0}")
                    print(f"  style_id ambiguous (handle has >1 distinct value):  {sid_ambig or 0}  (left alone)")
                    print(f"  style_name fillable:                                {sname_fill or 0}")
                    print(f"  style_name_grouping fillable:                       {sgroup_fill or 0}")
                cur.execute(AMBIGUOUS_HANDLES_SQL)
                ambiguous = cur.fetchall()
                if ambiguous:
                    print(f"\n  {len(ambiguous)} handle(s) have inconsistent style_id/style_name/style_name_grouping "
                          f"across dbo.lookup rows and will be left blank for manual review:")
                    for h in ambiguous[:20]:
                        print(f"    {h}")
                    if len(ambiguous) > 20:
                        print(f"    ... and {len(ambiguous) - 20} more")

            print("\nDry run only -- no changes made. Re-run with --execute to apply.")
            conn.rollback()
            return

        # --- apply ---
        print("\nApplying changes...")
        cur.execute(BACKUP_REMOVED_SQL)
        cur.execute(BACKUP_TWIN_SQL)
        cur.execute(BACKUP_FILLED_SQL)
        cur.execute(DELETE_DUPES_SQL)
        cur.execute(UPDATE_FILL_SQL)
        print(f"  dbo.lookup: removed {dupe_count} duplicate row(s), filled {fill_count} row(s). "
              f"Full before-state backed up to dbo.{BACKUP_TABLE}.")

        # From here on, dbo.lookup reflects the fix above (same transaction, same
        # session -- reads see our own uncommitted writes), so re-derive the
        # sku_brand/handle matches fresh instead of reusing pre-fix numbers.

        if not args.skip_variant_metrics:
            cur.execute(CHECK_SKUBRAND_UNIQUE_SQL)
            dupe_skubrand = cur.fetchall()
            if dupe_skubrand:
                print(f"\n  dbo.variant_metrics backfill SKIPPED: {len(dupe_skubrand)} sku_brand value(s) "
                      f"are still not unique in dbo.lookup for {BRAND} even after the fix above -- "
                      f"investigate before backfilling from an ambiguous join.")
            else:
                cur.execute(UPDATE_VARIANT_METRICS_SQL)
                print(f"  dbo.variant_metrics: backfilled sku_shopify/barcode for {cur.rowcount} row(s) via sku_brand match.")

        if not args.skip_style_tables:
            cur.execute(BUILD_HANDLE_AGG_SQL)
            for table in ("style_info", "style_metrics"):
                cur.execute(update_style_table_sql(table))
                print(f"  dbo.{table}: backfilled unambiguous style_id/style_name/style_name_grouping "
                      f"for {cur.rowcount} row(s) by matching handle.")

        conn.commit()
        print(f"\nDone. Committed at {datetime.now().isoformat()}.")

    except Exception:
        conn.rollback()
        print("\nERROR -- transaction rolled back, no changes were made.", file=sys.stderr)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
