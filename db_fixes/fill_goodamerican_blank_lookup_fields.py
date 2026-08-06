#!/usr/bin/env python3
r"""
Fill blank style_id / style_name / style_name_grouping / sku_shopify / barcode
for brand = GOODAMERICAN, everywhere those fields show up.

This script never deletes or merges any row, in any table. dbo.lookup is
allowed to (and does) carry more than one row for the same sku_brand over
time -- that is by design, so that data captured against an older row is
never orphaned. This script only ever fills a NULL field with a value; it
never overwrites a value that is already there, and it never removes a row.

STEP 1 -- dbo.lookup
    For each lookup_id in the input file, fill style_id, style_name,
    style_name_grouping, sku_shopify, and barcode wherever they are
    currently NULL, using the value supplied for that row (input file
    columns "If <field> is blank, fill with").

    The one real constraint: dbo.lookup has a unique index on
    (brand, sku_shopify) for non-NULL sku_shopify. If the sku_shopify value
    we're about to write already exists on a *different* lookup_id, SQL
    Server will not allow it on this row too. When that happens, this
    script still fills every other blank field on that row and simply
    leaves sku_shopify as it was (NULL), then prints exactly which
    lookup_id it conflicts with so you can look at it by hand. Nothing is
    deleted or guessed on your behalf.

STEP 2 -- dbo.style_info and dbo.style_metrics
    For each row still missing style_id, style_name, or style_name_grouping,
    fill it from dbo.lookup by matching brand + product_name. Where more
    than one dbo.lookup row shares that product_name (expected, given
    dbo.lookup intentionally keeps old and new rows), any one populated,
    non-NULL value found for that product_name is used -- MAX() ignores
    NULLs, so it naturally picks the first real value it finds.

STEP 3 -- dbo.variant_metrics
    For each row still missing sku_shopify or barcode, fill it from
    dbo.lookup by matching brand + sku_brand, the same way.

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
1. Set the SQL_SERVER / SQL_DATABASE / SQL_USERNAME / SQL_PASSWORD
   environment variables (Windows cmd.exe example):

       set SQL_SERVER=denim-sql.database.windows.net
       set SQL_DATABASE=denim_analytics
       set SQL_USERNAME=...
       set SQL_PASSWORD=...

2. Look at the DRY_RUN setting a few lines below (right after the imports).
   Leave it as DRY_RUN = True the first time -- that mode only reports what
   *would* happen and makes no database changes. Once you've reviewed the
   report, change it to DRY_RUN = False and run the script again to apply
   the changes for real, inside one transaction (it rolls back automatically
   on any error).

3. Run it with Python -- a .py file is not itself a program Windows can
   execute, it has to be handed to python.exe. In Command Prompt:

       python "C:\path\to\fill_goodamerican_blank_lookup_fields.py"

Requires `pymssql` (`pip install pymssql`) -- no local ODBC driver needed.

Credentials are read only from the SQL_SERVER / SQL_DATABASE / SQL_USERNAME /
SQL_PASSWORD environment variables. As an alternative, you can instead type
them directly into the SQL_SERVER / SQL_DATABASE / SQL_USERNAME /
SQL_PASSWORD constants a few lines below -- whichever of the two you fill
in, environment variables win if both are set.

IMPORTANT if you fill in the constants below: this file then contains your
database password in plain text. Keep it local to your own machine only --
never email it, upload it anywhere shared (a shared drive, Slack, a git
repo, etc.), or send it to anyone.
"""
import argparse
import csv
import os
import sys
from datetime import datetime

# ============================================================================
# SET THIS. True = preview only, no database changes. False = apply for real.
DRY_RUN = True

# OPTIONAL: fill these in so you don't have to run `set` commands first.
# Leave them blank ("") to keep using environment variables instead.
# See the IMPORTANT note above the imports before you put a real password here.
SQL_SERVER_INLINE = ""
SQL_DATABASE_INLINE = ""
SQL_USERNAME_INLINE = ""
SQL_PASSWORD_INLINE = ""
# ============================================================================

BRAND = "GOODAMERICAN"

BACKUP_TABLE = "backup_goodamerican_lookup_fillblanks"

# NOTE: BRAND is interpolated as a literal (not passed as a bound parameter)
# in every statement that CREATEs a temp table, and for simplicity in every
# other statement too, since it's a hardcoded constant, not user input.

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

# Rows whose target sku_shopify already exists on a DIFFERENT lookup_id.
# dbo.lookup's unique index on (brand, sku_shopify) would reject writing it
# there too, so these are left with sku_shopify untouched and reported.
PREVIEW_SKU_CONFLICTS_SQL = f"""
SELECT s.lookup_id, s.fill_sku_shopify, other.lookup_id AS conflicts_with_lookup_id
FROM #ga_fill_staging s
JOIN dbo.lookup other
  ON other.brand = '{BRAND}'
 AND other.sku_shopify = s.fill_sku_shopify
 AND other.lookup_id <> s.lookup_id
ORDER BY s.lookup_id;
"""

PREVIEW_COUNTS_SQL = """
SELECT
    (SELECT COUNT(*) FROM #ga_fill_staging) AS staged_rows,
    (SELECT COUNT(*) FROM #ga_fill_staging s LEFT JOIN dbo.lookup l ON l.lookup_id = s.lookup_id WHERE l.lookup_id IS NULL) AS lookup_ids_not_found
"""

BACKUP_LOOKUP_ROWS_SQL = f"""
INSERT INTO dbo.{BACKUP_TABLE}
    (lookup_id, brand, style_id, product_name, handle, sku_url, color, style_name,
     style_name_grouping, variant_title, size, sku_shopify, sku_brand, barcode, backup_at)
SELECT l.lookup_id, l.brand, l.style_id, l.product_name, l.handle, l.sku_url, l.color, l.style_name,
       l.style_name_grouping, l.variant_title, l.size, l.sku_shopify, l.sku_brand, l.barcode, SYSDATETIME()
FROM dbo.lookup l
JOIN #ga_fill_staging s ON s.lookup_id = l.lookup_id;
"""

# Fills every blank field. sku_shopify is only written when doing so would
# not collide with a different row's existing sku_shopify (see
# PREVIEW_SKU_CONFLICTS_SQL above) -- otherwise it's left exactly as it was.
UPDATE_LOOKUP_SQL = f"""
UPDATE l
SET style_id = COALESCE(l.style_id, s.fill_style_id),
    style_name = COALESCE(l.style_name, s.fill_style_name),
    style_name_grouping = COALESCE(l.style_name_grouping, s.fill_style_name_grouping),
    barcode = COALESCE(l.barcode, s.fill_barcode),
    sku_shopify = CASE
        WHEN l.sku_shopify IS NOT NULL THEN l.sku_shopify
        WHEN EXISTS (
            SELECT 1 FROM dbo.lookup other
            WHERE other.brand = l.brand AND other.sku_shopify = s.fill_sku_shopify AND other.lookup_id <> l.lookup_id
        ) THEN NULL
        ELSE s.fill_sku_shopify
    END
FROM dbo.lookup l
JOIN #ga_fill_staging s ON s.lookup_id = l.lookup_id
WHERE l.brand = '{BRAND}';
"""

# --- Step 2: style_info / style_metrics, matched on product_name ----------

def preview_style_table_sql(table):
    return f"""
    SELECT COUNT(*)
    FROM dbo.{table} t
    JOIN (
        SELECT product_name,
               MAX(style_id) AS style_id,
               MAX(style_name) AS style_name,
               MAX(style_name_grouping) AS style_name_grouping
        FROM dbo.lookup
        WHERE brand = '{BRAND}' AND product_name IS NOT NULL
        GROUP BY product_name
    ) agg ON agg.product_name = t.product_name
    WHERE t.brand = '{BRAND}'
      AND (t.style_id IS NULL OR t.style_name IS NULL OR t.style_name_grouping IS NULL)
      AND (agg.style_id IS NOT NULL OR agg.style_name IS NOT NULL OR agg.style_name_grouping IS NOT NULL)
    """

def update_style_table_sql(table):
    return f"""
    UPDATE t
    SET style_id = COALESCE(t.style_id, agg.style_id),
        style_name = COALESCE(t.style_name, agg.style_name),
        style_name_grouping = COALESCE(t.style_name_grouping, agg.style_name_grouping)
    FROM dbo.{table} t
    JOIN (
        SELECT product_name,
               MAX(style_id) AS style_id,
               MAX(style_name) AS style_name,
               MAX(style_name_grouping) AS style_name_grouping
        FROM dbo.lookup
        WHERE brand = '{BRAND}' AND product_name IS NOT NULL
        GROUP BY product_name
    ) agg ON agg.product_name = t.product_name
    WHERE t.brand = '{BRAND}';
    """

# --- Step 3: variant_metrics, matched on sku_brand -------------------------

PREVIEW_VARIANT_METRICS_SQL = f"""
SELECT COUNT(*)
FROM dbo.variant_metrics vm
JOIN (
    SELECT sku_brand, MAX(sku_shopify) AS sku_shopify, MAX(barcode) AS barcode
    FROM dbo.lookup
    WHERE brand = '{BRAND}' AND sku_brand IS NOT NULL
    GROUP BY sku_brand
) agg ON agg.sku_brand = vm.sku_brand
WHERE vm.brand = '{BRAND}'
  AND (vm.sku_shopify IS NULL OR vm.barcode IS NULL)
  AND (agg.sku_shopify IS NOT NULL OR agg.barcode IS NOT NULL)
"""

# sku_brand values where dbo.lookup has more than one distinct non-NULL
# sku_shopify -- MAX() will pick one deterministically, but this is worth a
# look since it would mean a real disagreement in the source data.
PREVIEW_VARIANT_METRICS_AMBIGUOUS_SQL = f"""
SELECT sku_brand, COUNT(DISTINCT sku_shopify) AS distinct_sku_shopify
FROM dbo.lookup
WHERE brand = '{BRAND}' AND sku_brand IS NOT NULL AND sku_shopify IS NOT NULL
GROUP BY sku_brand
HAVING COUNT(DISTINCT sku_shopify) > 1
"""

UPDATE_VARIANT_METRICS_SQL = f"""
UPDATE vm
SET sku_shopify = COALESCE(vm.sku_shopify, agg.sku_shopify),
    barcode = COALESCE(vm.barcode, agg.barcode)
FROM dbo.variant_metrics vm
JOIN (
    SELECT sku_brand, MAX(sku_shopify) AS sku_shopify, MAX(barcode) AS barcode
    FROM dbo.lookup
    WHERE brand = '{BRAND}' AND sku_brand IS NOT NULL
    GROUP BY sku_brand
) agg ON agg.sku_brand = vm.sku_brand
WHERE vm.brand = '{BRAND}'
  AND (vm.sku_shopify IS NULL OR vm.barcode IS NULL);
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
        # utf-8-sig (not plain utf-8) strips the invisible BOM marker Excel
        # writes at the start of a CSV it saves -- without this, that marker
        # glues onto the first header ("lookup_id" reads as "﻿lookup_id")
        # and every row fails with KeyError: 'lookup_id'.
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            required = ["lookup_id", "sku_brand", "fill_style_id", "fill_style_name",
                        "fill_style_name_grouping", "fill_sku_shopify", "fill_barcode"]
            missing_cols = [c for c in required if c not in (reader.fieldnames or [])]
            if missing_cols:
                raise ValueError(
                    f"{path} is missing column(s) {missing_cols}. "
                    f"Columns actually found: {reader.fieldnames}"
                )
            for row in reader:
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
    server = os.environ.get("SQL_SERVER") or SQL_SERVER_INLINE
    database = os.environ.get("SQL_DATABASE") or SQL_DATABASE_INLINE
    user = os.environ.get("SQL_USERNAME") or SQL_USERNAME_INLINE
    password = os.environ.get("SQL_PASSWORD") or SQL_PASSWORD_INLINE
    missing = [n for n, v in [("SQL_SERVER", server), ("SQL_DATABASE", database),
                               ("SQL_USERNAME", user), ("SQL_PASSWORD", password)] if not v]
    if missing:
        sys.exit(f"Missing {', '.join(missing)} -- either run `set` for these first, "
                  f"or fill in the SQL_*_INLINE constants near the top of this file.")
    return pymssql.connect(server=server, database=database, user=user, password=password,
                            login_timeout=30, timeout=120, autocommit=False)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input", default=os.path.join(os.path.dirname(__file__), "data", "goodamerican_lookup_fill_values.csv"),
                     help="CSV or .xlsx with lookup_id + fill_* columns (default: bundled CSV)")
    ap.add_argument("--skip-variant-metrics", action="store_true", help="Skip the variant_metrics backfill pass.")
    ap.add_argument("--skip-style-tables", action="store_true", help="Skip the style_info/style_metrics backfill pass.")
    args = ap.parse_args()
    args.dry_run = DRY_RUN
    args.execute = not DRY_RUN

    print("DRY RUN (preview only, no changes will be made)" if DRY_RUN else "EXECUTE (changes WILL be written to the database)")
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

        cur.execute(PREVIEW_COUNTS_SQL)
        staged, missing_count = cur.fetchone()
        print("\n=== dbo.lookup ===")
        print(f"  staged rows (from input file):     {staged}")
        print(f"  lookup_id not found in dbo.lookup:   {missing_count}  (already handled previously?)")
        print(f"  Nothing is ever deleted. Every blank field on every one of these rows gets filled,")
        print(f"  except see the sku_shopify conflicts below, if any.")

        cur.execute(PREVIEW_SKU_CONFLICTS_SQL)
        conflicts = cur.fetchall()
        if conflicts:
            print(f"\n  {len(conflicts)} row(s) have a target sku_shopify that already exists on a "
                  f"DIFFERENT lookup_id. dbo.lookup's unique index on (brand, sku_shopify) won't allow "
                  f"writing it there too, so sku_shopify is left as-is on these rows (every other blank "
                  f"field on them still gets filled). Nothing is deleted -- these just need a human look:")
            print(f"  (lookup_id, target_sku_shopify, conflicts_with_lookup_id)")
            for c in conflicts[:30]:
                print(f"    {c}")
            if len(conflicts) > 30:
                print(f"    ... and {len(conflicts) - 30} more")
        else:
            print(f"\n  No sku_shopify conflicts found -- every row can be filled completely.")

        if not args.skip_variant_metrics:
            cur.execute(PREVIEW_VARIANT_METRICS_AMBIGUOUS_SQL)
            ambiguous_vm = cur.fetchall()
            if ambiguous_vm:
                print(f"\n  NOTE: {len(ambiguous_vm)} sku_brand value(s) have more than one distinct "
                      f"sku_shopify across dbo.lookup rows -- worth a look (sku_brand, distinct_count):")
                for a in ambiguous_vm[:20]:
                    print(f"    {a}")

        if args.dry_run:
            # These two passes read dbo.lookup, which Step 1 (above) is what
            # fixes -- so in a dry run they can only report today's (pre-fix)
            # numbers as a lower bound.
            print("\n--- estimates below are computed against dbo.lookup's CURRENT (pre-fix) state ---")
            print("--- actual --execute numbers will be different (usually higher) ---")

            if not args.skip_variant_metrics:
                cur.execute(PREVIEW_VARIANT_METRICS_SQL)
                (vm_count,) = cur.fetchone()
                print(f"\n=== dbo.variant_metrics (estimate) ===")
                print(f"  rows with blank sku_shopify/barcode fillable via sku_brand match: {vm_count}")

            if not args.skip_style_tables:
                for table in ("style_info", "style_metrics"):
                    cur.execute(preview_style_table_sql(table))
                    (count,) = cur.fetchone()
                    print(f"\n=== dbo.{table} (estimate) ===")
                    print(f"  rows with blank style_id/style_name/style_name_grouping fillable via product_name match: {count}")

            print("\nDry run only -- no changes made. Re-run with --execute to apply.")
            conn.rollback()
            return

        # --- apply ---
        print("\nApplying changes...")
        cur.execute(BACKUP_LOOKUP_ROWS_SQL)
        cur.execute(UPDATE_LOOKUP_SQL)
        print(f"  dbo.lookup: filled blanks on {cur.rowcount} row(s) (sku_shopify held back on the "
              f"{len(conflicts)} conflict(s) reported above). Pre-update state backed up to dbo.{BACKUP_TABLE}.")

        # dbo.lookup now reflects the fix above (same transaction, same
        # session -- reads see our own uncommitted writes).

        if not args.skip_variant_metrics:
            cur.execute(UPDATE_VARIANT_METRICS_SQL)
            print(f"  dbo.variant_metrics: filled sku_shopify/barcode on {cur.rowcount} row(s) via sku_brand match.")

        if not args.skip_style_tables:
            for table in ("style_info", "style_metrics"):
                cur.execute(update_style_table_sql(table))
                print(f"  dbo.{table}: filled style_id/style_name/style_name_grouping on {cur.rowcount} row(s) via product_name match.")

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
    try:
        main()
    except Exception as e:
        print(f"\n{type(e).__name__}: {e}", file=sys.stderr)
    finally:
        # Keeps the window open when this is run by double-clicking in
        # Windows Explorer instead of from an already-open Command Prompt --
        # otherwise it flashes and closes before the output can be read.
        input("\nPress Enter to close this window...")
