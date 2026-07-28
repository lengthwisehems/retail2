# -*- coding: utf-8 -*-
"""
Backfill / repair TRIARCHY categorization in the denim_analytics database.

WHY
---
TRIARCHY rows imported before triarchy_inventory.py was improved have
categorization fields that are blank or no longer match the current rules.
This re-derives them with the *exact* scraper functions (imported directly from
triarchy_inventory.py, including the cross-row group passes) and fixes the
database in place.

WHAT IT CHANGES  (all scoped to brand = 'TRIARCHY')
---------------------------------------------------
Seven fields, written where the stored value is blank OR doesn't match the
rules (existing values ARE replaced):

  style_info only:
     jean_style, rise_label, inseam_style, color_simplified, color_standardized
  style_info + lookup + style_metrics (kept in sync on the product_name join):
     color, style_name

SAFETY
------
* DRY_RUN = True by default. Prints a full preview and writes NOTHING.
* Rows with is_manual_override = 1 are skipped (human-locked).
* Only NON-BLANK derived values overwrite; a value the rules can't produce
  never blanks an existing one.
* Every style_info field change is logged to manual_overrides (old -> new).
* is_manual_override is never set, so the daily scrape can still update rows.
* The whole apply runs in one transaction and rolls back on any error.

HOW TO RUN  (Windows, plain Command Prompt - the black "cmd" window)
--------------------------------------------------------------------
    pip install pymssql
    set SQL_USERNAME=carrieboromisa
    set SQL_PASSWORD=your-password
    python "C:\\path\\to\\backfill_triarchy_style_info.py"

Keep triarchy_inventory.py in the SAME folder as this file (or point
SCRAPER_PATH at it). DRY_RUN is True until you set it False.
"""
from __future__ import annotations

import importlib.util
import os
import re
import sys
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

# ===========================================================================
# CONFIG
# ===========================================================================
DRY_RUN = True                    # True = preview only. False = apply.
LOG_TO_MANUAL_OVERRIDES = True
OVERRIDDEN_BY = "triarchy backfill"

BRAND = "TRIARCHY"

SQL_SERVER   = os.environ.get("SQL_SERVER",   "denim-sql.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "denim_analytics")
HARDCODED_USERNAME = ""
HARDCODED_PASSWORD = ""
SQL_USERNAME = HARDCODED_USERNAME or os.environ.get("SQL_USERNAME", "")
SQL_PASSWORD = HARDCODED_PASSWORD or os.environ.get("SQL_PASSWORD", "")

SCRAPER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "triarchy_inventory.py")

# ===========================================================================
# Field maps (CSV header in the scraper -> style_info column)
# ===========================================================================
# Categorization fields that live ONLY in style_info:
STYLE_INFO_FIELDS: List[Tuple[str, str]] = [
    ("Jean Style",           "jean_style"),
    ("Rise Label",           "rise_label"),
    ("Inseam Style",         "inseam_style"),
    ("Color - Simplified",   "color_simplified"),
    ("Color - Standardized", "color_standardized"),
]
# color and style_name also live in these tables (table, pk column):
MULTI_TABLE = [
    ("style_info",    "style_info_id"),
    ("lookup",        "lookup_id"),
    ("style_metrics", "style_metric_id"),
]


# ===========================================================================
# Helpers
# ===========================================================================
def s(v) -> str:
    if v is None:
        return ""
    if isinstance(v, Decimal):
        v = f"{v:.3f}".rstrip("0").rstrip(".")
    return str(v).strip()


def norm(v) -> str:
    return re.sub(r"\s+", " ", s(v)).strip().lower()


def load_scraper(path: str):
    if not os.path.exists(path):
        sys.exit(f"ERROR: scraper not found at {path}\n"
                 f"Put triarchy_inventory.py next to this script or edit SCRAPER_PATH.")
    spec = importlib.util.spec_from_file_location("triarchy_inventory", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod


# ===========================================================================
# Re-derive every managed field, replicating TriarchyScraper.build_rows()
# minus all network I/O.
# ===========================================================================
def build_derived_rows(g, db_rows: List[Dict]) -> List[List[str]]:
    H = g.CSV_HEADERS
    rows: List[List[str]] = []
    for db in db_rows:
        row = [""] * len(H)
        title = db["product_name"]
        desc  = g.normalize_text(db["description"])
        tags  = db["tags"]
        leg   = db["leg_opening"]
        ins   = db["inseam"]
        rise  = db["rise"]

        # Color is derived from the product title's " - COLOR" suffix (the
        # scraper's variant_title/option1 fallbacks aren't available at
        # style_info grain, but product_name carries the color for TRIARCHY).
        color = g.extract_color(title, "", "")

        jean_style   = g.derive_jean_style(title, desc, leg)
        rise_label   = g.derive_rise_label(title, desc)
        inseam_style = g.derive_inseam_style(jean_style, title, ins, desc)
        color_std    = g.derive_color_standardized(color, desc, tags)
        color_simp   = g.derive_color_simplified(color, desc, color_std)
        style_name   = g.derive_style_name_base(title)

        g._set(row, "Product",              title)
        g._set(row, "Style Name",           style_name)
        g._set(row, "Description",          desc)
        g._set(row, "Tags",                 tags)
        g._set(row, "Color",                color)
        g._set(row, "Rise",                 rise)
        g._set(row, "Inseam",               ins)
        g._set(row, "Leg Opening",          leg)
        g._set(row, "Jean Style",           jean_style)
        g._set(row, "Rise Label",           rise_label)
        g._set(row, "Inseam Style",         inseam_style)
        g._set(row, "Color - Simplified",   color_simp)
        g._set(row, "Color - Standardized", color_std)
        rows.append(row)

    # cross-row group passes, same order as the live scraper
    g.apply_style_name_rules(rows)
    g.apply_jean_style_inference(rows)
    g.apply_inseam_style_refresh(rows)
    g.apply_rise_label_inference(rows)
    g.apply_color_inference(rows)
    g.apply_petite_inseam_rule(rows)   # blanks petite Inseam (measurement only)
    return rows


# ===========================================================================
# Main
# ===========================================================================
def main() -> None:
    print("=" * 66)
    print("TRIARCHY STYLE_INFO BACKFILL")
    print("DRY RUN - nothing will be written" if DRY_RUN else "LIVE RUN - applying changes")
    print("=" * 66)

    g = load_scraper(SCRAPER_PATH)
    print(f"Loaded classification rules from: {SCRAPER_PATH}")

    if not SQL_USERNAME or not SQL_PASSWORD:
        sys.exit("ERROR: set SQL_USERNAME and SQL_PASSWORD (env vars) or fill "
                 "HARDCODED_USERNAME/PASSWORD near the top of this file.")

    import pymssql
    conn = pymssql.connect(server=SQL_SERVER, user=SQL_USERNAME,
                           password=SQL_PASSWORD, database=SQL_DATABASE,
                           timeout=120, login_timeout=30)
    cur = conn.cursor(as_dict=True)

    cur.execute("""
        SELECT style_info_id, is_manual_override, brand, style_id,
               product_name, handle, color, style_name,
               jean_style, inseam_style, rise_label,
               color_simplified, color_standardized,
               description, tags, leg_opening, inseam, rise
        FROM style_info
        WHERE brand = %s
    """, (BRAND,))
    raw = cur.fetchall()
    print(f"Read {len(raw)} {BRAND} rows from style_info.\n")

    db_rows = [{
        "pk":           r["style_info_id"],
        "locked":       str(r["is_manual_override"]) in ("1", "True", "true"),
        "brand":        s(r["brand"]),
        "style_id":     s(r["style_id"]),
        "handle":       s(r["handle"]),
        "product_name": s(r["product_name"]),
        "color":        s(r["color"]),
        "style_name":   s(r["style_name"]),
        "description":  s(r["description"]),
        "tags":         s(r["tags"]),
        "leg_opening":  s(r["leg_opening"]),
        "inseam":       s(r["inseam"]),
        "rise":         s(r["rise"]),
        "_stored": {
            "jean_style":         s(r["jean_style"]),
            "inseam_style":       s(r["inseam_style"]),
            "rise_label":         s(r["rise_label"]),
            "color_simplified":   s(r["color_simplified"]),
            "color_standardized": s(r["color_standardized"]),
        },
    } for r in raw]

    derived = build_derived_rows(g, db_rows)

    # ---- stage changes ----------------------------------------------------
    plan: List[Tuple[str, str, object, str, str, str, bool]] = []
    counts: Dict[str, int] = {}
    unresolved: List[Tuple[str, str, str]] = []
    skipped_locked = 0

    def add(table, pk_col, pk, field, old, new, is_si=False):
        plan.append((table, pk_col, pk, field, s(old), s(new), is_si))
        counts[field if is_si and table == "style_info" else f"{field} ({table})"] = \
            counts.get(field if is_si and table == "style_info" else f"{field} ({table})", 0) + 1

    for db, drow in zip(db_rows, derived):
        if db["locked"]:
            skipped_locked += 1
            continue
        pk = db["pk"]
        match = {"brand": db["brand"], "product_name": db["product_name"]}

        # (1) style_info-only categorization fields
        for hdr, colname in STYLE_INFO_FIELDS:
            new_val = s(g._col(drow, hdr))
            old_val = db["_stored"][colname]
            if new_val == "":
                if old_val:
                    unresolved.append((db["product_name"], hdr, old_val))
                continue
            if norm(new_val) != norm(old_val):
                add("style_info", "style_info_id", pk, colname, old_val, new_val, True)

        # (2) color - style_info + lookup + style_metrics
        new_color = s(g._col(drow, "Color"))
        if new_color and norm(new_color) != norm(db["color"]):
            add("style_info", "style_info_id", pk, "color", db["color"], new_color, True)
            for table, pkc in MULTI_TABLE:
                if table != "style_info":
                    _stage_match_update(cur, plan, counts, table, pkc, "color",
                                        new_color, match)

        # (3) style_name - style_info + lookup + style_metrics
        new_sn = s(g._col(drow, "Style Name"))
        if new_sn and norm(new_sn) != norm(db["style_name"]):
            add("style_info", "style_info_id", pk, "style_name", db["style_name"], new_sn, True)
            for table, pkc in MULTI_TABLE:
                if table != "style_info":
                    _stage_match_update(cur, plan, counts, table, pkc, "style_name",
                                        new_sn, match)

    # ---- preview ----------------------------------------------------------
    print("-" * 66)
    print("PLANNED CHANGES (rows affected, by field/table):")
    if counts:
        for k in sorted(counts):
            print(f"   {k:<38} {counts[k]:>6}")
    else:
        print("   none - everything already matches the rules")
    print(f"\nRows skipped (is_manual_override = 1): {skipped_locked}")
    print(f"Existing values the rules can't confirm (left as-is): {len(unresolved)}")
    for prod, hdr, val in unresolved[:10]:
        print(f"   {prod} :: {hdr} = '{val}'")
    if len(unresolved) > 10:
        print(f"   ... and {len(unresolved) - 10} more")
    print(f"\nTotal individual column writes queued: {len(plan)}")

    if DRY_RUN:
        print("\nDRY RUN complete - nothing written. Review the numbers, then set "
              "DRY_RUN = False and run again to apply.")
        conn.close()
        return

    # ---- apply (single transaction; rolls back on any error) --------------
    print("\nApplying...")
    applied = audit = 0
    try:
        for table, pkc, pk, field, old, new, is_si in plan:
            cur.execute(f"UPDATE [{table}] SET [{field}] = %s WHERE [{pkc}] = %s",
                        (new, pk))
            applied += 1
            if is_si and LOG_TO_MANUAL_OVERRIDES:
                cur.execute(
                    "INSERT INTO manual_overrides "
                    "(table_name, record_id, field_name, old_value, new_value, "
                    " overridden_at, overridden_by, notes) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (table, pk, field, old, new, datetime.now(), OVERRIDDEN_BY,
                     "automated TRIARCHY categorization backfill"))
                audit += 1
        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        print("\nERROR during apply - transaction rolled back, database unchanged.")
        raise

    conn.close()
    print(f"Done. {applied} column writes applied. {audit} style_info changes "
          f"logged to manual_overrides.")


def _stage_match_update(cur, plan, counts, table, pk_col, field, new_val, match):
    """Find rows in `table` matching `match` (dict of col->value, blanks
    ignored) and queue a per-row UPDATE for `field` where it actually differs."""
    where, params = [], []
    for c, v in match.items():
        if v == "" or v is None:
            continue
        where.append(f"[{c}] = %s")
        params.append(v)
    if not where:
        return
    cur.execute(f"SELECT [{pk_col}] AS pk, [{field}] AS cur_val FROM [{table}] "
                f"WHERE {' AND '.join(where)}", params)
    for r in cur.fetchall():
        if norm(r["cur_val"]) != norm(new_val):
            plan.append((table, pk_col, r["pk"], field, s(r["cur_val"]),
                         s(new_val), False))
            key = f"{field} ({table})"
            counts[key] = counts.get(key, 0) + 1


if __name__ == "__main__":
    main()
