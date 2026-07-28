# -*- coding: utf-8 -*-
"""
Backfill / repair RAMYBROOK categorization in the denim_analytics database.

WHY
---
RAMYBROOK rows imported before ramybrook_inventory.py was upgraded have
categorization fields that are blank or no longer match the current rules.
This script re-derives them with the *exact* scraper logic and fixes the
database in place. It also links the duplicate "double color" listings back
to their older twin so both histories live under one product.

WHAT IT CHANGES  (all scoped to brand = 'RAMYBROOK')
----------------------------------------------------
1. Categorization fields in style_info (blank OR not matching the rules):
       jean_style, rise_label, inseam_label, inseam_style,
       color_simplified, color_standardized
   -> re-derived with the real scraper functions, including the cross-row
      group passes (sibling inference, majority vote, color propagation).

1b. product_type: BLANK-fill only (never overwrite). ramybrook_inventory.py
    passes Shopify's productType through unchanged ("Jeans" for every RAMYBROOK
    jeans row), so blanks are filled from a same-style sibling, else the brand
    mode. product_type exists only in style_info.

2. style_name re-derived and kept in sync across style_info + lookup +
   style_metrics (validated 100% against the canonical scraper output). It is
   the only one of these fields that exists on tables other than style_info.

3. product_name: the "Style | COLOR - COLOR" duplicates are normalized to the
   "Style - COLOR" convention in style_info + lookup + style_metrics +
   image_url_history. NOTHING IS DELETED.

   (style_name_grouping is intentionally left untouched - the daily scraper
    does not populate it, so neither does this backfill.)

SAFETY
------
* DRY_RUN = True by default. Prints a full preview and writes NOTHING.
  Read the preview, then set DRY_RUN = False and run again to apply.
* Rows with is_manual_override = 1 are skipped (human-locked).
* Only NON-BLANK derived values overwrite; a value the rules can't produce
  never blanks an existing one.
* Every style_info field change is logged to manual_overrides (old -> new).
* is_manual_override is never set by this script, so future scraper runs can
  still update these rows normally.

HOW TO RUN  (Windows, plain Command Prompt - the black "cmd" window)
--------------------------------------------------------------------
    pip install pymssql
    set SQL_USERNAME=carrieboromisa
    set SQL_PASSWORD=your-password
    python "C:\\path\\to\\backfill_ramybrook_style_info.py"

Keep ramybrook_inventory.py in the SAME folder as this file (or point
SCRAPER_PATH at it). You can also fill HARDCODED_USERNAME/PASSWORD below to
skip the `set` lines.
"""
from __future__ import annotations

import importlib.util
import os
import re
import sys
import csv
from collections import Counter
from datetime import datetime, date
from decimal import Decimal

_MIN_DATE = date(1900, 1, 1)
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ===========================================================================
# CONFIG
# ===========================================================================
DRY_RUN = True                       # True = preview only. False = apply.
FIX_DOUBLE_COLOR_PRODUCT_NAMES = True
# style_name_grouping is NOT populated by ramybrook_inventory.py, so the
# backfill leaves it alone too (keeps the DB consistent with the daily scrape).
LINK_VIA_STYLE_NAME_GROUPING   = False
LOG_TO_MANUAL_OVERRIDES        = True

# After normalizing product_names you can end up with TWO style_info rows for
# the same product_name (the old dead-style_id listing + the new live one).
# style_info is the product DIMENSION and needs a unique product_name for a
# clean Power Pivot join ("all in one line"). This step keeps the newest row
# per (brand, product_name) and deletes the older redundant one FROM style_info
# ONLY - never from lookup/style_metrics/variant_metrics, so no history is lost.
# Off by default because it deletes dimension rows; the dry run shows you every
# keep/delete first. Locked rows (is_manual_override=1) are never deleted.
CONSOLIDATE_DUPLICATE_STYLE_INFO = False
OVERRIDDEN_BY = "ramybrook backfill"

BRAND = "RAMYBROOK"

SQL_SERVER   = os.environ.get("SQL_SERVER",   "denim-sql.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "denim_analytics")
HARDCODED_USERNAME = ""
HARDCODED_PASSWORD = ""
SQL_USERNAME = HARDCODED_USERNAME or os.environ.get("SQL_USERNAME", "")
SQL_PASSWORD = HARDCODED_PASSWORD or os.environ.get("SQL_PASSWORD", "")

SCRAPER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "ramybrook_inventory.py")
OUTPUT_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / "Output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ===========================================================================
# Exact schema (from denim_analytics). CSV-header -> style_info column.
# ===========================================================================
# Categorization fields that live ONLY in style_info:
STYLE_INFO_FIELDS: List[Tuple[str, str]] = [
    ("Jean Style",           "jean_style"),
    ("Inseam Label",         "inseam_label"),
    ("Inseam Style",         "inseam_style"),
    ("Rise Label",           "rise_label"),
    ("Color - Simplified",   "color_simplified"),
    ("Color - Standardized", "color_standardized"),
]
# style_name lives in these tables and must stay in sync (table, pk column):
STYLE_NAME_TABLES = [
    ("style_info",    "style_info_id"),
    ("lookup",        "lookup_id"),
    ("style_metrics", "style_metric_id"),
]
# product_name lives in these tables (image_url_history has no handle/color):
PRODUCT_NAME_TABLES = [
    ("style_info",       "style_info_id"),
    ("lookup",           "lookup_id"),
    ("style_metrics",    "style_metric_id"),
    ("image_url_history", "image_id"),
]
GROUPING_TABLES = STYLE_NAME_TABLES  # style_name_grouping lives here too


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
                 f"Put ramybrook_inventory.py next to this script or edit SCRAPER_PATH.")
    spec = importlib.util.spec_from_file_location("ramybrook_inventory", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod


def normalize_double_color(name: str) -> Optional[str]:
    """'Cindy ... Jean | BLACK - BLACK' -> 'Cindy ... Jean - BLACK'.
    Returns the corrected name, or None if it isn't a doubled-color name."""
    if " | " not in name:
        return None
    left, right = name.split(" | ", 1)
    parts = [p.strip() for p in right.split(" - ") if p.strip()]
    if not parts:
        return None
    fixed = f"{left.strip()} - {parts[-1]}"
    return fixed if fixed != name else None


def grouping_base(product_name: str, color: str) -> str:
    """The 'Style' portion used to link colorways/duplicates together:
    product_name minus the trailing '- COLOR'."""
    pn = product_name.strip()
    if color and pn.lower().endswith(" - " + color.strip().lower()):
        return pn[: len(pn) - len(color.strip()) - 3].strip()
    if " - " in pn:
        return pn.rsplit(" - ", 1)[0].strip()
    return pn


# ===========================================================================
# Re-derive every managed field, replicating RamyBrookScraper.build_rows()
# minus all network I/O.
# ===========================================================================
def build_derived_rows(g, db_rows: List[Dict]) -> List[List[str]]:
    H = g.CSV_HEADERS
    rows: List[List[str]] = []
    for db in db_rows:
        row = [""] * len(H)
        title = db["product_name"]
        desc  = g.clean_description(db["description"])
        color = db["color"]
        tags  = db["tags"]

        leg  = db["leg_opening"] or g.extract_leg_opening(desc)
        ins  = db["inseam"]      or g.extract_inseam(desc)
        rise = db["rise"]        or g.extract_rise(desc)

        jean_style = g.derive_jean_style(title, desc, leg)
        rise_label = g.derive_rise_label(title, desc, tags)
        color_std  = g.derive_color_standardized(color, title)
        color_simp = g.derive_color_simplified(color, color_std)
        inseam_label = g.derive_inseam_label(jean_style, ins, title, "", desc)
        inseam_style = g.derive_inseam_style(jean_style, ins, inseam_label)

        g._set(row, "Product",              title)
        g._set(row, "Style Name",           g.derive_style_name_base(title))
        g._set(row, "Description",          desc)
        g._set(row, "Tags",                 tags)
        g._set(row, "Color",                color)
        g._set(row, "Rise",                 rise)
        g._set(row, "Inseam",               ins)
        g._set(row, "Leg Opening",          leg)
        g._set(row, "Jean Style",           jean_style)
        g._set(row, "Rise Label",           rise_label)
        g._set(row, "Inseam Label",         inseam_label)
        g._set(row, "Inseam Style",         inseam_style)
        g._set(row, "Color - Simplified",   color_simp)
        g._set(row, "Color - Standardized", color_std)
        rows.append(row)

    # cross-row group passes, same order/behaviour as the live scraper
    g.apply_petite_inseam_rule(rows)
    g.apply_style_name_rules(rows)
    g.apply_jean_style_inference(rows)
    g.apply_jean_style_from_algolia_tags(rows, {})   # no Algolia here -> no-op
    g.apply_jean_style_inference(rows)
    g.apply_inseam_label_style_refresh(rows)
    g.apply_rise_label_inference(rows)
    g.apply_color_inference(rows)
    return rows


# ===========================================================================
# Main
# ===========================================================================
def main() -> None:
    print("=" * 66)
    print("RAMYBROOK STYLE_INFO BACKFILL")
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

    # ---- read every RAMYBROOK style_info row ------------------------------
    cur.execute("""
        SELECT style_info_id, is_manual_override, brand, style_id,
               product_name, handle, color,
               style_name, style_name_grouping, product_type,
               jean_style, inseam_label, inseam_style, rise_label,
               color_simplified, color_standardized,
               description, tags, leg_opening, inseam, rise, created_at
        FROM style_info
        WHERE brand = %s
    """, (BRAND,))
    raw = cur.fetchall()
    print(f"Read {len(raw)} {BRAND} rows from style_info.\n")

    db_rows = [{
        "pk":                  r["style_info_id"],
        "locked":              str(r["is_manual_override"]) in ("1", "True", "true"),
        "brand":               s(r["brand"]),
        "style_id":            s(r["style_id"]),
        "handle":              s(r["handle"]),
        "color":               s(r["color"]),
        "product_name":        s(r["product_name"]),
        "style_name":          s(r["style_name"]),
        "style_name_grouping": s(r["style_name_grouping"]),
        "product_type":        s(r["product_type"]),
        "description":         s(r["description"]),
        "tags":                s(r["tags"]),
        "leg_opening":         s(r["leg_opening"]),
        "inseam":              s(r["inseam"]),
        "rise":                s(r["rise"]),
        "created_at":          r["created_at"],
        "_stored": {
            "jean_style":         s(r["jean_style"]),
            "inseam_label":       s(r["inseam_label"]),
            "inseam_style":       s(r["inseam_style"]),
            "rise_label":         s(r["rise_label"]),
            "color_simplified":   s(r["color_simplified"]),
            "color_standardized": s(r["color_standardized"]),
        },
    } for r in raw]

    derived = build_derived_rows(g, db_rows)

    # ---- stage changes ----------------------------------------------------
    # each planned change: (table, pk_col, pk, field, old, new, is_style_info)
    plan: List[Tuple[str, str, object, str, str, str, bool]] = []
    counts: Dict[str, int] = {}
    pname_fixes: List[Tuple[str, str]] = []
    unresolved: List[Tuple[str, str, str]] = []
    skipped_locked = 0

    def add(table, pk_col, pk, field, old, new, is_si=False):
        plan.append((table, pk_col, pk, field, s(old), s(new), is_si))
        counts[field] = counts.get(field, 0) + 1

    for db, drow in zip(db_rows, derived):
        if db["locked"]:
            skipped_locked += 1
            continue
        pk = db["pk"]

        # (1) categorization fields - style_info only
        for hdr, colname in STYLE_INFO_FIELDS:
            new_val = s(g._col(drow, hdr))
            old_val = db["_stored"][colname]
            if new_val == "":
                if old_val:
                    unresolved.append((db["product_name"], hdr, old_val))
                continue
            if norm(new_val) != norm(old_val):
                add("style_info", "style_info_id", pk, colname, old_val, new_val, True)

        # (2) style_name - re-derived and kept in sync across
        # style_info + lookup + style_metrics. Validated 100% against the
        # canonical scraper's output. lookup/style_metrics join to style_info
        # on product_name (the documented connective key).
        new_sn = s(g._col(drow, "Style Name"))
        if new_sn and norm(new_sn) != norm(db["style_name"]):
            add("style_info", "style_info_id", pk, "style_name",
                db["style_name"], new_sn, True)
            match = {"brand": db["brand"], "product_name": db["product_name"]}
            for table, pkc in STYLE_NAME_TABLES:
                if table == "style_info":
                    continue
                _stage_match_update(cur, plan, counts, table, pkc, "style_name",
                                    new_sn, match)

        # (3) product_name normalization (doubled -> convention)
        norm_pn = normalize_double_color(db["product_name"]) if FIX_DOUBLE_COLOR_PRODUCT_NAMES else None
        effective_pn = norm_pn or db["product_name"]
        if norm_pn:
            pname_fixes.append((db["product_name"], norm_pn))
            add("style_info", "style_info_id", pk, "product_name",
                db["product_name"], norm_pn, True)
            # propagate to lookup / style_metrics (match on OLD product_name)
            # and image_url_history (match on brand + style_id + old name)
            m_old = {"brand": db["brand"], "product_name": db["product_name"]}
            for table, pkc in [("lookup", "lookup_id"), ("style_metrics", "style_metric_id")]:
                _stage_match_update(cur, plan, counts, table, pkc, "product_name",
                                    norm_pn, m_old)
            _stage_match_update(cur, plan, counts, "image_url_history", "image_id",
                                "product_name", norm_pn,
                                {"brand": db["brand"], "style_id": db["style_id"],
                                 "product_name": db["product_name"]})

        # (4) style_name_grouping - link duplicates/colorways together
        if LINK_VIA_STYLE_NAME_GROUPING:
            grp = grouping_base(effective_pn, db["color"])
            if grp and norm(grp) != norm(db["style_name_grouping"]):
                match = {"brand": db["brand"], "product_name": db["product_name"]}
                for table, pkc in GROUPING_TABLES:
                    if table == "style_info":
                        add("style_info", pkc, pk, "style_name_grouping",
                            db["style_name_grouping"], grp, True)
                    else:
                        _stage_match_update(cur, plan, counts, table, pkc,
                                            "style_name_grouping", grp, match)

    # ---- product_type (blank fill only) -----------------------------------
    # ramybrook_inventory.py has no text rule for product_type - it passes
    # Shopify's productType straight through, which is "Jeans" for every
    # RAMYBROOK jeans-collection row. So fill BLANKS only (never overwrite),
    # using a same-style sibling's value, falling back to the brand's mode.
    # product_type exists only in style_info, so nothing to propagate.
    _pt_all = [d["product_type"] for d in db_rows if d["product_type"]]
    _pt_global = Counter(_pt_all).most_common(1)[0][0] if _pt_all else ""
    _pt_by_sn: Dict[str, Counter] = {}
    for d in db_rows:
        if d["product_type"]:
            _pt_by_sn.setdefault(norm(d["style_name"]), Counter())[d["product_type"]] += 1
    for d in db_rows:
        if d["locked"] or d["product_type"]:
            continue
        c = _pt_by_sn.get(norm(d["style_name"]))
        fill = c.most_common(1)[0][0] if c else _pt_global
        if fill:
            add("style_info", "style_info_id", d["pk"], "product_type", "", fill, True)

    # ---- consolidate duplicate style_info dimension rows (opt-in) ---------
    # keep newest per (brand, final product_name); delete older redundant ones
    deletes: List[Dict] = []
    if CONSOLIDATE_DUPLICATE_STYLE_INFO:
        by_pn: Dict[Tuple[str, str], List[Dict]] = {}
        for db in db_rows:
            final_pn = normalize_double_color(db["product_name"]) or db["product_name"]
            db["_final_pn"] = final_pn
            by_pn.setdefault((db["brand"], norm(final_pn)), []).append(db)
        for (_, _), members in by_pn.items():
            if len(members) < 2:
                continue
            # never delete a locked row; keep newest created_at then highest pk
            keepers = sorted(
                members,
                key=lambda d: (d["created_at"] or _MIN_DATE, d["pk"]),
                reverse=True)
            survivor = keepers[0]
            for d in keepers[1:]:
                if d["locked"]:
                    continue  # protected - leave it, admit two rows remain
                deletes.append({"pk": d["pk"], "product_name": d["_final_pn"],
                                "style_id": d["style_id"], "survivor": survivor["pk"],
                                "survivor_style_id": survivor["style_id"]})

    report_path = _write_duplicate_report(g, db_rows)

    # ---- preview ----------------------------------------------------------
    print("-" * 66)
    print("PLANNED CHANGES (rows affected, by field/table):")
    if counts:
        for k in sorted(counts):
            print(f"   {k:<38} {counts[k]:>6}")
    else:
        print("   none - everything already matches the rules")
    print(f"\nDouble-color product_names normalized: {len(pname_fixes)}")
    for old, new in pname_fixes[:15]:
        print(f"   {old}  ->  {new}")
    if len(pname_fixes) > 15:
        print(f"   ... and {len(pname_fixes) - 15} more")
    print(f"\nRows skipped (is_manual_override = 1): {skipped_locked}")
    print(f"Existing values the rules can't confirm (left as-is): {len(unresolved)}")
    for prod, hdr, val in unresolved[:10]:
        print(f"   {prod} :: {hdr} = '{val}'")
    if len(unresolved) > 10:
        print(f"   ... and {len(unresolved) - 10} more")
    if CONSOLIDATE_DUPLICATE_STYLE_INFO:
        print(f"\nDuplicate style_info rows to DELETE (dimension only, history kept): {len(deletes)}")
        for d in deletes[:15]:
            print(f"   delete style_info_id={d['pk']} style_id={d['style_id']} "
                  f"'{d['product_name']}'  (kept id={d['survivor']} "
                  f"style_id={d['survivor_style_id']})")
        if len(deletes) > 15:
            print(f"   ... and {len(deletes) - 15} more")

    print(f"\nTotal individual column writes queued: {len(plan)}")
    print(f"Duplicate review report: {report_path}")

    if DRY_RUN:
        print("\nDRY RUN complete - nothing written. Review the numbers and the "
              "report csv, then set DRY_RUN = False and run again to apply.")
        conn.close()
        return

    # ---- apply (single transaction; rolls back completely on any error) ---
    print("\nApplying...")
    applied = audit = deleted = 0
    try:
        # 1) DELETE the consolidated duplicate style_info rows FIRST. style_info
        #    has a unique index on (brand, product_name), so the stale row must
        #    be gone before we rename its surviving twin onto the same name.
        deleted_pks = set()
        if CONSOLIDATE_DUPLICATE_STYLE_INFO:
            for d in deletes:
                if LOG_TO_MANUAL_OVERRIDES:
                    cur.execute(
                        "INSERT INTO manual_overrides "
                        "(table_name, record_id, field_name, old_value, new_value, "
                        " overridden_at, overridden_by, notes) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                        ("style_info", d["pk"], "(row deleted - duplicate)",
                         f"{d['product_name']} / style_id {d['style_id']}", None,
                         datetime.now(), OVERRIDDEN_BY,
                         f"consolidated into style_info_id {d['survivor']}"))
                cur.execute("DELETE FROM style_info WHERE style_info_id = %s", (d["pk"],))
                deleted_pks.add(d["pk"])
                deleted += 1

        # 2) Apply the staged column writes.
        for table, pkc, pk, field, old, new, is_si in plan:
            if is_si and pk in deleted_pks:
                continue  # this row was just consolidated away
            # defensive guard: never violate the unique (brand, product_name)
            # index - if the target name is still taken by another surviving
            # style_info row, skip the rename instead of crashing.
            if is_si and field == "product_name":
                cur.execute("SELECT COUNT(*) AS n FROM style_info "
                            "WHERE brand = %s AND product_name = %s "
                            "AND style_info_id <> %s", (BRAND, new, pk))
                if (cur.fetchone() or {}).get("n", 0) > 0:
                    print(f"   SKIP rename style_info_id={pk} -> '{new}' "
                          f"(name already exists; enable "
                          f"CONSOLIDATE_DUPLICATE_STYLE_INFO to merge)")
                    continue
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
                     "automated RAMYBROOK categorization backfill"))
                audit += 1

        conn.commit()
    except Exception:
        conn.rollback()
        conn.close()
        print("\nERROR during apply - transaction rolled back, database unchanged.")
        raise

    conn.close()
    print(f"Done. {applied} column writes applied. {audit} style_info changes "
          f"logged to manual_overrides." +
          (f" {deleted} duplicate style_info rows consolidated." if deleted else ""))


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


def _write_duplicate_report(g, db_rows) -> Path:
    """Non-destructive: pair the newer '| COLOR - COLOR' rows with the older
    'Style - COLOR' rows so you can eyeball the linkage."""
    groups: Dict[Tuple[str, str], List[Dict]] = {}
    for db in db_rows:
        key = (norm(g.derive_style_name_base(db["product_name"])), norm(db["color"]))
        groups.setdefault(key, []).append(db)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = OUTPUT_DIR / f"RAMYBROOK_duplicate_report_{ts}.csv"
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(["group_style_name", "group_color", "kind", "style_info_id",
                    "style_id", "product_name", "handle", "created_at"])
        for (sn, cl), members in sorted(groups.items()):
            has_double = any(" | " in m["product_name"] for m in members)
            if len(members) < 2 and not has_double:
                continue
            for m in members:
                kind = "double-color/new" if " | " in m["product_name"] else "convention/old"
                w.writerow([sn, cl, kind, m["pk"], m["style_id"],
                            m["product_name"], m["handle"], m["created_at"]])
    return path


if __name__ == "__main__":
    main()
