# -*- coding: utf-8 -*-
"""
AGJEANS database cleanup - like the AMO cleanup, but corrections are matched by
VALUE + handle/sku (never by *_id).

Driven by AG_corrections_not_using_ID_claude.csv, whose columns read as a rule:

    On: <tables>   IF: <field> = <current value>   And IF: <key field> = <key>
    Then Change: <field>  To: <new value | "USE agjeans_inventory TO FILL">

So each row means, for every listed table that has those columns:

    UPDATE <table>
       SET <change field> = <new value>
     WHERE brand = 'AGJEANS'
       AND <match field>  = <current value>     -- blank current => field IS blank
       AND <key field>    = <key>               -- handle, or sku_shopify

Matching on the CURRENT value means a row already carrying the corrected value is
left alone (idempotent), and only the intended rows change. All string compares
are wrapped in CAST(... AS varchar) so the brand/handle/sku/value predicates use
their indexes instead of scanning (variant_metrics is ~24M rows).

PHASES
  1. REMOVE   the non-jean handles (from the review workbook's ACTION=REMOVE
              rows) out of lookup / style_info / style_metrics, and their
              variants out of variant_metrics (linked by sku_shopify).
  2. CORRECT  apply the literal corrections. Rows whose "To" is
              "USE agjeans_inventory TO FILL" are DERIVED - see DERIVE below;
              until that stage is enabled they are counted and skipped, never
              guessed.
  3. DEDUPE   collapse duplicates the corrections create, keeping the oldest /
              corrected row - same keys as AMO:
                lookup      (brand+sku_shopify), (brand+sku_brand)
                style_info  (brand+product_name+inseam_label),
                            (brand+style_id+inseam_label)

DERIVE (stage 2, DO_DERIVED): the 320 "USE agjeans_inventory TO FILL" cells and
the one missing style_info insert (Farrah Skinny Ankle - WHITE) are computed with
agjeans_inventory.py @ 74af9d5. Left off by default until validated.

SAFETY: DRY_RUN=True prints the whole plan and writes nothing. Per-phase and
per-N-row commits; a checkpoint lets a dropped run resume. Every log line is
Central-time stamped. Ids/barcodes written as full-precision strings.
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
import datetime as dt
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

# ===========================================================================
# CONFIG
# ===========================================================================
DRY_RUN = True
DO_REMOVE      = True
DO_CORRECTIONS = True
DO_DEDUPE      = True
DO_DERIVED     = False   # stage 2: apply "USE agjeans_inventory TO FILL" cells
DO_INSERT_MISSING = False  # stage 2: insert the one missing Farrah style_info row
RESET_PROGRESS = False

BRAND = "AGJEANS"

CORRECTIONS_CSV = r"AG_corrections_not_using_ID_claude.csv"
REVIEW_CSV      = r"AGJEANS_DB_Values_20260904_124324_Claude.csv"   # ACTION=REMOVE
SCRAPER_PATH    = r"agjeans_inventory.py"     # pinned 74af9d5 (for DO_DERIVED)
OUTPUT_DIR      = r"AGJEANS_Output"           # scraper CSVs (for the insert)

# The single missing style_info row (given by you); its description/measurements
# come from the AGJEANS_2025-10-09_00-19-47 output file when DO_INSERT_MISSING.
MISSING_STYLE = {
    "style_id": "8522771202280",
    "product_name": "Farrah Skinny Ankle - WHITE",
    "handle": "farrah-skinny-ankle-mid-rise-skinny-ankle-cloud-soft-denim-hsd1777rhwht",
}

SQL_SERVER   = os.environ.get("SQL_SERVER",   "denim-sql.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "denim_analytics")
SQL_USERNAME = os.environ.get("SQL_USERNAME", "carrieboromisa")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CHECKPOINT_FILE = os.path.join(SCRIPT_DIR, f"cleanup_{BRAND.lower()}_checkpoint.json")
COMMIT_EVERY = 2000
DELETE_BATCH = 5000
VC = "CAST(%s AS varchar(255))"

PK = {"lookup": "lookup_id", "style_info": "style_info_id",
      "style_metrics": "style_metric_id", "variant_metrics": "variant_metric_id"}

# "On:" text (lowercased) -> the DB tables it applies to.
ON_TABLES = {
    "lookup, style_info, and style_metrics": ["lookup", "style_info", "style_metrics"],
    "style_info": ["style_info"],
    "lookup and variant_metrics": ["lookup", "variant_metrics"],
    "variant_metrics": ["variant_metrics"],
}

# Which columns each table actually has (so a correction only touches tables that
# carry that column). Confirmed from the DB export.
TABLE_COLS = {
    "lookup": {"style_id", "product_name", "handle", "sku_url", "color",
               "style_name", "style_name_grouping", "variant_title", "size",
               "sku_shopify", "sku_brand", "barcode"},
    "style_info": {"style_id", "product_name", "handle", "sku_url", "color",
                   "style_name", "style_name_grouping", "color_simplified",
                   "color_standardized", "gender", "hem_style", "inseam_label",
                   "inseam_style", "jean_style", "product_line", "product_type",
                   "rise_label", "stretch", "back_rise", "inseam", "knee",
                   "leg_opening", "rise", "description", "image_url", "size_chart",
                   "tags", "vendor", "country_produced", "fabric_source"},
    "style_metrics": {"style_id", "product_name", "handle", "sku_url", "color",
                      "style_name", "style_name_grouping", "inseam", "inseam_label"},
    "variant_metrics": {"sku_shopify", "sku_brand", "barcode", "variant_title",
                        "size", "quantity_available", "quantity_available_online"},
}

DEDUPE_KEYS = {
    "lookup":     [["brand", "sku_shopify"], ["brand", "sku_brand"]],
    "style_info": [["brand", "product_name", "inseam_label"],
                   ["brand", "style_id", "inseam_label"]],
}


# ===========================================================================
# Central-time logging
# ===========================================================================
def _tz():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo("America/Chicago")
    except Exception:
        return dt.timezone(dt.timedelta(hours=-6))
_TZ = _tz()


def log(msg: str = "") -> None:
    stamp = dt.datetime.now(_TZ).strftime("%Y-%m-%d %H:%M:%S")
    for part in str(msg).split("\n"):
        print(f"[{stamp}] {part}")


# ===========================================================================
# Value helpers
# ===========================================================================
def s(v) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, Decimal):
        return f"{v:f}"
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else repr(v)
    return str(v).strip()


def is_blank(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def is_use_scraper(v) -> bool:
    return isinstance(v, str) and "agjeans_inventory" in v.lower()


def norm_field(name: str) -> str:
    """Spec field label -> real DB column name."""
    return re.sub(r"\s+", "_", s(name).strip().lower())


def resolve(path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(SCRIPT_DIR, path)


def _read_csv(path: str) -> Tuple[List[str], List[List[str]]]:
    p = resolve(path)
    if not os.path.exists(p):
        sys.exit(f"ERROR: file not found: {p}")
    # these workbooks are exported from Excel as cp1252 (has degree signs etc.)
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            with open(p, encoding=enc, newline="") as fh:
                rows = list(csv.reader(fh))
            return rows[0], [r for r in rows[1:] if any(c.strip() for c in r)]
        except UnicodeDecodeError:
            continue
    sys.exit(f"ERROR: could not decode {p}")


# ===========================================================================
# Checkpoint
# ===========================================================================
def _empty_ckpt() -> dict:
    return {"removals": {}, "corrections": {}, "dedupe_done": {}}


def load_checkpoint() -> dict:
    if RESET_PROGRESS or not os.path.exists(CHECKPOINT_FILE):
        return _empty_ckpt()
    try:
        with open(CHECKPOINT_FILE, encoding="utf-8") as fh:
            data = json.load(fh)
        base = _empty_ckpt()
        base.update({k: data.get(k, base[k]) for k in base})
        return base
    except Exception as exc:
        log(f"(could not read checkpoint: {exc} - starting fresh)")
        return _empty_ckpt()


def save_checkpoint(ckpt: dict) -> None:
    try:
        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as fh:
            json.dump(ckpt, fh, indent=1)
    except Exception as exc:
        log(f"(WARNING: could not write checkpoint: {exc})")


def clear_checkpoint() -> None:
    try:
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
    except Exception as exc:
        log(f"(WARNING: could not remove checkpoint: {exc})")


# ===========================================================================
# Load the correction spec
# ===========================================================================
class Correction:
    __slots__ = ("tables", "match_field", "match_val", "key_field", "key_val",
                 "change_field", "new_val", "derive")

    def __init__(self, tables, mf, mv, kf, kv, cf, nv):
        self.tables = tables
        self.match_field = mf
        self.match_val = mv
        self.key_field = kf
        self.key_val = kv
        self.change_field = cf
        self.new_val = nv
        self.derive = is_use_scraper(nv)


def load_corrections() -> List[Correction]:
    hdr, rows = _read_csv(CORRECTIONS_CSV)
    out: List[Correction] = []
    skipped = 0
    for r in rows:
        r = (r + [""] * 7)[:7]
        on = s(r[0]).lower()
        tables = ON_TABLES.get(on)
        if not tables:
            skipped += 1
            continue
        out.append(Correction(
            tables=tables,
            mf=norm_field(r[1]), mv=r[2],           # keep raw current value
            kf=norm_field(r[3]), kv=r[4],
            cf=norm_field(r[5]), nv=r[6]))
    if skipped:
        log(f"(corrections: {skipped} rows had an unrecognized 'On:' and were skipped)")
    return out


def load_remove_handles() -> List[str]:
    hdr, rows = _read_csv(REVIEW_CSV)
    idx = {h.strip(): i for i, h in enumerate(hdr)}
    hi, ai = idx.get("handle"), idx.get("ACTION")
    if hi is None or ai is None:
        log("(review workbook missing handle/ACTION - no removals)")
        return []
    handles = {s(r[hi]).lower() for r in rows
               if len(r) > max(hi, ai) and "remove" in s(r[ai]).lower() and s(r[hi])}
    return sorted(handles)


# ===========================================================================
# Correction execution (value + key matched)
# ===========================================================================
def _match_clause(field: str, val) -> Tuple[str, list]:
    """SQL predicate + params for '<field> = <val>' with blank -> IS NULL/''."""
    if is_blank(val):
        return f"([{field}] IS NULL OR LTRIM(RTRIM([{field}])) = '')", []
    return f"[{field}] = {VC}", [s(val)]


def apply_corrections(cur, conn, corrections, deriver, ckpt):
    done = ckpt.setdefault("corrections", {})
    total_ops = derived_skipped = changed = 0
    start = int(done.get("_i", 0))
    for i, c in enumerate(corrections):
        if i < start:
            continue
        if c.derive:
            if not DO_DERIVED or deriver is None:
                derived_skipped += 1
                _advance(cur, conn, ckpt, done, i, changed)
                continue
            new_val = deriver.get(c.change_field, c.key_val, c.key_field)
            if is_blank(new_val):
                derived_skipped += 1
                _advance(cur, conn, ckpt, done, i, changed)
                continue
        else:
            new_val = c.new_val
        for tbl in c.tables:
            cols = TABLE_COLS.get(tbl, set())
            if c.change_field not in cols or c.match_field not in cols or c.key_field not in cols:
                continue
            mclause, mparams = _match_clause(c.match_field, c.match_val)
            kclause, kparams = _match_clause(c.key_field, c.key_val)
            sql = (f"UPDATE {tbl} SET [{c.change_field}] = {VC} "
                   f"WHERE brand = {VC} AND {mclause} AND {kclause}")
            params = [s(new_val), BRAND] + mparams + kparams
            cur.execute(sql, params)
            rc = cur.rowcount if (cur.rowcount and cur.rowcount > 0) else 0
            changed += rc
            total_ops += 1
        if (i + 1) % COMMIT_EVERY == 0:
            conn.commit()
            done["_i"] = i + 1
            save_checkpoint(ckpt)
            log(f"   corrections: {i+1}/{len(corrections)} rows applied "
                f"({changed} DB rows changed)...")
    conn.commit()
    done["_i"] = len(corrections)
    save_checkpoint(ckpt)
    log(f"   corrections: {len(corrections)} spec rows done - {changed} DB rows "
        f"changed; {derived_skipped} derived cells "
        f"{'applied' if DO_DERIVED else 'skipped (DO_DERIVED off)'}")


def _advance(cur, conn, ckpt, done, i, changed):
    if (i + 1) % COMMIT_EVERY == 0:
        conn.commit()
        done["_i"] = i + 1
        save_checkpoint(ckpt)


# ===========================================================================
# Removals (whole handles, all tables)
# ===========================================================================
def _delete_each_value(cur, conn, table, col, values) -> int:
    total = 0
    for v in values:
        while True:
            cur.execute(f"DELETE TOP ({DELETE_BATCH}) FROM {table} "
                        f"WHERE brand={VC} AND [{col}]={VC}", (BRAND, v))
            rc = cur.rowcount if (cur.rowcount and cur.rowcount > 0) else 0
            conn.commit()
            total += rc
            if rc < DELETE_BATCH:
                break
    return total


def apply_removals(cur, conn, handles, ckpt):
    done = ckpt.setdefault("removals", {})
    if not handles:
        log("   removals: none")
        return
    # variant_metrics has no handle column - resolve its variants via lookup FIRST
    if not done.get("variant_metrics"):
        skus = set()
        for h in handles:
            cur.execute(f"SELECT sku_shopify FROM lookup WHERE brand={VC} AND handle={VC}",
                        (BRAND, h))
            skus.update(s(r[0]) for r in cur.fetchall() if s(r[0]))
        n = _delete_each_value(cur, conn, "variant_metrics", "sku_shopify", sorted(skus))
        done["variant_metrics"] = True
        save_checkpoint(ckpt)
        log(f"   variant_metrics: removed {n} rows for {len(skus)} skus "
            f"of {len(handles)} handles (committed)")
    for tbl in ("lookup", "style_info", "style_metrics"):
        if done.get(tbl):
            continue
        n = _delete_each_value(cur, conn, tbl, "handle", handles)
        done[tbl] = True
        save_checkpoint(ckpt)
        log(f"   {tbl}: removed {n} rows for {len(handles)} handles (committed)")


# ===========================================================================
# Dedupe (keep oldest captured_datetime; blank key parts never merge)
# ===========================================================================
def norm(v) -> str:
    return re.sub(r"\s+", " ", s(v)).strip().lower()


def dedupe(cur, conn, table, key_cols, pk):
    cur.execute(f"SELECT {pk} AS pk, {', '.join('['+c+']' for c in key_cols)}, "
                f"captured_datetime FROM {table} WHERE brand={VC}", (BRAND,))
    groups: Dict[tuple, list] = {}
    for r in cur.fetchall():
        k = tuple(norm(r[i + 1]) for i in range(len(key_cols)))
        if any(part == "" for part in k):
            continue
        groups.setdefault(k, []).append((r[0], r[-1]))
    deleted = 0
    for k, members in groups.items():
        if len(members) < 2:
            continue
        # keep oldest captured_datetime (then lowest pk); delete the rest
        members.sort(key=lambda t: (t[1] is None, t[1], t[0]))
        keep = members[0][0]
        for pkv, _ in members[1:]:
            cur.execute(f"DELETE FROM {table} WHERE {pk}=%s", (pkv,))
            deleted += 1
    conn.commit()
    log(f"   {table} [{'+'.join(key_cols)}]: deduped, removed {deleted} rows")


# ===========================================================================
# Main
# ===========================================================================
def main() -> None:
    log("=" * 60)
    log(f"AGJEANS DATABASE CLEANUP  ({'DRY RUN' if DRY_RUN else 'LIVE RUN'})")
    log("=" * 60)

    corrections = load_corrections()
    remove_handles = load_remove_handles() if DO_REMOVE else []
    n_lit = sum(1 for c in corrections if not c.derive)
    n_der = sum(1 for c in corrections if c.derive)
    log(f"Corrections: {len(corrections)} spec rows ({n_lit} literal, {n_der} derived)")
    per_on = {}
    for c in corrections:
        per_on[tuple(c.tables)] = per_on.get(tuple(c.tables), 0) + 1
    for tabs, n in per_on.items():
        log(f"   -> {'+'.join(tabs):40} {n} rows")
    log(f"Remove handles: {len(remove_handles)}"
        + (": " + ", ".join(remove_handles) if remove_handles else ""))
    log(f"Derived corrections: {n_der} "
        f"({'WILL be applied' if DO_DERIVED else 'SKIPPED (DO_DERIVED off)'})")
    log(f"Missing insert: {'ON' if DO_INSERT_MISSING else 'off'} "
        f"({MISSING_STYLE['product_name']})")

    if DRY_RUN:
        log("DRY RUN - nothing written. Sample literal corrections:")
        shown = 0
        for c in corrections:
            if c.derive:
                continue
            log(f"   {'+'.join(c.tables):32} set {c.change_field}="
                f"{c.new_val!r:36} where {c.match_field}={c.match_val!r} "
                f"& {c.key_field}={c.key_val!r}")
            shown += 1
            if shown >= 10:
                break
        return

    deriver = None
    if DO_DERIVED or DO_INSERT_MISSING:
        deriver = build_deriver()   # stage 2

    import pymssql
    conn = pymssql.connect(server=SQL_SERVER, user=SQL_USERNAME, password=SQL_PASSWORD,
                           database=SQL_DATABASE, timeout=600, login_timeout=60)
    cur = conn.cursor()
    ckpt = load_checkpoint()
    if not RESET_PROGRESS and os.path.exists(CHECKPOINT_FILE):
        log(f"Resuming from checkpoint: {ckpt}")
    try:
        if DO_REMOVE:
            log("Phase 1: removals...")
            apply_removals(cur, conn, remove_handles, ckpt)
        if DO_INSERT_MISSING:
            log("Phase 1b: insert missing style (stage 2)...")
            insert_missing(cur, conn, deriver, ckpt)
        if DO_CORRECTIONS:
            log("Phase 2: corrections (value + key matched)...")
            apply_corrections(cur, conn, corrections, deriver, ckpt)
        if DO_DEDUPE:
            log("Phase 3: dedupe...")
            for table, keysets in DEDUPE_KEYS.items():
                for kc in keysets:
                    tag = f"{table}|{'+'.join(kc)}"
                    if ckpt["dedupe_done"].get(tag):
                        log(f"   {tag}: already deduped (checkpoint)")
                        continue
                    dedupe(cur, conn, table, kc, PK[table])
                    ckpt["dedupe_done"][tag] = True
                    save_checkpoint(ckpt)
        clear_checkpoint()
        log("Done - all phases committed. Checkpoint cleared.")
    except Exception:
        conn.rollback()
        log("ERROR - current step rolled back. Committed steps kept; re-run to resume.")
        raise
    finally:
        conn.close()


# ===========================================================================
# Stage 2 stubs (derivation + insert) - wired once validated
# ===========================================================================
def build_deriver():
    raise SystemExit("DO_DERIVED / DO_INSERT_MISSING are stage 2 and not wired yet. "
                     "Run stage 1 first (leave both False).")


def insert_missing(cur, conn, deriver, ckpt):
    raise SystemExit("insert_missing is stage 2 and not wired yet.")


if __name__ == "__main__":
    main()
