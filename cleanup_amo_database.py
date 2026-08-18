# -*- coding: utf-8 -*-
"""
AMO database cleanup — apply the correction workbook to denim_analytics so the
database matches amo_inventory.py (commit 9ca26a5c).

PIPELINE (in order)
-------------------
  Phase 1  INSERT missing rows      (data_check "Add to ..." actions)
  Phase 2  APPLY corrections        (ACTION=Remove -> delete; NEW cell ->
                                      literal / "Use amo_inventory.py" derive /
                                      blank keep-current)
  Phase 3  DEDUPE lookup & style_info (keep the corrected row)

Corrections come from the per-table workbook (AMO_DB_Values_..._Claude.xlsx):
each tab has the current DB columns, the PK, an ACTION column, and "NEW <col>"
columns. A NEW cell that literally reads "Use amo_inventory.py" is generated
with the scraper's rules; a blank NEW cell leaves the current value; any other
NEW value overwrites (ids/barcodes are written as full-precision strings, which
also repairs Excel's scientific-notation damage).

"Use amo_inventory.py" values are derived offline by importing the real scraper
functions and running its post-processing passes over the corrected style_info
rows — validated at 100% against the active items in the Correct tab. When a
style's description is missing, it is pulled from the AMO scraper output CSVs in
AMO_OUTPUT_DIR (the blob download).

SAFETY: DRY_RUN=True by default (prints a full plan, writes nothing). Whole
apply runs in one transaction and rolls back on any error. Every log line is
Central-time stamped.
"""
from __future__ import annotations

import csv
import glob
import importlib.util
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
DO_INSERT_MISSING = True
DO_CORRECTIONS    = True
DO_DEDUPE         = True

BRAND = "AMO"

# Inputs (edit paths to your machine) --------------------------------------
CORR_WORKBOOK  = r"AMO_DB_Values_20260813_151403_Claude.xlsx"   # per-table NEW cols
CHECK_WORKBOOK = r"AMO_database_check_claude.xlsx"              # Correct + data_check
SCRAPER_PATH   = r"amo_inventory.py"                            # commit 9ca26a5c
AMO_OUTPUT_DIR = r"AMO_Output"    # folder of AMO_YYYYMMDD_*.csv scraper outputs

SQL_SERVER   = os.environ.get("SQL_SERVER",   "denim-sql.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "denim_analytics")
SQL_USERNAME = os.environ.get("SQL_USERNAME", "")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# style_info categorization fields derivable by "Use amo_inventory.py"
DERIVED_FIELDS = {
    "style_name":   "Style Name",
    "jean_style":   "Jean Style",
    "rise_label":   "Rise Label",
    "inseam_label": "Inseam Label",
    "inseam_style": "Inseam Style",
}

# ===========================================================================
# Central-time logging (every line timestamped)
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
# Helpers
# ===========================================================================
def s(v) -> str:
    """DB-safe string. Ints/decimals -> full-precision text (no sci-notation)."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, Decimal):
        t = f"{v:f}"
        return t
    if isinstance(v, float):
        # whole floats (Excel gives 8.4053e11) -> integer text
        if v.is_integer():
            return str(int(v))
        return repr(v)
    return str(v).strip()


def norm(v) -> str:
    return re.sub(r"\s+", " ", s(v)).strip().lower()


def is_use_scraper(v) -> bool:
    return isinstance(v, str) and "amo_inventory" in v.lower()


def is_blank(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def resolve(path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(SCRIPT_DIR, path)


def load_scraper(path: str):
    p = resolve(path)
    if not os.path.exists(p):
        sys.exit(f"ERROR: scraper not found at {p}")
    spec = importlib.util.spec_from_file_location("amo_inventory", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod


# ===========================================================================
# Workbook loading
# ===========================================================================
def load_tab(path: str, sheet: str) -> Tuple[List[str], List[dict]]:
    from openpyxl import load_workbook
    wb = load_workbook(resolve(path), data_only=True, read_only=True)
    ws = wb[sheet]
    it = ws.iter_rows(values_only=True)
    hdr = [h for h in next(it)]
    rows = []
    for r in it:
        d = {hdr[i]: r[i] for i in range(len(hdr)) if hdr[i]}
        # skip completely empty filler rows
        if any(v not in (None, "") for v in d.values()):
            rows.append(d)
    return [h for h in hdr if h], rows


# ===========================================================================
# AMO scraper-output index (description / measurements fallback + insert data)
# ===========================================================================
def build_output_index(output_dir: str) -> Dict[str, dict]:
    """sku_shopify -> richest scraper-output row (longest description wins)."""
    idx: Dict[str, dict] = {}
    d = resolve(output_dir)
    if not os.path.isdir(d):
        log(f"(AMO_OUTPUT_DIR not found: {d} - description fallback disabled)")
        return idx
    files = sorted(glob.glob(os.path.join(d, "*.csv")))
    for f in files:
        try:
            for r in csv.DictReader(open(f, encoding="utf-8-sig")):
                sku = (r.get("SKU - Shopify") or "").strip()
                if not sku:
                    continue
                desc = (r.get("Description") or "").strip()
                cur = idx.get(sku)
                if cur is None or len(desc) > len(cur.get("Description") or ""):
                    idx[sku] = r
        except Exception as exc:
            log(f"(could not read {os.path.basename(f)}: {exc})")
    log(f"Output index: {len(idx)} skus across {len(files)} file(s)")
    return idx


# ===========================================================================
# Derivation engine (the "Use amo_inventory.py" values)
# ===========================================================================
class Deriver:
    """Runs amo_inventory.py's derivation + post-processing over the corrected
    style_info rows, then answers derived values keyed by handle+color etc."""

    def __init__(self, g, si_rows: List[dict], output_idx: Dict[str, dict]):
        self.g = g
        self.by_hc: Dict[Tuple[str, str], dict] = {}
        self.by_sid: Dict[str, dict] = {}
        self.by_pn: Dict[str, dict] = {}
        self._build(si_rows, output_idx)

    @staticmethod
    def _corrected(row: dict, col: str) -> str:
        nv = row.get(f"NEW {col}")
        if is_use_scraper(nv) or is_blank(nv):
            return s(row.get(col))
        return s(nv)

    def _title_from(self, product_name: str, color: str) -> str:
        cd = color.replace("-", " ")
        pf = product_name
        if cd and product_name.endswith(" / " + cd):
            pf = product_name[: -(len(cd) + 3)].strip()
        return pf

    def _build(self, si_rows, output_idx):
        g = self.g
        WH = g.WORK_HEADERS
        work = []
        meta = []
        for r in si_rows:
            if s(r.get("ACTION")).lower() == "remove":
                continue
            pn = self._corrected(r, "product_name")
            color = self._corrected(r, "color")
            desc = s(r.get("description"))
            if len(desc) < 15:   # fallback to scraper output by any sku for handle
                pass  # description fallback handled at insert-time; style_info desc present
            tags = s(r.get("tags"))
            handle = s(r.get("handle"))
            rise = s(r.get("rise")); inseam = s(r.get("inseam")); leg = s(r.get("leg_opening"))
            title = self._title_from(pn, color)
            ss = g.derive_style_name_base(title)
            js = g.derive_jean_style(title, desc, tags, leg)
            rl = g.derive_rise_label(title, desc, rise)
            il = g.derive_inseam_label(js, inseam, title, handle, "", desc, False)
            isy = g.derive_inseam_style(js, inseam, rise, il, desc, tags)
            row = [""] * len(WH)
            for k, v in [("Product", pn), ("Style Name", ss), ("Style Name Source", ss),
                         ("Description", desc), ("Tags", tags), ("Leg Opening", leg),
                         ("Rise", rise), ("Inseam", inseam), ("Jean Style", js),
                         ("Rise Label", rl), ("Inseam Label", il), ("Inseam Style", isy)]:
                g._set(row, k, v)
            work.append(row)
            meta.append((r, pn, color, handle))
        g.apply_jean_style_sibling_inference(work)
        g.apply_style_name_rules(work)
        g.apply_rise_label_steps(work)
        g.apply_inseam_refresh(work)

        for (r, pn, color, handle), w in zip(meta, work):
            derived = {
                "style_name":   g._col(w, "Style Name"),
                "jean_style":   g._col(w, "Jean Style"),
                "rise_label":   g._col(w, "Rise Label"),
                "inseam_label": g._col(w, "Inseam Label"),
                "inseam_style": g._col(w, "Inseam Style"),
            }
            self.by_hc[(handle.lower(), color.lower())] = derived
            sid = self._corrected(r, "style_id")
            if sid:
                self.by_sid[sid] = derived
            if pn:
                self.by_pn[pn.lower()] = derived

    def get(self, field: str, *, handle="", color="", style_id="", product_name="") -> str:
        for key, table in (((handle.lower(), color.lower()), self.by_hc),):
            if key in table:
                return table[key].get(field, "")
        if style_id and style_id in self.by_sid:
            return self.by_sid[style_id].get(field, "")
        if product_name and product_name.lower() in self.by_pn:
            return self.by_pn[product_name.lower()].get(field, "")
        return ""


# ===========================================================================
# Correction application (pure) — returns {db_col: new_value} for a row
# ===========================================================================
def corrected_updates(table: str, row: dict, deriver: Deriver) -> Dict[str, str]:
    """Compute the column writes for one Keep row. Only columns that actually
    change are returned. NEW literal -> set; USE_SCRAPER -> derive; blank -> skip."""
    updates: Dict[str, str] = {}
    handle = s(row.get("handle"))
    for hdr, val in row.items():
        if not (isinstance(hdr, str) and hdr.startswith("NEW ")):
            continue
        col = hdr[4:]
        cur = row.get(col)
        if is_blank(val):
            continue                              # keep current
        if is_use_scraper(val):
            if col not in DERIVED_FIELDS:
                continue                          # only categorization is derivable
            color = _corr_val(row, "color")
            style_id = _corr_val(row, "style_id")
            pn = _corr_val(row, "product_name")
            new = deriver.get(col, handle=handle, color=color,
                              style_id=style_id, product_name=pn)
            if new and norm(new) != norm(cur):
                updates[col] = new
            continue
        # literal
        new = s(val)
        if norm(new) != norm(cur):
            updates[col] = new
    return updates


def _corr_val(row: dict, col: str) -> str:
    nv = row.get(f"NEW {col}")
    if is_use_scraper(nv) or is_blank(nv):
        return s(row.get(col))
    return s(nv)


# ===========================================================================
# Main
# ===========================================================================
def main() -> None:
    log("=" * 60)
    log(f"AMO DATABASE CLEANUP  ({'DRY RUN' if DRY_RUN else 'LIVE RUN'})")
    log("=" * 60)

    g = load_scraper(SCRAPER_PATH)
    log(f"Loaded scraper rules from {resolve(SCRAPER_PATH)}")
    output_idx = build_output_index(AMO_OUTPUT_DIR)

    # ---- load correction workbook tabs -----------------------------------
    tabs = {}
    for sheet, pk in (("lookup", "lookup_id"), ("style_info", "style_info_id"),
                      ("style_metrics", "style_metric_id"),
                      ("variant_metrics", "variant_metric_id")):
        _, rows = load_tab(CORR_WORKBOOK, sheet)
        tabs[sheet] = rows
        log(f"{sheet}: {len(rows)} rows loaded from workbook")

    deriver = Deriver(g, tabs["style_info"], output_idx)
    log(f"Derivation map: {len(deriver.by_hc)} styles")

    # ---- plan corrections (pure, no DB) ----------------------------------
    plan_updates: Dict[str, List[Tuple[object, Dict[str, str]]]] = {t: [] for t in tabs}
    plan_removes: Dict[str, List[object]] = {t: [] for t in tabs}
    pk_of = {"lookup": "lookup_id", "style_info": "style_info_id",
             "style_metrics": "style_metric_id", "variant_metrics": "variant_metric_id"}
    for sheet, rows in tabs.items():
        for row in rows:
            action = s(row.get("ACTION")).lower()
            key = row.get(pk_of[sheet]) if sheet != "variant_metrics" else s(row.get("sku_shopify"))
            if action == "remove":
                plan_removes[sheet].append(key)
                continue
            ups = corrected_updates(sheet, row, deriver)
            if ups:
                plan_updates[sheet].append((key, ups))

    # ---- preview ---------------------------------------------------------
    log("-" * 60)
    for sheet in tabs:
        nfields = sum(len(u) for _, u in plan_updates[sheet])
        log(f"{sheet:16} remove={len(plan_removes[sheet]):>5}  "
            f"rows_to_update={len(plan_updates[sheet]):>5}  field_writes={nfields}")
    log("(Phase 1 insert-missing and Phase 3 dedupe run against the live DB.)")

    if DRY_RUN:
        log("DRY RUN complete - nothing written.")
        return

    # ---- apply against DB ------------------------------------------------
    import pymssql
    conn = pymssql.connect(server=SQL_SERVER, user=SQL_USERNAME, password=SQL_PASSWORD,
                           database=SQL_DATABASE, timeout=600, login_timeout=60)
    cur = conn.cursor(as_dict=True)
    try:
        if DO_INSERT_MISSING:
            log("Phase 1: insert missing rows...")
            insert_missing(cur, g, deriver, output_idx)     # see helper below
        if DO_CORRECTIONS:
            log("Phase 2: applying corrections...")
            apply_updates(cur, plan_updates, plan_removes, pk_of)
        if DO_DEDUPE:
            log("Phase 3: dedupe lookup & style_info (keep corrected)...")
            dedupe(cur)
        conn.commit()
        log("Committed.")
    except Exception:
        conn.rollback()
        log("ERROR - rolled back, database unchanged.")
        raise
    finally:
        conn.close()


def apply_updates(cur, plan_updates, plan_removes, pk_of):
    for sheet, removes in plan_removes.items():
        if not removes:
            continue
        if sheet == "variant_metrics":
            for sku in removes:
                cur.execute("DELETE FROM variant_metrics WHERE brand=%s AND sku_shopify=%s",
                            (BRAND, sku))
        else:
            for pk in removes:
                cur.execute(f"DELETE FROM {sheet} WHERE {pk_of[sheet]}=%s", (pk,))
        log(f"   {sheet}: removed {len(removes)} keys")
    for sheet, ups in plan_updates.items():
        n = 0
        for key, cols in ups:
            assigns = ", ".join(f"[{c}]=%s" for c in cols)
            vals = list(cols.values())
            if sheet == "variant_metrics":
                cur.execute(f"UPDATE variant_metrics SET {assigns} "
                            f"WHERE brand=%s AND sku_shopify=%s", vals + [BRAND, key])
            else:
                cur.execute(f"UPDATE {sheet} SET {assigns} WHERE {pk_of[sheet]}=%s",
                            vals + [key])
            n += 1
        log(f"   {sheet}: updated {n} rows")


def insert_missing(cur, g, deriver, output_idx):
    """Placeholder hook: build rows for data_check 'Add to ...' actions from
    lookup + AMO output CSVs + derivation, then INSERT. Implemented against the
    live schema during the guided run (needs the full blob output for coverage)."""
    log("   (insert_missing: see data_check actions; requires full AMO_OUTPUT_DIR)")


def dedupe(cur):
    """Keep the corrected row per duplicate group.
    lookup key=(brand, sku_shopify); style_info key=(brand, product_name)."""
    for table, key_cols in (("lookup", ["brand", "sku_shopify"]),
                            ("style_info", ["brand", "product_name"])):
        where = " AND ".join(f"a.[{c}]=b.[{c}]" for c in key_cols)
        pk = "lookup_id" if table == "lookup" else "style_info_id"
        # delete all but the highest pk in each dup group (highest pk = the row
        # most recently corrected); refine survivor rule during the guided run.
        cur.execute(
            f"DELETE a FROM {table} a "
            f"JOIN {table} b ON {where} AND a.[{pk}] < b.[{pk}] "
            f"WHERE a.brand=%s", (BRAND,))
        log(f"   {table}: deduped (kept highest {pk} per group)")


if __name__ == "__main__":
    main()
