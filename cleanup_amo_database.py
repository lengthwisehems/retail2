# -*- coding: utf-8 -*-
"""
AMO database cleanup — apply the correction workbook to denim_analytics so the
database matches amo_inventory.py (commit 9ca26a5c).

ORDER
-----
  Phase 1  INSERT rows missing from style_info / style_metrics (styles that
           exist in lookup but not in style_info), built from the AMO scraper
           output CSVs + lookup identity + amo_inventory.py derivation.
  Phase 2  APPLY corrections from the workbook (ACTION=Remove -> delete;
           NEW cell -> literal / "Use amo_inventory.py" derive / blank keep).
  Phase 3  DEDUPE lookup & style_info, keeping the corrected row.

Derivation ("Use amo_inventory.py"): the real scraper functions are imported and
its post-processing passes run over the UNION of the corrected style_info rows
and the missing styles, keyed by HANDLE (style-level fields are the same across
a product's colors/sizes). Validated: 100% vs the Correct tab's active items,
and 541/541 + 95/95 resolution of the workbook's "Use amo_inventory.py" cells.

Inputs
------
  CORR_WORKBOOK   per-table tabs with current cols + NEW cols + ACTION
  SCRAPER_PATH    amo_inventory.py @ commit 9ca26a5c
  AMO_OUTPUT_DIR  folder of AMO_YYYYMMDD_HHMMSS.csv scraper outputs (blob)

SAFETY: DRY_RUN=True prints the full plan and writes nothing. Apply runs in one
transaction, rolls back on any error. Every log line is Central-time stamped.
Ids/barcodes are written as full-precision strings (repairs Excel sci-notation).
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

CORR_WORKBOOK  = r"AMO_DB_Values_20260813_151403_Claude.xlsx"
SCRAPER_PATH   = r"amo_inventory.py"
AMO_OUTPUT_DIR = r"AMO_Output"

SQL_SERVER   = os.environ.get("SQL_SERVER",   "denim-sql.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "denim_analytics")
SQL_USERNAME = os.environ.get("SQL_USERNAME", "")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

DERIVED_FIELDS = {"style_name", "jean_style", "rise_label",
                  "inseam_label", "inseam_style"}

PK = {"lookup": "lookup_id", "style_info": "style_info_id",
      "style_metrics": "style_metric_id", "variant_metrics": "variant_metric_id"}


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
    """DB-safe full-precision string (no scientific notation)."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, Decimal):
        return f"{v:f}"
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else repr(v)
    return str(v).strip()


def norm(v) -> str:
    return re.sub(r"\s+", " ", s(v)).strip().lower()


def is_use_scraper(v) -> bool:
    return isinstance(v, str) and "amo_inventory" in v.lower()


def is_blank(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def resolve(path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(SCRIPT_DIR, path)


def corr_val(row: dict, col: str) -> str:
    """Corrected value of a workbook column: NEW literal wins; else current."""
    nv = row.get(f"NEW {col}")
    if is_use_scraper(nv) or is_blank(nv):
        return s(row.get(col))
    return s(nv)


# ===========================================================================
# Loaders
# ===========================================================================
def load_scraper(path: str):
    p = resolve(path)
    if not os.path.exists(p):
        sys.exit(f"ERROR: scraper not found at {p}")
    spec = importlib.util.spec_from_file_location("amo_inventory", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod


def load_tab(path: str, sheet: str) -> List[dict]:
    from openpyxl import load_workbook
    wb = load_workbook(resolve(path), data_only=True, read_only=True)
    ws = wb[sheet]
    it = ws.iter_rows(values_only=True)
    hdr = list(next(it))
    out = []
    for r in it:
        d = {hdr[i]: r[i] for i in range(len(hdr)) if hdr[i]}
        if any(v not in (None, "") for v in d.values()):
            out.append(d)
    return out


def build_output_index(output_dir: str) -> Dict[str, dict]:
    """handle -> richest/most-recent AMO scraper-output row (style-level data)."""
    idx: Dict[str, Tuple[str, dict]] = {}
    d = resolve(output_dir)
    if not os.path.isdir(d):
        log(f"WARNING: AMO_OUTPUT_DIR not found ({d}); cannot insert missing rows.")
        return {}
    files = sorted(glob.glob(os.path.join(d, "*.csv")))
    for f in files:
        m = re.search(r"AMO_(\d{8}_\d{6})", os.path.basename(f))
        date = m.group(1) if m else "00000000_000000"
        try:
            for r in csv.DictReader(open(f, encoding="utf-8-sig")):
                h = (r.get("Handle") or "").strip().lower()
                if not h:
                    continue
                prev = idx.get(h)
                dl = len(r.get("Description") or "")
                if (prev is None or date > prev[0]
                        or (date == prev[0] and dl > len(prev[1].get("Description") or ""))):
                    idx[h] = (date, r)
        except Exception as exc:
            log(f"(could not read {os.path.basename(f)}: {exc})")
    log(f"Output index: {len(idx)} handles across {len(files)} file(s)")
    return {h: r for h, (_, r) in idx.items()}


# ===========================================================================
# Derivation engine (keyed by handle)
# ===========================================================================
def _title_of(product_name: str, color: str) -> str:
    cd = color.replace("-", " ")
    if cd and product_name.endswith(" / " + cd):
        return product_name[: -(len(cd) + 3)].strip()
    return product_name


class Deriver:
    """Runs amo_inventory.py derivation + post-processing over the union of the
    corrected style_info rows and the missing styles. Answers by handle."""

    def __init__(self, g, si_rows: List[dict], missing: Dict[str, dict]):
        self.g = g
        self.by_handle: Dict[str, dict] = {}
        self._build(si_rows, missing)

    def _build(self, si_rows, missing):
        g = self.g
        WH = g.WORK_HEADERS
        work, handles = [], []

        def add(title, desc, tags, handle, rise, inseam, leg):
            ss = g.derive_style_name_base(title)
            js = g.derive_jean_style(title, desc, tags, leg)
            rl = g.derive_rise_label(title, desc, rise)
            il = g.derive_inseam_label(js, inseam, title, handle, "", desc, False)
            isy = g.derive_inseam_style(js, inseam, rise, il, desc, tags)
            row = [""] * len(WH)
            for k, v in (("Style Name", ss), ("Style Name Source", ss),
                         ("Description", desc), ("Tags", tags), ("Leg Opening", leg),
                         ("Rise", rise), ("Inseam", inseam), ("Jean Style", js),
                         ("Rise Label", rl), ("Inseam Label", il), ("Inseam Style", isy)):
                g._set(row, k, v)
            work.append(row)
            handles.append(handle.lower())

        for d in si_rows:
            if s(d.get("ACTION")).lower() == "remove":
                continue
            pn = corr_val(d, "product_name")
            color = corr_val(d, "color")
            add(_title_of(pn, color), s(d.get("description")), s(d.get("tags")),
                s(d.get("handle")), s(d.get("rise")), s(d.get("inseam")),
                s(d.get("leg_opening")))
        for h, r in missing.items():
            prod = (r.get("Product") or "")
            base = prod.split(" / ")[0] if " / " in prod else prod
            add(g.product_title_for_product_field(base),
                (r.get("Description") or "").strip(), (r.get("Tags") or "").strip(),
                h, (r.get("Rise") or "").strip(), (r.get("Inseam") or "").strip(),
                (r.get("Leg Opening") or "").strip())

        g.apply_jean_style_sibling_inference(work)
        g.apply_style_name_rules(work)
        g.apply_rise_label_steps(work)
        g.apply_inseam_refresh(work)
        for h, w in zip(handles, work):
            self.by_handle[h] = {
                "style_name":   g._col(w, "Style Name"),
                "jean_style":   g._col(w, "Jean Style"),
                "rise_label":   g._col(w, "Rise Label"),
                "inseam_label": g._col(w, "Inseam Label"),
                "inseam_style": g._col(w, "Inseam Style"),
            }

    def get(self, field: str, handle: str) -> str:
        return self.by_handle.get(handle.lower(), {}).get(field, "")


# ===========================================================================
# Correction planning (pure)
# ===========================================================================
def corrected_updates(row: dict, deriver: Deriver) -> Dict[str, str]:
    updates: Dict[str, str] = {}
    handle = s(row.get("handle"))
    for hdr, val in row.items():
        if not (isinstance(hdr, str) and hdr.startswith("NEW ")):
            continue
        col = hdr[4:]
        cur = row.get(col)
        if is_blank(val):
            continue
        if is_use_scraper(val):
            if col not in DERIVED_FIELDS:
                continue
            new = deriver.get(col, handle)
            if new and norm(new) != norm(cur):
                updates[col] = new
            continue
        new = s(val)
        if norm(new) != norm(cur):
            updates[col] = new
    return updates


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

    tabs = {sheet: load_tab(CORR_WORKBOOK, sheet)
            for sheet in ("lookup", "style_info", "style_metrics", "variant_metrics")}
    for sheet, rows in tabs.items():
        log(f"{sheet}: {len(rows)} rows loaded")

    # --- determine missing styles (in lookup Keep, not in style_info) ------
    si_handles = {s(d.get("handle")).lower() for d in tabs["style_info"]}
    missing_handles = sorted(
        {s(d.get("handle")).lower() for d in tabs["lookup"]
         if s(d.get("ACTION")).lower() != "remove"} - si_handles)
    missing = {h: output_idx[h] for h in missing_handles if h in output_idx}
    no_data = [h for h in missing_handles if h not in output_idx]
    log(f"Missing styles: {len(missing_handles)} "
        f"(with output data: {len(missing)}, no data: {len(no_data)})")
    if no_data:
        log("   NO OUTPUT DATA for: " + ", ".join(no_data[:15]))

    deriver = Deriver(g, tabs["style_info"], missing)
    log(f"Derivation map: {len(deriver.by_handle)} handles")

    # --- plan corrections --------------------------------------------------
    plan_updates: Dict[str, List[Tuple[object, Dict[str, str]]]] = {t: [] for t in tabs}
    plan_removes: Dict[str, List[object]] = {t: [] for t in tabs}
    for sheet, rows in tabs.items():
        for row in rows:
            key = (s(row.get("sku_shopify")) if sheet == "variant_metrics"
                   else row.get(PK[sheet]))
            if s(row.get("ACTION")).lower() == "remove":
                plan_removes[sheet].append(key)
                continue
            ups = corrected_updates(row, deriver)
            if ups:
                plan_updates[sheet].append((key, ups))

    # --- plan inserts ------------------------------------------------------
    inserts = plan_inserts(tabs["lookup"], missing, deriver, output_idx)

    # --- preview -----------------------------------------------------------
    log("-" * 60)
    log(f"Phase 1 INSERT: style_info +{len(inserts['style_info'])}, "
        f"style_metrics +{len(inserts['style_metrics'])}")
    for sheet in tabs:
        nf = sum(len(u) for _, u in plan_updates[sheet])
        log(f"Phase 2 {sheet:16} remove={len(plan_removes[sheet]):>5} "
            f"update_rows={len(plan_updates[sheet]):>5} field_writes={nf}")
    log("Phase 3 dedupe: lookup + style_info (keep corrected) — runs on live DB")

    if DRY_RUN:
        log("Sample derived style_names for missing styles:")
        for h in list(missing)[:8]:
            log(f"   {h:34} -> {deriver.get('style_name', h)!r}")
        log("DRY RUN complete — nothing written.")
        return

    # --- apply -------------------------------------------------------------
    import pymssql
    conn = pymssql.connect(server=SQL_SERVER, user=SQL_USERNAME, password=SQL_PASSWORD,
                           database=SQL_DATABASE, timeout=600, login_timeout=60)
    cur = conn.cursor(as_dict=True)
    corrected_pks = {"lookup": set(), "style_info": set()}
    try:
        if DO_INSERT_MISSING:
            log("Phase 1: inserting missing rows...")
            do_inserts(cur, inserts)
        if DO_CORRECTIONS:
            log("Phase 2: applying corrections...")
            apply_corrections(cur, plan_updates, plan_removes, corrected_pks)
        if DO_DEDUPE:
            log("Phase 3: dedupe (keep corrected)...")
            dedupe(cur, "lookup", ["brand", "sku_shopify"], "lookup_id", corrected_pks["lookup"])
            dedupe(cur, "style_info", ["brand", "product_name"], "style_info_id", corrected_pks["style_info"])
        conn.commit()
        log("Committed.")
    except Exception:
        conn.rollback()
        log("ERROR — rolled back, database unchanged.")
        raise
    finally:
        conn.close()


# ===========================================================================
# Insert planning / execution
# ===========================================================================
def plan_inserts(lookup_rows, missing, deriver, output_idx) -> Dict[str, List[dict]]:
    """Build style_info + style_metrics rows for each missing colorway."""
    colorways: Dict[Tuple[str, str], dict] = {}
    for d in lookup_rows:
        if s(d.get("ACTION")).lower() == "remove":
            continue
        h = s(d.get("handle")).lower()
        if h not in missing:
            continue
        color = corr_val(d, "color")
        colorways.setdefault((h, color), d)   # one representative lookup row

    si_rows, sm_rows = [], []
    for (h, color), lk in colorways.items():
        out = missing[h]
        der = deriver.by_handle.get(h, {})
        now = dt.datetime.now(_TZ).replace(tzinfo=None)
        cap_date = _out_date(out)
        base = {
            "brand": BRAND,
            "style_id": corr_val(lk, "style_id") or s(out.get("Style Id")),
            "product_name": corr_val(lk, "product_name") or s(out.get("Product")),
            "handle": s(lk.get("handle")),
            "sku_url": s(lk.get("sku_url")) or s(out.get("SKU URL")),
            "color": color,
            "style_name": der.get("style_name", ""),
            "source_file_name": "AMO_cleanup_insert",
            "captured_date": cap_date, "captured_datetime": cap_date,
        }
        si_rows.append({**base,
            "jean_style": der.get("jean_style", ""),
            "rise_label": der.get("rise_label", ""),
            "inseam_label": der.get("inseam_label", ""),
            "inseam_style": der.get("inseam_style", ""),
            "product_type": s(out.get("Product Type")),
            "description": (out.get("Description") or "").strip(),
            "tags": (out.get("Tags") or "").strip(),
            "vendor": s(out.get("Vendor")),
            "image_url": s(out.get("Image URL")),
            "rise": _num(out.get("Rise")), "back_rise": _num(out.get("Back Rise")),
            "inseam": _num(out.get("Inseam")), "leg_opening": _num(out.get("Leg Opening")),
            "created_at": _date(out.get("Published At")),
            "is_manual_override": 0})
        sm_rows.append({**base,
            "price": _num(out.get("Price")),
            "compare_at_price": _num(out.get("Compare at Price")),
            "quantity_of_style": _int(out.get("Quantity of style")),
            "created_at": _date(out.get("Published At")),
            "published_at": _date(out.get("Published At"))})
    return {"style_info": si_rows, "style_metrics": sm_rows}


def do_inserts(cur, inserts):
    for table in ("style_info", "style_metrics"):
        rows = inserts[table]
        for r in rows:
            cols = [c for c, v in r.items() if v != "" and v is not None]
            placeholders = ", ".join("%s" for _ in cols)
            cur.execute(f"INSERT INTO {table} ({', '.join('['+c+']' for c in cols)}) "
                        f"VALUES ({placeholders})", [r[c] for c in cols])
        log(f"   inserted {len(rows)} rows into {table}")


def _num(v) -> str:
    t = s(v).replace("$", "").replace(",", "").strip()
    return t if re.fullmatch(r"-?\d+(\.\d+)?", t or "") else ""


def _int(v) -> str:
    t = _num(v)
    return str(int(float(t))) if t else ""


def _date(v) -> str:
    t = s(v)
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(t, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return ""


def _out_date(out) -> dt.datetime:
    return dt.datetime.now(_TZ).replace(tzinfo=None)


# ===========================================================================
# Correction execution
# ===========================================================================
def apply_corrections(cur, plan_updates, plan_removes, corrected_pks):
    for sheet, removes in plan_removes.items():
        for key in removes:
            if sheet == "variant_metrics":
                cur.execute("DELETE FROM variant_metrics WHERE brand=%s AND sku_shopify=%s",
                            (BRAND, key))
            else:
                cur.execute(f"DELETE FROM {sheet} WHERE {PK[sheet]}=%s", (key,))
        if removes:
            log(f"   {sheet}: removed {len(removes)}")
    for sheet, ups in plan_updates.items():
        for key, cols in ups:
            assigns = ", ".join(f"[{c}]=%s" for c in cols)
            vals = list(cols.values())
            if sheet == "variant_metrics":
                cur.execute(f"UPDATE variant_metrics SET {assigns} "
                            f"WHERE brand=%s AND sku_shopify=%s", vals + [BRAND, key])
            else:
                cur.execute(f"UPDATE {sheet} SET {assigns} WHERE {PK[sheet]}=%s",
                            vals + [key])
                if sheet in corrected_pks:
                    corrected_pks[sheet].add(key)
        if ups:
            log(f"   {sheet}: updated {len(ups)}")


def dedupe(cur, table, key_cols, pk, corrected_pks):
    """Delete duplicate rows per key group, keeping a corrected row if present,
    else the highest pk."""
    cur.execute(f"SELECT {pk} AS pk, {', '.join(key_cols)} FROM {table} WHERE brand=%s",
                (BRAND,))
    groups: Dict[tuple, List[int]] = {}
    for r in cur.fetchall():
        k = tuple(norm(r[c]) for c in key_cols)
        groups.setdefault(k, []).append(r["pk"])
    deleted = 0
    for k, pks in groups.items():
        if len(pks) < 2:
            continue
        keep = next((p for p in sorted(pks, reverse=True) if p in corrected_pks), max(pks))
        for p in pks:
            if p != keep:
                cur.execute(f"DELETE FROM {table} WHERE {pk}=%s", (p,))
                deleted += 1
    log(f"   {table}: deduped, removed {deleted} duplicate rows")


if __name__ == "__main__":
    main()
