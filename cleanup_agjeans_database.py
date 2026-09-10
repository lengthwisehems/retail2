# -*- coding: utf-8 -*-
"""
AGJEANS database cleanup - the AMO cleanup, but rows are matched by VALUE
(current field value + handle / sku_shopify) instead of by *_id.

SOURCE OF TRUTH: the review workbook AGJEANS_DB_Values_..._Claude.xlsx, one tab
per table (style_info, lookup, style_metrics, variant_metrics). Each tab carries
the current columns, an ACTION column, and "NEW <col>" columns:

    ACTION = KEEP / "Keep and Add To style_info"  -> apply the NEW cells
    ACTION = REMOVE                               -> delete the row's item
    ACTION = ADD                                  -> insert (Farrah style_info)

For each "NEW <col>" cell:
    blank                         -> leave the current value
    a literal value               -> set it
    "USE agjeans_inventory ..."   -> DERIVE it with agjeans_inventory.py's rules
                                     (offline: build_product_title falls back to
                                     the handle, exactly as the scraper does when
                                     Constructor data is absent)

A correction becomes:
    UPDATE <tab's table>
       SET <col> = <new>, [is_manual_override = 1 for style_info]
     WHERE brand = 'AGJEANS'
       AND <col> = <current value>     (blank current -> match where blank)
       AND <handle | sku_shopify> = <the row's key>
All string predicates are varchar-cast so they seek (variant_metrics is ~24M).

PHASES: 1 REMOVE  2 ADD  3 CORRECT  4 DEDUPE (lookup & style_info, AMO keys).
is_manual_override is set TRUE on every style_info row we correct or add.

SAFETY: DRY_RUN prints the plan (and a derivation preview) and writes nothing.
Per-phase / per-N-row commits + checkpoint. Every log line is Central-time
stamped. DO_DERIVED gates the "USE agjeans_inventory" cells so you can eyeball
the derived values in a dry run before they are applied.
"""
from __future__ import annotations

import importlib.util
import json
import os
import re
import sys
import time
import types
import datetime as dt
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

# ===========================================================================
# CONFIG
# ===========================================================================
DRY_RUN = True
DO_REMOVE      = True
DO_ADD         = True
DO_PRE_DEDUPE  = True    # collapse duplicate style_info rows per handle FIRST
DO_CORRECTIONS = True
DO_DEDUPE      = True
DO_DERIVED     = True    # apply/preview "USE agjeans_inventory TO FILL" cells
RESET_PROGRESS = False

BRAND = "AGJEANS"

WORKBOOK     = r"AGJEANS_DB_Values_20260904_124324_Claude.xlsx"
SCRAPER_PATH = r"agjeans_inventory.py"     # the 74af9d5 rules

SQL_SERVER   = os.environ.get("SQL_SERVER",   "denim-sql.database.windows.net")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "denim_analytics")
SQL_USERNAME = os.environ.get("SQL_USERNAME", "carrieboromisa")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CHECKPOINT_FILE = os.path.join(SCRIPT_DIR, f"cleanup_{BRAND.lower()}_checkpoint.json")
COMMIT_EVERY = 500      # commit + log progress this often (visibility + safe resume)
SLOW_UPDATE_SEC = 20    # log a warning for any single correction slower than this
DELETE_BATCH = 5000
FILL_QAO_PK_WINDOW = 200000   # variant_metric_id span per fill_qao batch (clustered range)
VC = "CAST(%s AS varchar(255))"

TABS = ("style_info", "lookup", "style_metrics", "variant_metrics")
PK = {"lookup": "lookup_id", "style_info": "style_info_id",
      "style_metrics": "style_metric_id", "variant_metrics": "variant_metric_id"}

# fields whose NEW cell may say "USE agjeans_inventory TO FILL"
DERIVED_FIELDS = {"product_name", "style_name", "jean_style", "rise_label",
                  "inseam_label", "inseam_style", "variant_title"}
# a change to one of these is a per-variant fix keyed by sku_shopify; else handle
SKU_KEYED = {"variant_title", "quantity_available_online"}

# columns each table actually has (a correction only touches tables that have it)
TABLE_COLS = {
    "lookup": {"style_id", "product_name", "handle", "sku_url", "color",
               "style_name", "style_name_grouping", "variant_title", "size",
               "sku_shopify", "sku_brand", "barcode"},
    "style_info": {"is_manual_override", "style_id", "product_name", "handle",
                   "sku_url", "color", "style_name", "style_name_grouping",
                   "color_simplified", "color_standardized", "gender", "hem_style",
                   "inseam_label", "inseam_style", "jean_style", "product_line",
                   "product_type", "rise_label", "stretch", "back_rise", "inseam",
                   "knee", "leg_opening", "rise", "created_at", "description",
                   "image_url", "size_chart", "tags", "vendor", "country_produced",
                   "fabric_source"},
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


def norm(v) -> str:
    return re.sub(r"\s+", " ", s(v)).strip().lower()


def is_blank(v) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def is_use_scraper(v) -> bool:
    return isinstance(v, str) and "agjeans_inventory" in v.lower()


def _is_fill_qao(v) -> bool:
    """NEW quantity_available_online = 'FILL WITH THE VALUE CURRENTLY IN QUANTITY
    AVAILABLE' -> copy quantity_available into it (not a literal)."""
    n = norm(v)
    return "fill with" in n and "quantity available" in n


def resolve(path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(SCRIPT_DIR, path)


def key_field_for(col: str) -> str:
    return "sku_shopify" if col in SKU_KEYED else "handle"


# ===========================================================================
# Checkpoint
# ===========================================================================
def _empty_ckpt() -> dict:
    return {"removals": {}, "add_done": False, "pre_dedupe_done": False,
            "corrections": {}, "fill_qao_done": False,
            "fill_qao_pk": None, "fill_qao_total": 0, "dedupe_done": {}}


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
# Workbook
# ===========================================================================
def load_tab(sheet: str) -> Tuple[List[str], List[dict]]:
    from openpyxl import load_workbook
    wb = load_workbook(resolve(WORKBOOK), data_only=True, read_only=True)
    if sheet not in wb.sheetnames:
        return [], []
    ws = wb[sheet]
    it = ws.iter_rows(values_only=True)
    hdr = list(next(it))
    cols = [h for h in hdr if h]
    out = []
    for r in it:
        if all(v in (None, "") for v in r):
            continue
        out.append({hdr[i]: (r[i] if i < len(r) else None)
                    for i in range(len(hdr)) if hdr[i]})
    return cols, out


# ===========================================================================
# Derivation (agjeans_inventory.py rules, offline, handle fallback)
# ===========================================================================
def load_scraper():
    """Import agjeans_inventory.py with requests/bs4 stubbed so it loads offline
    (only the pure derivation functions are used)."""
    p = resolve(SCRAPER_PATH)
    if not os.path.exists(p):
        sys.exit(f"ERROR: scraper not found at {p}")
    req = types.ModuleType("requests")

    class _S:
        def __init__(self, *a, **k): self.headers = {}
        def mount(self, *a, **k): pass
    req.Session = lambda *a, **k: _S()
    ad = types.ModuleType("requests.adapters")
    ad.HTTPAdapter = type("H", (), {"__init__": lambda s, *a, **k: None})
    req.adapters = ad
    pk = types.ModuleType("requests.packages")
    u3 = types.ModuleType("urllib3"); u3.disable_warnings = lambda *a, **k: None
    uu = types.ModuleType("urllib3.util")
    ur = types.ModuleType("urllib3.util.retry")
    ur.Retry = type("R", (), {"__init__": lambda s, *a, **k: None})
    uu.retry = ur; u3.util = uu; pk.urllib3 = u3; req.packages = pk
    b = types.ModuleType("bs4"); b.BeautifulSoup = lambda *a, **k: None
    for name, mod in (("requests", req), ("requests.adapters", ad),
                      ("requests.packages", pk), ("requests.packages.urllib3", u3),
                      ("urllib3", u3), ("urllib3.util", uu),
                      ("urllib3.util.retry", ur), ("bs4", b)):
        sys.modules.setdefault(name, mod)
    spec = importlib.util.spec_from_file_location("agjeans_inventory", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod


def _handle_title(handle: str) -> str:
    """Title-ish text from the handle (drop the trailing style code), so
    build_product_title keeps the leftover NAME instead of dropping it."""
    base = handle.rsplit("-", 1)[0] if "-" in handle else handle
    return base.replace("-", " ").strip()


class Deriver:
    """Runs agjeans_inventory.py's rules over the style_info rows, keyed by
    handle, using the handle fallback for the product title (offline). Fills the
    scraper work-row and runs its post-processing passes, then answers by handle
    for style-level fields; variant_title is rebuilt as Style Name + the current
    variant suffix."""

    def __init__(self, g, si_rows: List[dict], sku2handle: Dict[str, str] = None):
        self.g = g
        self.by_handle: Dict[str, dict] = {}
        self.sku2handle = sku2handle or {}
        self._build(si_rows)

    def _first_nonblank(self, rows, col):
        for r in rows:
            v = r.get(col)
            if not is_blank(v):
                return s(v)
        return ""

    def _build(self, si_rows):
        g = self.g
        # one representative (richest description) row per handle
        rep: Dict[str, dict] = {}
        for d in si_rows:
            if norm(d.get("ACTION")) == "remove":
                continue
            h = norm(d.get("handle"))
            if not h:
                continue
            prev = rep.get(h)
            if prev is None or len(s(d.get("description"))) > len(s(prev.get("description"))):
                rep[h] = d
        work: List[dict] = []
        handles: List[str] = []
        for h, d in rep.items():
            title = _handle_title(h)
            desc = s(d.get("description"))
            tags = s(d.get("tags"))
            color = s(d.get("color"))
            rise = s(d.get("rise")); knee = s(d.get("knee"))
            inseam = s(d.get("inseam")); leg = s(d.get("leg_opening"))
            size = s(d.get("size")) if "size" in d else ""
            product_base = g.build_product_title(title, "", "", h)[0]
            product_base = _titlecase(product_base)
            style_name = g.build_style_name(h)
            # Seed Jean Style / Rise / Inseam Label from the workbook's LITERAL
            # correction when it has one, else derive. Style Name's one-word rule
            # (apply_style_name_rules) appends the first word of Jean Style, so
            # the FINAL Jean Style has to be in place before that pass runs.
            def _lit(col, derived):
                nv = d.get("NEW " + col)
                return s(nv) if (not is_use_scraper(nv) and not is_blank(nv)) else derived
            jean_style = _lit("jean_style",
                              g.jean_style_from_source(product_base, leg)
                              or g.jean_style_from_source(title, leg))
            rise_label = _lit("rise_label", g.determine_rise_label(product_base, desc, h, rise))
            inseam_label = _lit("inseam_label", g.determine_inseam_label(product_base, desc, size))
            product_display = f"{product_base} - {color.title()}" if color else product_base
            row = {
                "Handle": h, "Product": product_display, "Style Name": style_name,
                "Jean Style": jean_style, "Inseam Style": "", "Rise Label": rise_label,
                "Inseam Label": inseam_label, "Description": desc, "Tags": tags,
                "Rise": rise, "Knee": knee, "Inseam": inseam, "Leg Opening": leg,
                "_product_base": product_base, "_color": color,
            }
            work.append(row); handles.append(h)
        # scraper post-processing passes (sibling/one-word rules)
        for fn in ("apply_jean_style_by_style_name", "apply_style_name_rules",
                   "apply_jean_style_by_style_name", "apply_inseam_style",
                   "apply_rise_label_fallbacks"):
            f = getattr(g, fn, None)
            if f:
                try:
                    f(work)
                except Exception as exc:
                    log(f"(derivation pass {fn} skipped: {exc})")
        for h, w in zip(handles, work):
            # product_name = base + color (rebuild from FINAL nothing changes here)
            self.by_handle[h] = {
                "product_name": w["Product"],
                "style_name": w["Style Name"],
                "jean_style": w["Jean Style"],
                "rise_label": w["Rise Label"],
                "inseam_label": w["Inseam Label"],
                "inseam_style": w["Inseam Style"],
            }

    def get(self, field: str, key_val, key_field: str, current) -> str:
        if field == "variant_title":
            # Variant Title = derived Style Name + the current variant suffix,
            # mirroring the scraper's apply_variant_titles (Style Name + title).
            h = norm(self.sku2handle.get(s(key_val), ""))
            sn = self.by_handle.get(h, {}).get("style_name", "")
            cur = s(current)
            suffix = cur.split(" - ", 1)[1] if " - " in cur else cur
            if not sn or not suffix:
                return ""
            return f"{sn} - {suffix}".strip(" -")
        h = norm(key_val) if key_field == "handle" else None
        if h and h in self.by_handle:
            return self.by_handle[h].get(field, "")
        return ""


def _titlecase(t: str) -> str:
    return " ".join(w if (w.isupper() and len(w) > 1) else w.capitalize()
                    for w in t.split())


# ===========================================================================
# Build the plan from the workbook
# ===========================================================================
class Correction:
    __slots__ = ("table", "change_field", "match_val", "key_field", "key_val",
                 "new_val", "derive", "handle")

    def __init__(self, table, cf, mv, kf, kv, nv, derive, handle):
        self.table = table; self.change_field = cf; self.match_val = mv
        self.key_field = kf; self.key_val = kv; self.new_val = nv
        self.derive = derive; self.handle = handle


def build_plan():
    corrections: List[Correction] = []
    remove_handles = set()
    remove_skus = set()
    add_rows: List[dict] = []
    si_rows_for_derive: List[dict] = []
    sku2handle: Dict[str, str] = {}
    fill_qao = False
    for tab in TABS:
        cols, rows = load_tab(tab)
        if not rows:
            continue
        if tab == "style_info":
            si_rows_for_derive = rows
        if tab == "lookup":
            for r in rows:
                sk, h = s(r.get("sku_shopify")), s(r.get("handle"))
                if sk and h:
                    sku2handle[sk] = h
        new_cols = [c for c in cols if isinstance(c, str) and c.lower().startswith("new ")]
        for r in rows:
            action = norm(r.get("ACTION"))
            handle = s(r.get("handle"))
            if action == "remove":
                if tab == "variant_metrics":
                    sk = s(r.get("sku_shopify"))
                    if sk:
                        remove_skus.add(sk)
                elif handle:
                    remove_handles.add(handle.lower())
                continue
            if action == "add":
                if tab == "style_info":
                    add_rows.append(r)
                continue
            # KEEP / "keep and add to style_info" -> apply NEW cells
            for nc in new_cols:
                col = nc[4:].strip()
                col_db = re.sub(r"\s+", "_", col.lower())
                if col_db not in TABLE_COLS.get(tab, set()):
                    continue
                nv = r.get(nc)
                if is_blank(nv):
                    continue
                if _is_fill_qao(nv):
                    fill_qao = True     # set quantity_available_online = quantity_available
                    continue            # handled once, set-based, for all AGJEANS
                derive = is_use_scraper(nv)
                if derive and col_db not in DERIVED_FIELDS:
                    continue
                if not derive and norm(nv) == norm(r.get(col_db)):
                    continue    # NEW == current -> no change to make
                kf = key_field_for(col_db)
                kv = s(r.get(kf))
                if not kv:
                    continue
                corrections.append(Correction(
                    table=tab, cf=col_db, mv=r.get(col_db), kf=kf, kv=kv,
                    nv=("" if derive else s(nv)), derive=derive, handle=handle))
    return {"corrections": corrections, "remove_handles": sorted(remove_handles),
            "remove_skus": sorted(remove_skus), "add_rows": add_rows,
            "si_rows": si_rows_for_derive, "sku2handle": sku2handle,
            "fill_qao": fill_qao}


# ===========================================================================
# SQL helpers
# ===========================================================================
def _match_clause(field: str, val) -> Tuple[str, list]:
    if is_blank(val):
        return f"([{field}] IS NULL OR LTRIM(RTRIM([{field}])) = '')", []
    return f"[{field}] = {VC}", [s(val)]


def _delete_each_value(cur, conn, table, col, values) -> int:
    total = 0
    for v in values:
        while True:
            cur.execute(f"DELETE TOP ({DELETE_BATCH}) FROM {table} "
                        f"WHERE brand={VC} AND [{col}]={VC}", (BRAND, v))
            rc = cur.rowcount if (cur.rowcount and cur.rowcount > 0) else 0
            conn.commit(); total += rc
            if rc < DELETE_BATCH:
                break
    return total


# ===========================================================================
# Phases
# ===========================================================================
def apply_removals(cur, conn, handles, skus, ckpt):
    done = ckpt.setdefault("removals", {})
    if handles and not done.get("variant_metrics_by_handle"):
        vm_skus = set(skus)
        for h in handles:
            cur.execute(f"SELECT sku_shopify FROM lookup WHERE brand={VC} AND handle={VC}",
                        (BRAND, h))
            vm_skus.update(s(r[0]) for r in cur.fetchall() if s(r[0]))
        n = _delete_each_value(cur, conn, "variant_metrics", "sku_shopify", sorted(vm_skus))
        done["variant_metrics_by_handle"] = True
        save_checkpoint(ckpt)
        log(f"   variant_metrics: removed {n} rows for {len(vm_skus)} skus (committed)")
    for tbl in ("lookup", "style_info", "style_metrics"):
        if done.get(tbl):
            continue
        n = _delete_each_value(cur, conn, tbl, "handle", handles)
        done[tbl] = True
        save_checkpoint(ckpt)
        log(f"   {tbl}: removed {n} rows for {len(handles)} handles (committed)")


def apply_fill_qao(cur, conn, ckpt):
    """Set quantity_available_online = quantity_available for every AGJEANS
    variant_metrics row.

    variant_metrics is ~24M rows, so we must NOT use
        UPDATE TOP (N) ... WHERE brand=AGJEANS AND qao <> qa
    That WHERE empties itself as we go: once the first rows are fixed they no
    longer match, so each pass has to scan PAST all the already-fixed rows to
    find the next N that still differ. After a big partial run (e.g. a WAN drop
    at ~1.275M rows) the very first resumed batch has to scan through all of
    those millions of now-matching rows before finding any work, and that single
    statement blows past the query timeout - which is exactly the "first batch
    of the resumed run times out with no progress logged" failure.

    Instead we walk the CLUSTERED primary key (variant_metric_id) in fixed
    id-windows. Each window is a bounded clustered range read - constant work no
    matter how many rows are already fixed - and the last finished window's high
    id is checkpointed, so a dropped connection resumes at the next window
    instead of rescanning from the front. Still idempotent: the qao<>qa residual
    inside each window means already-correct rows aren't rewritten."""
    if ckpt.get("fill_qao_done"):
        log("   fill_qao: already done (checkpoint)")
        return
    # clustered PK endpoints are index bounds -> instant, whole table
    cur.execute("SELECT MIN(variant_metric_id), MAX(variant_metric_id) "
                "FROM variant_metrics")
    lo, hi = (cur.fetchone() or (None, None))
    if lo is None:
        ckpt["fill_qao_done"] = True
        save_checkpoint(ckpt)
        log("   fill_qao: variant_metrics is empty - nothing to do")
        return
    start = ckpt.get("fill_qao_pk")
    cur_lo = (start + 1) if isinstance(start, int) else lo
    total = ckpt.get("fill_qao_total", 0) or 0
    log(f"   fill_qao: walking variant_metric_id {cur_lo}..{hi} in windows of "
        f"{FILL_QAO_PK_WINDOW}"
        + (f" (resuming; {total} already updated)" if start is not None else ""))
    while cur_lo <= hi:
        cur_hi = min(cur_lo + FILL_QAO_PK_WINDOW - 1, hi)
        cur.execute(
            "UPDATE variant_metrics "
            "SET quantity_available_online = quantity_available "
            "WHERE variant_metric_id BETWEEN %s AND %s "
            f"AND brand={VC} AND "
            "ISNULL(quantity_available_online, -2147483648) <> "
            "ISNULL(quantity_available, -2147483648)",
            (cur_lo, cur_hi, BRAND))
        rc = cur.rowcount if (cur.rowcount and cur.rowcount > 0) else 0
        conn.commit()
        total += rc
        ckpt["fill_qao_pk"] = cur_hi
        ckpt["fill_qao_total"] = total
        save_checkpoint(ckpt)
        if rc:
            log(f"   fill_qao: ids {cur_lo}..{cur_hi} -> {rc} rows set "
                f"({total} total)")
        cur_lo = cur_hi + 1
    ckpt["fill_qao_done"] = True
    save_checkpoint(ckpt)
    log(f"   fill_qao: done - {total} variant_metrics rows updated")


def apply_add(cur, conn, add_rows, ckpt):
    if ckpt.get("add_done"):
        log("   add: already done (checkpoint)")
        return
    for r in add_rows:
        # idempotent: skip if this style is already in style_info (e.g. a prior
        # run inserted it, or RESET_PROGRESS re-ran this phase)
        sid0, h0 = s(r.get("style_id")), s(r.get("handle"))
        cur.execute(f"SELECT COUNT(*) FROM style_info WHERE brand={VC} AND "
                    f"style_id={VC} AND handle={VC}", (BRAND, sid0, h0))
        if (cur.fetchone() or [0])[0]:
            log(f"   add: {sid0} already in style_info - skipping insert")
            continue
        rec = {"brand": BRAND, "is_manual_override": "1"}
        # 1) plain columns the row already carries (sku_url, hem_style, tags,
        #    style_id, handle, product_name, ...) exactly as filled in H..AM
        for col in TABLE_COLS["style_info"]:
            v = r.get(col)
            if not is_blank(v):
                rec[col] = s(v)
        rec["style_id"] = s(r.get("style_id")) or rec.get("style_id", "")
        rec["handle"] = s(r.get("handle")) or rec.get("handle", "")
        # 2) NEW literals override the plain values
        for k in list(r.keys()):
            if isinstance(k, str) and k.lower().startswith("new "):
                col = re.sub(r"\s+", "_", k[4:].strip().lower())
                if col in TABLE_COLS["style_info"] and not is_blank(r.get(k)):
                    rec[col] = s(r.get(k))
        rec["is_manual_override"] = "1"
        # 3) created_at / captured_date / captured_datetime / source_file_name
        #    from the existing style_metrics row for this style_id (its newest
        #    capture); fall back to now / a marker if the style isn't there.
        sid = rec.get("style_id", "")
        cur.execute(f"SELECT TOP 1 published_at, created_at, captured_date, "
                    f"captured_datetime, source_file_name FROM style_metrics "
                    f"WHERE brand={VC} AND style_id={VC} "
                    f"ORDER BY captured_datetime DESC", (BRAND, sid))
        sm = cur.fetchone()
        now = dt.datetime.now(_TZ).replace(tzinfo=None)
        if sm:
            pub, cre, cdate, cdt, sfn = sm
            rec.setdefault("created_at", s(pub) or s(cre))   # published_at -> created_at
            rec["captured_date"] = cdate if cdate is not None else now
            rec["captured_datetime"] = cdt if cdt is not None else now
            rec["source_file_name"] = s(sfn) or "AGJEANS_cleanup_add"
        else:
            rec.setdefault("captured_date", now)
            rec.setdefault("captured_datetime", now)
            rec.setdefault("source_file_name", "AGJEANS_cleanup_add")
        cols = [c for c in rec if not is_blank(rec[c])]
        ph = ", ".join("%s" for _ in cols)
        cur.execute(f"INSERT INTO style_info ({', '.join('['+c+']' for c in cols)}) "
                    f"VALUES ({ph})", [rec[c] for c in cols])
        log(f"   add: inserted style_info {rec.get('product_name')} "
            f"(sku_url set: {'sku_url' in rec}, from style_metrics: {bool(sm)})")
    conn.commit()
    ckpt["add_done"] = True
    save_checkpoint(ckpt)


def compute_keep_pn(si_rows) -> Dict[str, str]:
    """handle -> the ONE product_name to keep. Correcting several style_info rows
    of a handle to the same product_name would violate uq_style_info_product
    (brand, product_name, inseam_label), so we collapse each handle to one row
    FIRST. Keep the already-correct row (blank NEW product_name); if every
    duplicate has a NEW product_name, keep one. Verified: no handle has >1
    distinct inseam_label, so this never merges legitimately different rows."""
    byh: Dict[str, list] = {}
    for r in si_rows:
        if norm(r.get("ACTION")) in ("remove", "add"):
            continue
        h = norm(r.get("handle"))
        if h:
            byh.setdefault(h, []).append(r)
    keep: Dict[str, str] = {}
    for h, rows in byh.items():
        if len(rows) < 2:
            continue
        blanks = [r for r in rows if is_blank(r.get("NEW product_name"))]
        chosen = blanks[0] if blanks else rows[0]
        pn = s(chosen.get("product_name"))
        if pn:
            keep[h] = pn
    return keep


def apply_pre_dedupe(cur, conn, keep_pn, ckpt):
    if ckpt.get("pre_dedupe_done"):
        log("   pre-dedupe: already done (checkpoint)")
        return
    total = 0
    items = sorted(keep_pn.items())
    for i, (h, pn) in enumerate(items, 1):
        cur.execute(f"DELETE FROM style_info WHERE brand={VC} AND handle={VC} "
                    f"AND product_name <> {VC}", (BRAND, h, pn))
        rc = cur.rowcount if (cur.rowcount and cur.rowcount > 0) else 0
        total += rc
        if i % COMMIT_EVERY == 0:
            conn.commit()
            log(f"   pre-dedupe: {i}/{len(items)} handles, {total} dup rows removed...")
    conn.commit()
    ckpt["pre_dedupe_done"] = True
    save_checkpoint(ckpt)
    log(f"   pre-dedupe: collapsed {len(items)} handles, removed {total} "
        f"duplicate style_info rows")


def _resolve_si_conflict(cur, changed_col, new_val, match_val, handle) -> bool:
    """Return True if this update should be SKIPPED to avoid a
    uq_style_info_product collision. A true duplicate is same style_id AND same
    handle - and those are already collapsed by the per-handle pre-dedupe. So any
    remaining conflict is with a DIFFERENT handle: a distinct style that merely
    maps to the same (product_name, inseam_label). We do NOT merge those - skip
    the update, leaving the row its own unique name to be reworked in the workbook.
    """
    other = "inseam_label" if changed_col == "product_name" else "product_name"
    mclause, mparams = _match_clause(changed_col, match_val)
    cur.execute(f"SELECT DISTINCT [{other}] FROM style_info "
                f"WHERE brand={VC} AND {mclause} AND handle={VC}",
                [BRAND] + mparams + [handle])
    for (oval,) in cur.fetchall():
        oclause, oparams = _match_clause(other, oval)
        cur.execute(f"SELECT TOP 1 1 FROM style_info "
                    f"WHERE brand={VC} AND [{changed_col}]={VC} AND {oclause} "
                    f"AND handle<>{VC}", [BRAND, s(new_val)] + oparams + [handle])
        if cur.fetchone():
            return True
    return False


def _colliding_variant_titles(corrections, deriver) -> set:
    """variant_title values that two or more DISTINCT skus would be set to. On
    variant_metrics those collide on uq_variant_metrics_title (they share capture
    dates), so they must be skipped and the names reworked in the workbook."""
    by_val: Dict[str, set] = {}
    for c in corrections:
        if c.change_field != "variant_title":
            continue
        nv = c.new_val
        if c.derive and deriver is not None:
            nv = deriver.get("variant_title", c.key_val, c.key_field, c.match_val)
        if is_blank(nv):
            continue
        by_val.setdefault(norm(nv), set()).add(s(c.key_val))
    return {v for v, skus in by_val.items() if len(skus) > 1}


def apply_corrections(cur, conn, corrections, deriver, ckpt, collide_vt=None):
    done = ckpt.setdefault("corrections", {})
    start = int(done.get("_i", 0))
    conflicts: List[tuple] = []
    vt_skipped = 0
    collide_vt = collide_vt or set()
    changed = derived_used = derived_miss = 0
    for i, c in enumerate(corrections):
        if i < start:
            continue
        new_val = c.new_val
        if c.derive:
            if deriver is None:
                derived_miss += 1
                new_val = None
            else:
                new_val = deriver.get(c.change_field, c.key_val, c.key_field, c.match_val)
                if is_blank(new_val):
                    derived_miss += 1
                    new_val = None
                else:
                    derived_used += 1
        if (c.change_field == "variant_title" and not is_blank(new_val)
                and norm(new_val) in collide_vt):
            # two distinct skus would get this variant_title -> collides on
            # uq_variant_metrics_title; skip both, rename in the workbook.
            conflicts.append(("variant_title", s(new_val), c.key_val))
            vt_skipped += 1
            if (i + 1) % COMMIT_EVERY == 0:
                conn.commit(); done["_i"] = i + 1; save_checkpoint(ckpt)
            continue
        if not is_blank(new_val) and norm(new_val) != norm(c.match_val):
            # style_info has a unique index (brand, product_name, inseam_label).
            # Resolve a would-be collision: delete a same-style_id duplicate, or
            # SKIP the update when a DIFFERENT style_id already holds the target
            # (two distinct styles - don't merge; leave this row's unique name).
            if c.table == "style_info" and c.change_field in ("product_name", "inseam_label"):
                if _resolve_si_conflict(cur, c.change_field, s(new_val),
                                        c.match_val, c.key_val):
                    conflicts.append((c.change_field, s(new_val), c.key_val))
                    continue
            sets = [f"[{c.change_field}] = {VC}"]
            params = [s(new_val)]
            if c.table == "style_info":
                sets.append("[is_manual_override] = 1")
            mclause, mparams = _match_clause(c.change_field, c.match_val)
            kclause, kparams = _match_clause(c.key_field, c.key_val)
            sql = (f"UPDATE {c.table} SET {', '.join(sets)} "
                   f"WHERE brand = {VC} AND {mclause} AND {kclause}")
            t0 = time.monotonic()
            cur.execute(sql, params + [BRAND] + mparams + kparams)
            dt_s = time.monotonic() - t0
            if dt_s > SLOW_UPDATE_SEC:
                log(f"   SLOW ({dt_s:.0f}s): {c.table}.{c.change_field} "
                    f"match={s(c.match_val)[:20]!r} {c.key_field}={c.key_val[:26]!r}")
            rc = cur.rowcount if (cur.rowcount and cur.rowcount > 0) else 0
            changed += rc
        if (i + 1) % COMMIT_EVERY == 0:
            conn.commit(); done["_i"] = i + 1; save_checkpoint(ckpt)
            log(f"   corrections: {i+1}/{len(corrections)} ({changed} rows changed)...")
    conn.commit(); done["_i"] = len(corrections); save_checkpoint(ckpt)
    log(f"   corrections: {len(corrections)} spec cells - {changed} DB rows changed; "
        f"derived used {derived_used}, unresolved {derived_miss}; "
        f"variant_title skipped {vt_skipped}")
    if conflicts:
        uniq = sorted(set(conflicts))
        log(f"   NOTE: {len(uniq)} correction(s) SKIPPED - a DISTINCT style (different "
            f"handle/sku) would take the same name, which collides on a unique index. "
            f"Rework these names in the workbook (BOTH lookup and variant_metrics "
            f"tabs for variant_title) so they don't repeat, then re-run:")
        for cf, nv, kv in uniq[:60]:
            log(f"      {cf}={nv!r}  key={kv}")
        if len(uniq) > 60:
            log(f"      ... and {len(uniq)-60} more")


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
        members.sort(key=lambda t: (t[1] is None, t[1], t[0]))
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

    plan = build_plan()
    corrections = plan["corrections"]
    n_lit = sum(1 for c in corrections if not c.derive)
    n_der = sum(1 for c in corrections if c.derive)
    by_tab = {}
    for c in corrections:
        by_tab[c.table] = by_tab.get(c.table, 0) + 1
    log(f"Corrections: {len(corrections)} cells ({n_lit} literal, {n_der} derived)")
    for t, n in by_tab.items():
        log(f"   {t:16} {n}")
    log(f"Remove: {len(plan['remove_handles'])} handles + {len(plan['remove_skus'])} skus")
    log(f"Add: {len(plan['add_rows'])} style_info row(s)")
    if DO_PRE_DEDUPE:
        log(f"Pre-dedupe: {len(compute_keep_pn(plan['si_rows']))} handles collapse to "
            f"one style_info row before correcting (avoids uq_style_info_product)")
    if plan.get("fill_qao"):
        log("fill_qao: quantity_available_online will be set = quantity_available "
            "for ALL AGJEANS variant_metrics")

    deriver = None
    if DO_DERIVED and n_der:
        g = load_scraper()
        deriver = Deriver(g, plan["si_rows"], plan.get("sku2handle"))
        log(f"Deriver: {len(deriver.by_handle)} handles from agjeans_inventory rules")

    if DRY_RUN:
        if deriver:
            log("Derivation preview (first 15 UNIQUE handles -> product_name | "
                "style_name | jean_style | rise_label | inseam_label | inseam_style):")
            seen = set()
            for c in corrections:
                if not c.derive or c.key_field != "handle":
                    continue
                h = norm(c.key_val)
                if h in seen:
                    continue
                seen.add(h)
                d = deriver.by_handle.get(h, {})
                log(f"   {c.key_val[:34]:34} {d.get('product_name','')[:40]:40} | "
                    f"{d.get('style_name','')[:18]:18} | {d.get('jean_style','')[:16]:16} | "
                    f"{d.get('rise_label','')[:6]:6} | {d.get('inseam_label','')[:8]:8} | "
                    f"{d.get('inseam_style','')}")
                if len(seen) >= 15:
                    break
            # Farrah ground-truth check
            far = "farrah-skinny-ankle-mid-rise-skinny-ankle-cloud-soft-denim-hsd1777rhwht"
            if far in deriver.by_handle:
                log(f"   [Farrah check] derived product_name="
                    f"{deriver.by_handle[far]['product_name']!r}")
        log("Sample literal corrections:")
        shown = 0
        for c in corrections:
            if c.derive:
                continue
            log(f"   {c.table:14} set {c.change_field}={c.new_val[:34]!r} "
                f"where {c.change_field}={s(c.match_val)[:22]!r} & {c.key_field}={c.key_val[:26]!r}")
            shown += 1
            if shown >= 8:
                break
        log("DRY RUN - nothing written.")
        return

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
            apply_removals(cur, conn, plan["remove_handles"], plan["remove_skus"], ckpt)
        if DO_ADD:
            log("Phase 2: add missing style_info...")
            apply_add(cur, conn, plan["add_rows"], ckpt)
        if DO_PRE_DEDUPE:
            log("Phase 2.5: collapse duplicate style_info rows per handle...")
            apply_pre_dedupe(cur, conn, compute_keep_pn(plan["si_rows"]), ckpt)
        if DO_CORRECTIONS:
            log("Phase 3: corrections (value + key matched)...")
            collide_vt = _colliding_variant_titles(corrections, deriver)
            if collide_vt:
                log(f"   {len(collide_vt)} variant_title value(s) map to >1 sku "
                    f"(distinct styles) - these will be skipped and reported")
            apply_corrections(cur, conn, corrections, deriver, ckpt, collide_vt)
        if DO_CORRECTIONS and plan.get("fill_qao"):
            log("Phase 3.5: quantity_available_online = quantity_available (all AGJEANS)...")
            apply_fill_qao(cur, conn, ckpt)
        if DO_DEDUPE:
            log("Phase 4: dedupe...")
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


if __name__ == "__main__":
    main()
