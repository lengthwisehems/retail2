#!/usr/bin/env python3
"""Reformation basket-inventory probe (SFCC / Demandware).

Flow:
- Discover style+color PIDs from /jeans grid.
- Expand to size SKUs from Product-ShowQuickAdd (per style+color).
- Probe warehouse inventory via cart `basketInventory` for orderable SKUs.
- Optionally sum store ATS via Stores-FindStores + Stores-getAtsValue.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Set

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook

BRAND = "Reformation"
BASE_URL = "https://www.thereformation.com"
CATEGORY_URL = f"{BASE_URL}/jeans"
LOCALE_PATH = "/on/demandware.store/Sites-reformation-us-Site/en_US"
BATCH_SIZE = 30
# IMPORTANT: this limits style+color PIDs, not size SKUs.
MAX_VARIANTS: int | None = None
ENABLE_STORE_INVENTORY = True

US_STORE_IDS = [
    "53", "76", "92", "106", "113", "80", "68", "28", "112", "93", "110", "10", "108",
    "9", "54", "96", "116", "84", "90", "115", "91", "44", "107", "57", "89", "105",
    "64", "12", "87", "11", "109", "5",
]
CA_STORE_IDS = ["104"]

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
TIMEOUT = 30
MAX_RETRIES = 5

WAREHOUSES = ["web", "cvh", "vrn"]
METRICS = [
    "customATS",
    "alloc",
    "preOrder",
    "onOrder",
    "turnover",
    "damaged",
    "floor",
    "missing",
    "reserve",
    "return",
    "refOnOrder",
    "refTurnover",
]


class ProbeError(RuntimeError):
    pass


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("reformation_basket_inventory_probe")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    stamp = datetime.now().strftime("%Y-%m-%d")
    preferred = OUTPUT_DIR / f"{BRAND.lower()}_basket_inventory_probe_{stamp}.log"
    fallback = OUTPUT_DIR / f"{BRAND.lower()}_run.log"
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    try:
        fh = logging.FileHandler(preferred, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception as exc:  # pragma: no cover
        logger.warning("Could not open %s, fallback to %s (%s)", preferred, fallback, exc)
        try:
            fh = logging.FileHandler(fallback, encoding="utf-8")
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except Exception:
            pass

    return logger


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json,text/html,*/*", "X-Requested-With": "XMLHttpRequest"})
    return s


def request_json(session: requests.Session, method: str, url: str, logger: logging.Logger, **kwargs) -> dict:
    r = get_with_retries(session, method, url, logger, **kwargs)
    try:
        return r.json() or {}
    except Exception as exc:
        raise ProbeError(f"Non-JSON response from {url}: {exc}") from exc


def get_with_retries(session: requests.Session, method: str, url: str, logger: logging.Logger, **kwargs) -> requests.Response:
    delay = 1.0
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.request(method=method, url=url, timeout=TIMEOUT, **kwargs)
            if r.status_code in {429, 500, 502, 503, 504}:
                logger.warning("Transient HTTP %s for %s %s (attempt %s/%s)", r.status_code, method, r.url, attempt, MAX_RETRIES)
                time.sleep(delay)
                delay *= 2
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("Request error %s %s (attempt %s/%s): %s", method, url, attempt, MAX_RETRIES, exc)
            time.sleep(delay)
            delay *= 2

    raise ProbeError(f"Failed request after {MAX_RETRIES} attempts: {method} {url} | Last error: {last_error}")


def extract_pids_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    pids: Set[str] = set()
    for tag in soup.select("[data-pid]"):
        pid = (tag.get("data-pid") or "").strip()
        if re.fullmatch(r"\d{7}[A-Z]{3}", pid):
            pids.add(pid)

    text = soup.decode()
    for pid in re.findall(r'"pid"\s*:\s*"([A-Z0-9]+)"', text):
        if re.fullmatch(r"\d{7}[A-Z]{3}", pid):
            pids.add(pid)
    return sorted(pids)


def iter_jeans_style_color_pids(session: requests.Session, logger: logging.Logger) -> Iterable[str]:
    first = get_with_retries(session, "GET", CATEGORY_URL, logger)
    for pid in extract_pids_from_html(first.text):
        yield pid

    start = 16
    page_size = 16
    while True:
        url = f"{BASE_URL}{LOCALE_PATH}/Search-UpdateGrid?cgid=jeans&pmpt=qualifying&start={start}&sz={page_size}"
        resp = get_with_retries(session, "GET", url, logger)
        pids = extract_pids_from_html(resp.text)
        logger.info("Grid start=%s yielded %s base pids", start, len(pids))
        if not pids:
            break
        for pid in pids:
            yield pid
        start += page_size


def price_value(product: dict) -> str:
    price = product.get("price")
    if isinstance(price, dict):
        sales = price.get("sales")
        if isinstance(sales, dict):
            return str(sales.get("formatted") or sales.get("value") or "")
        return str(price.get("formatted") or price.get("value") or "")
    if price not in (None, ""):
        return str(price)
    rendered = product.get("renderedPrice") or ""
    return re.sub(r"<[^>]+>", "", rendered).strip()


def extract_color_bucket(product: dict, style_color_pid: str) -> dict | None:
    style_color = style_color_pid[:10]
    attrs = product.get("variationAttributes") or []
    for attr in attrs:
        attr_id = (attr.get("attributeId") or attr.get("id") or "").strip()
        if attr_id != "sizeByColor":
            continue
        for bucket in attr.get("values") or []:
            color = bucket.get("color") or {}
            sub_id = (color.get("productId") or "").strip()
            color_code = (color.get("value") or color.get("id") or "").strip()
            if sub_id == style_color or (re.fullmatch(r"[A-Z]{3}", color_code) and style_color_pid == style_color_pid[:7] + color_code):
                return bucket
    return None


def normalize_int(value: object) -> int:
    s = str(value or "").strip()
    if s in {"", "null", "None"}:
        return 0
    try:
        return int(float(s))
    except Exception:
        return 0


def discover_variant_catalog(session: requests.Session, style_color_pids: List[str], logger: logging.Logger) -> Dict[str, dict]:
    catalog: Dict[str, dict] = {}
    for sc_pid in style_color_pids:
        try:
            payload = request_json(session, "GET", f"{BASE_URL}{LOCALE_PATH}/Product-ShowQuickAdd", logger, params={"pid": sc_pid})
        except Exception as exc:
            logger.warning("Skipping style+color %s (%s)", sc_pid, exc)
            continue

        product = payload.get("product") or {}
        name = str(product.get("productName") or "")
        bucket = extract_color_bucket(product, sc_pid)
        if not bucket:
            continue

        sizes = bucket.get("sizes") or []
        for size in sizes:
            variant = size.get("product") or {}
            variant_pid = str(variant.get("id") or "").strip()
            if not variant_pid:
                continue

            availability = variant.get("availability") or {}
            ats = normalize_int(availability.get("ats"))
            pre = normalize_int(availability.get("preOrder"))
            on_order = normalize_int(availability.get("onOrder"))
            is_available = bool(variant.get("available"))
            selectable = bool(size.get("selectable"))
            probe_eligible = selectable and (is_available or ats > 0 or pre > 0 or on_order > 0)

            catalog[variant_pid] = {
                "Product Name": name,
                "PID": variant_pid,
                "StyleColor": sc_pid,
                "Style": sc_pid[:7],
                "Price": price_value(variant) or price_value(product),
                "probe_eligible": probe_eligible,
            }

    logger.info("Discovered %s unique size variant PIDs", len(catalog))
    return catalog


def get_csrf(session: requests.Session, logger: logging.Logger) -> dict:
    get_with_retries(session, "GET", CATEGORY_URL, logger)
    payload = request_json(session, "POST", f"{BASE_URL}{LOCALE_PATH}/CSRF-Generate", logger)
    csrf = payload.get("csrf") or {}
    if not csrf.get("tokenName") or not csrf.get("token"):
        raise ProbeError("Failed to retrieve CSRF token")
    return csrf


def add_variant_to_cart(session: requests.Session, csrf: dict, variant_pid: str, logger: logging.Logger) -> bool:
    data = {"pid": variant_pid, "quantity": 1, csrf["tokenName"]: csrf["token"]}
    payload = request_json(session, "POST", f"{BASE_URL}{LOCALE_PATH}/Cart-AddProduct", logger, data=data)
    result = payload.get("result") or {}
    ok = not result.get("error", False)
    if not ok:
        logger.warning("Add to cart failed for %s: %s", variant_pid, payload.get("message"))
    return ok


def parse_basket_inventory(text: str) -> List[dict]:
    rows: List[dict] = []
    for raw_line in text.split("\\n"):
        line = raw_line.strip().replace("\\n", "")
        if not line or "->" not in line or "=" not in line:
            continue
        sku, rest = line.split("->", 1)
        warehouse, metrics_blob = rest.split("=", 1)
        row = {"PID": sku.strip(), "Warehouse": warehouse.strip()}
        for kv in metrics_blob.split(","):
            if ":" not in kv:
                continue
            key, value = kv.split(":", 1)
            row[key.strip()] = value.strip()
        rows.append(row)
    return rows


def scrape_basket_inventory(session: requests.Session, logger: logging.Logger) -> List[dict]:
    cart = get_with_retries(session, "GET", f"{BASE_URL}/cart", logger)
    m = re.search(r'var\s+basketInventory\s*=\s*"([\s\S]*?)";', cart.text)
    if not m:
        return []
    return parse_basket_inventory(m.group(1))


def chunked(items: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(items), n):
        yield items[i : i + n]


def get_configured_store_ids(logger: logging.Logger) -> tuple[List[str], List[str]]:
    us_ids = sorted(set(US_STORE_IDS), key=lambda x: int(x) if x.isdigit() else x)
    ca_ids = sorted(set(CA_STORE_IDS), key=lambda x: int(x) if x.isdigit() else x)
    logger.info("Configured %s US stores and %s CA stores for ATS lookup", len(us_ids), len(ca_ids))
    return us_ids, ca_ids


def get_store_ats_sum(session: requests.Session, logger: logging.Logger, pid: str, store_ids: List[str]) -> int:
    total = 0
    for sid in store_ids:
        try:
            payload = request_json(
                session,
                "GET",
                f"{BASE_URL}{LOCALE_PATH}/Stores-getAtsValue",
                logger,
                params={"pid": pid, "storeId": sid},
            )
        except Exception:
            continue
        total += normalize_int(payload.get("atsValue"))
    return total


def warehouse_columns_from_map(pid: str, wh_map: Dict[str, dict]) -> dict:
    row = {"PID": pid}
    for metric in METRICS:
        for wh in WAREHOUSES:
            key = f"{wh}.{metric}"
            row[key] = (wh_map.get(wh) or {}).get(metric, "")
    return row


def run_probe(logger: logging.Logger) -> List[dict]:
    discovery_session = build_session()
    style_color_pids = sorted(set(iter_jeans_style_color_pids(discovery_session, logger)))
    if MAX_VARIANTS is not None:
        style_color_pids = style_color_pids[:MAX_VARIANTS]
    logger.info("Discovered %s style+color jeans pids", len(style_color_pids))

    variant_catalog = discover_variant_catalog(discovery_session, style_color_pids, logger)
    all_variant_pids = sorted(variant_catalog.keys())
    probe_variant_pids = [pid for pid in all_variant_pids if variant_catalog[pid].get("probe_eligible")]

    logger.info("Total size SKUs discovered: %s", len(all_variant_pids))
    logger.info("Eligible size SKUs for cart probing: %s", len(probe_variant_pids))
    logger.info("Distinct style+color in catalog: %s", len({v['StyleColor'] for v in variant_catalog.values()}))
    logger.info("Distinct style ids in catalog: %s", len({v['Style'] for v in variant_catalog.values()}))

    basket_map: Dict[tuple[str, str], dict] = {}
    for batch_num, batch in enumerate(chunked(probe_variant_pids, BATCH_SIZE), start=1):
        logger.info("Batch %s: probing %s variants via cart", batch_num, len(batch))
        s = build_session()
        csrf = get_csrf(s, logger)

        for pid in batch:
            add_variant_to_cart(s, csrf, pid, logger)

        parsed = scrape_basket_inventory(s, logger)
        logger.info("Batch %s: parsed %s basketInventory lines", batch_num, len(parsed))
        for row in parsed:
            basket_map[(row.get("PID", ""), row.get("Warehouse", ""))] = row

    wh_by_pid: Dict[str, Dict[str, dict]] = {}
    for (pid, wh), metrics in basket_map.items():
        if pid not in wh_by_pid:
            wh_by_pid[pid] = {}
        wh_by_pid[pid][wh] = metrics

    us_store_ats_by_pid: Dict[str, int] = {}
    ca_store_ats_by_pid: Dict[str, int] = {}
    if ENABLE_STORE_INVENTORY and all_variant_pids:
        stores_session = build_session()
        us_store_ids, ca_store_ids = get_configured_store_ids(logger)
        for idx, pid in enumerate(all_variant_pids, start=1):
            if idx % 100 == 0:
                logger.info("Store ATS progress: %s/%s", idx, len(all_variant_pids))
            if us_store_ids:
                us_store_ats_by_pid[pid] = get_store_ats_sum(stores_session, logger, pid, us_store_ids)
            if ca_store_ids:
                ca_store_ats_by_pid[pid] = get_store_ats_sum(stores_session, logger, pid, ca_store_ids)

    output_rows: List[dict] = []
    for pid in all_variant_pids:
        meta = variant_catalog.get(pid, {})
        wh_cols = warehouse_columns_from_map(pid, wh_by_pid.get(pid, {}))
        row = {
            "Product Name": meta.get("Product Name", ""),
            "PID": pid,
            "Price": meta.get("Price", ""),
            **wh_cols,
            "store.customATS": us_store_ats_by_pid.get(pid, ""),
            "store.ca.customATS": ca_store_ats_by_pid.get(pid, ""),
            "Source": f"{BASE_URL}/cart",
        }
        output_rows.append(row)

    return output_rows


def write_excel(rows: List[dict], logger: logging.Logger) -> Path:
    headers = [
        "Product Name",
        "PID",
        "Price",
        "web.customATS",
        "cvh.customATS",
        "vrn.customATS",
        "web.alloc",
        "store.customATS",
        "store.ca.customATS",
        "cvh.alloc",
        "vrn.alloc",
        "web.preOrder",
        "cvh.preOrder",
        "vrn.preOrder",
        "web.onOrder",
        "cvh.onOrder",
        "vrn.onOrder",
        "web.turnover",
        "cvh.turnover",
        "vrn.turnover",
        "web.damaged",
        "cvh.damaged",
        "vrn.damaged",
        "web.floor",
        "cvh.floor",
        "vrn.floor",
        "web.missing",
        "cvh.missing",
        "vrn.missing",
        "web.reserve",
        "cvh.reserve",
        "vrn.reserve",
        "web.return",
        "cvh.return",
        "vrn.return",
        "web.refOnOrder",
        "cvh.refOnOrder",
        "vrn.refOnOrder",
        "web.refTurnover",
        "cvh.refTurnover",
        "vrn.refTurnover",
        "Source",
    ]

    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out = OUTPUT_DIR / f"{BRAND}_BasketInventory_Probe_{stamp}.xlsx"

    wb = Workbook()
    ws = wb.active
    ws.title = "Basket Inventory"
    ws.append(headers)
    for row in rows:
        ws.append([row.get(h, "") for h in headers])

    wb.save(out)
    logger.info("Excel written: %s", out.resolve())
    return out


def main() -> None:
    logger = setup_logger()
    rows = run_probe(logger)

    dedup: Dict[str, dict] = {}
    for row in rows:
        dedup[row.get("PID", "")] = row

    final_rows = list(dedup.values())
    logger.info("Final deduped rows: %s", len(final_rows))
    write_excel(final_rows, logger)


if __name__ == "__main__":
    main()
