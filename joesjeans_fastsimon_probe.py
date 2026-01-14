import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "joesjeans_fastsimon_probe.log"

FAST_SIMON_ENDPOINT = "https://api.fastsimon.com/categories_navigation"
STORE_ID = "2912321571"
CATEGORY_ID = "89539608611"
UUID = "477e327a-303f-475d-abfa-c3813e0eb675"
ISP_TOKEN = "g01K6HKP6XAGN48R69XVK8YCJXE"

DEFAULT_PARAMS = {
    "store_id": STORE_ID,
    "category_id": CATEGORY_ID,
    "UUID": UUID,
    "uuid": UUID,
    "api_type": "json",
    "facets_required": 1,
    "with_product_attributes": 1,
    "request_source": "v-next-ssr",
    "src": "v-next-ssr",
    "narrow": "[]",
    "st": ISP_TOKEN,
}


def configure_logging() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    handlers = [logging.StreamHandler()]
    try:
        handlers.insert(0, logging.FileHandler(LOG_PATH, encoding="utf-8"))
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=handlers)
    except OSError:
        fallback = OUTPUT_DIR / "joesjeans_fastsimon_probe.log"
        handlers[0] = logging.FileHandler(fallback, encoding="utf-8")
        handlers.append(logging.StreamHandler())
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=handlers)
        logging.warning("Primary log path locked; using fallback %s", fallback)


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504], raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json,text/javascript,*/;q=0.9",
        }
    )
    session.verify = False
    return session


def format_epoch(ts: str) -> str:
    try:
        return datetime.fromtimestamp(int(ts), datetime.UTC).isoformat()
    except Exception:
        return ""


def fetch_page(
    session: requests.Session,
    page_num: int,
    per_page: int = 100,
    extra_params: Dict | None = None,
) -> Dict:
    params = {**DEFAULT_PARAMS, "page_num": page_num, "products_per_page": per_page}
    if extra_params:
        params.update(extra_params)
    resp = session.get(FAST_SIMON_ENDPOINT, params=params, timeout=30, verify=False)
    resp.raise_for_status()
    return resp.json()


def fetch_all_items(session: requests.Session, extra_params: Dict | None = None) -> List[Dict]:
    page = 1
    per_page = 100
    items: List[Dict] = []
    total_results = None
    while True:
        payload = fetch_page(session, page, per_page, extra_params=extra_params)
        batch = payload.get("items", [])
        total_results = payload.get("total_results", total_results)
        logging.info("Fetched page %s with %s items (total_results=%s)", page, len(batch), total_results)
        if not batch:
            break
        items.extend(batch)
        if len(batch) < per_page:
            break
        if total_results and len(items) >= total_results:
            break
        page += 1
    logging.info("Total items collected: %s", len(items))
    return items


def extract_inventory_map(items: Iterable[Dict]) -> Tuple[Dict[str, str], set]:
    inventory_map: Dict[str, str] = {}
    inventory_keys: set = set()
    priority = [
        "quantityAvailable",
        "inventory_quantity",
        "inventoryQuantity",
        "quantity_available",
        "quantity",
        "available_quantity",
    ]

    for item in items:
        for entry in item.get("vra", []) or []:
            if not isinstance(entry, list) or len(entry) < 2 or not isinstance(entry[0], int):
                continue
            variant_id, attrs = entry
            attr_map: Dict[str, str] = {}
            for pair in attrs or []:
                if not isinstance(pair, list) or len(pair) < 2:
                    continue
                key = str(pair[0])
                value = pair[1]
                if isinstance(value, list):
                    value = ";".join(str(v) for v in value if v is not None)
                else:
                    value = str(value)
                if any(token in key.lower() for token in ["inventory", "quantity"]):
                    attr_map[key] = value
            if attr_map:
                inventory_keys.update(attr_map.keys())
                chosen = next((attr_map[k] for k in priority if k in attr_map), None)
                inventory_map[str(variant_id)] = chosen or next(iter(attr_map.values()))
    return inventory_map, inventory_keys


def build_inventory_lookup(session: requests.Session, base_items: List[Dict]) -> Tuple[Dict[str, str], Dict[str, str], set]:
    inventory_map, inventory_keys = extract_inventory_map(base_items)
    source_map: Dict[str, str] = {vid: "categories_navigation" for vid in inventory_map}

    if inventory_map:
        logging.info("Inventory fields detected in baseline feed: %s", sorted(inventory_keys))
        return inventory_map, source_map, inventory_keys

    logging.info("Baseline Fast Simon feed missing inventory fields; attempting inventory-enriched params")
    enriched_items = fetch_all_items(
        session,
        extra_params={
            "with_inventory": 1,
            "with_product_inventory": 1,
            "with_variant_inventory": 1,
            "with_product_attributes": 1,
        },
    )
    if not enriched_items:
        logging.warning("Inventory-enriched Fast Simon fetch returned no items")
        return inventory_map, source_map, inventory_keys

    enriched_map, enriched_keys = extract_inventory_map(enriched_items)
    if enriched_map:
        inventory_map.update(enriched_map)
        inventory_keys.update(enriched_keys)
        for vid in enriched_map:
            source_map[vid] = "categories_navigation_with_inventory"
        logging.info("Inventory fields detected after enrichment: %s", sorted(inventory_keys))
    else:
        logging.warning("Fast Simon responses did not expose inventory/quantity fields even with enrichment flags")

    return inventory_map, source_map, inventory_keys


def parse_vra_entries(vra: Iterable) -> Tuple[List[Tuple[int, Dict[str, str]]], Dict[str, str]]:
    variant_entries: List[Tuple[int, Dict[str, str]]] = []
    product_attrs: Dict[str, str] = {}
    for entry in vra or []:
        if not isinstance(entry, list) or len(entry) < 2:
            continue
        key, values = entry[0], entry[1]
        if isinstance(key, int):
            attr_map: Dict[str, str] = {}
            for pair in values:
                if not isinstance(pair, list) or len(pair) < 2:
                    continue
                attr_name = str(pair[0])
                val_list = pair[1] if isinstance(pair[1], list) else [pair[1]]
                attr_map[attr_name] = ";".join(str(v) for v in val_list if v is not None)
            variant_entries.append((key, attr_map))
        else:
            val_list = values if isinstance(values, list) else [values]
            product_attrs[str(key)] = ";".join(str(v) for v in val_list if v is not None)
    return variant_entries, product_attrs


def normalize_rows(
    items: Iterable[Dict],
    inventory_map: Dict[str, str] | None = None,
    inventory_source_map: Dict[str, str] | None = None,
) -> List[Dict]:
    raw_rows: List[Dict] = []
    variant_attr_keys: set = set()
    product_attr_keys: set = set()
    inventory_map = inventory_map or {}
    inventory_source_map = inventory_source_map or {}

    for item in items:
        variant_entries, product_attrs = parse_vra_entries(item.get("vra"))
        if not variant_entries:
            variant_entries = [(None, {})]
        for _, attrs in variant_entries:
            variant_attr_keys.update(attrs.keys())
        product_attr_keys.update(product_attrs.keys())
        for variant_id, attrs in variant_entries:
            raw_rows.append(
                {
                    "product.id": item.get("id", ""),
                    "product.title": item.get("l", ""),
                    "product.url": f"https://www.joesjeans.com{item.get('u', '')}",
                    "product.currency": item.get("c", ""),
                    "product.price": item.get("p", ""),
                    "product.price_min": item.get("p_min", ""),
                    "product.price_max": item.get("p_max", ""),
                    "product.compare_at_price": item.get("p_c", ""),
                    "product.compare_at_price_min": item.get("p_min_c", ""),
                    "product.compare_at_price_max": item.get("p_max_c", ""),
                    "product.description": item.get("d", ""),
                    "product.thumb_primary": item.get("t", ""),
                    "product.thumb_secondary": item.get("t2", ""),
                    "product.flag": item.get("f", ""),
                    "product.default_variant": item.get("s", ""),
                    "product.default_sku": item.get("sku", ""),
                    "product.price_split": item.get("p_spl", ""),
                    "product.created_at": format_epoch(item.get("c_date")),
                    "product.variant_count": item.get("v_c", ""),
                    "product.iso_flag": item.get("iso", ""),
                    "product.skus_list": ";".join(item.get("skus", [])) if isinstance(item.get("skus"), list) else "",
                    "product.vendor": item.get("v", ""),
                    "variant.id": variant_id or "",
                    "variant.sku": attrs.get("Product-sku", ""),
                    "variant_attrs": attrs,
                    "product_attrs": product_attrs,
                    "variant.quantityAvailable": inventory_map.get(str(variant_id or ""), ""),
                    "inventory_source": inventory_source_map.get(str(variant_id or ""), ""),
                }
            )

    final_rows: List[Dict] = []
    for row in raw_rows:
        base = {k: v for k, v in row.items() if k not in {"variant_attrs", "product_attrs"}}
        for attr_key in sorted(variant_attr_keys):
            base[f"variant_attr.{attr_key}"] = row["variant_attrs"].get(attr_key, "")
        for attr_key in sorted(product_attr_keys):
            base[f"product_attr.{attr_key}"] = row["product_attrs"].get(attr_key, "")
        base["variant_attributes_json"] = json.dumps(row["variant_attrs"], ensure_ascii=False)
        base["product_attributes_json"] = json.dumps(row["product_attrs"], ensure_ascii=False)
        final_rows.append(base)
    return final_rows


def write_workbook(rows: List[Dict]) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_path = OUTPUT_DIR / f"joesjeans_fastsimon_probe_{timestamp}.xlsx"
    df = pd.DataFrame(rows)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="FastSimon", index=False)
    logging.info("CSV written: %s", out_path.resolve())
    return out_path


def main() -> None:
    configure_logging()
    session = build_session()
    items = fetch_all_items(session)
    inventory_map, inventory_source_map, inventory_keys = build_inventory_lookup(session, items)
    if inventory_map:
        logging.info("Inventory populated for %s variants", len(inventory_map))
    else:
        logging.info("No Fast Simon inventory fields detected; quantity columns will be blank")
    rows = normalize_rows(items, inventory_map=inventory_map, inventory_source_map=inventory_source_map)
    out_path = write_workbook(rows)
    logging.info("Workbook row count: %s", len(rows))
    logging.info("Workbook saved to %s", out_path.resolve())


if __name__ == "__main__":
    main()
