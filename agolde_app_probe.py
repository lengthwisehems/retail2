import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "agolde_app_probe.log"

CANONICAL_COLUMNS = [
    "product.id",
    "product.handle",
    "product.published_at",
    "product.created_at",
    "product.title",
    "product.productType",
    "product.tags_all",
    "product.vendor",
    "product.description",
    "product.descriptionHtml",
    "variant.title",
    "variant.option1",
    "variant.option2",
    "variant.option3",
    "variant.price",
    "variant.compare_at_price",
    "variant.available",
    "variant.quantityAvailable",
    "product.totalInventory",
    "variant.id",
    "variant.sku",
    "variant.barcode",
    "product.images[0].src",
    "product.onlineStoreUrl",
]

COLLECTION_URL = "https://agolde.com/collections/womens-jeans-category/products.json"
PREORDER_SETTINGS_URL = "https://app.preordernowapp.com/get_preloaded_data"
PREORDER_APP_JS = (
    "https://cdn.shopify.com/extensions/7d130b81-2445-42f2-b4a4-905651358e17"
    "/wod-preorder-now-28/assets/preorder-now-source.js"
)
STOREFRONT_ENDPOINT = "https://agolde.com/api/2024-10/graphql.json"


def configure_logging() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    try:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()],
        )
    except OSError:
        fallback = OUTPUT_DIR / "agolde_app_probe.log"
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.FileHandler(fallback, encoding="utf-8"), logging.StreamHandler()],
        )
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
            "Accept": "application/json, text/javascript, */*; q=0.01",
        }
    )
    session.verify = False
    return session


def fetch_collection_products(session: requests.Session) -> List[dict]:
    products: List[dict] = []
    page = 1
    while True:
        resp = session.get(COLLECTION_URL, params={"limit": 250, "page": page}, timeout=30, verify=False)
        resp.raise_for_status()
        payload = resp.json()
        batch = payload.get("products", [])
        logging.info("Fetched page %s with %s products", page, len(batch))
        if not batch:
            break
        products.extend(batch)
        if len(batch) < 250:
            break
        page += 1
    logging.info("Total products collected: %s", len(products))
    return products


def clean_description(html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    return soup.get_text(" ", strip=True)


def build_base_rows(products: Iterable[dict]) -> List[dict]:
    rows: List[dict] = []
    for product in products:
        variants = product.get("variants", [])
        total_inventory = sum((variant.get("inventory_quantity") or 0) for variant in variants)
        tags_all = ",".join(product.get("tags", [])) if isinstance(product.get("tags"), list) else product.get("tags", "")
        description_html = product.get("body_html") or ""
        description = clean_description(description_html)
        image_src = ""
        if product.get("images"):
            image_src = product["images"][0].get("src", "")
        for variant in variants:
            row = {
                "product.id": product.get("id"),
                "product.handle": product.get("handle"),
                "product.published_at": product.get("published_at"),
                "product.created_at": product.get("created_at"),
                "product.title": product.get("title"),
                "product.productType": product.get("product_type"),
                "product.tags_all": tags_all,
                "product.vendor": product.get("vendor"),
                "product.description": description,
                "product.descriptionHtml": description_html,
                "variant.title": variant.get("title"),
                "variant.option1": variant.get("option1"),
                "variant.option2": variant.get("option2"),
                "variant.option3": variant.get("option3"),
                "variant.price": variant.get("price"),
                "variant.compare_at_price": variant.get("compare_at_price"),
                "variant.available": variant.get("available"),
                "variant.quantityAvailable": variant.get("inventory_quantity"),
                "product.totalInventory": total_inventory,
                "variant.id": variant.get("id"),
                "variant.sku": variant.get("sku"),
                "variant.barcode": variant.get("barcode"),
                "product.images[0].src": image_src,
                "product.onlineStoreUrl": f"https://agolde.com/products/{product.get('handle')}",
                "variant.inventory_policy": variant.get("inventory_policy"),
                "variant.inventory_management": variant.get("inventory_management"),
            }
            rows.append(row)
    return rows


def fetch_preorder_settings(session: requests.Session) -> Dict[str, object]:
    resp = session.get(PREORDER_SETTINGS_URL, params={"shop": "agolde.myshopify.com"}, timeout=30, verify=False)
    resp.raise_for_status()
    return resp.json()


def extract_storefront_token(session: requests.Session) -> Optional[str]:
    try:
        resp = session.get(PREORDER_APP_JS, timeout=30, verify=False)
        resp.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        logging.warning("PreOrderNow source fetch failed: %s", exc)
        return None

    text = resp.text
    markers = [
        "storefrontAccessToken",
        "storefront_access_token",
        "X-Shopify-Storefront-Access-Token",
    ]

    for marker in markers:
        idx = text.find(marker)
        if idx == -1:
            continue
        snippet = text[idx : idx + 200]
        for quote in ('"', "'"):
            parts = snippet.split(quote)
            for i, part in enumerate(parts):
                if marker in part and i + 1 < len(parts):
                    candidate = parts[i + 1]
                    if len(candidate) > 10:
                        logging.info("Storefront token candidate found via %s", marker)
                        return candidate.strip()
    logging.info("No storefront token found in PreOrderNow source")
    return None


def decode_preorder_setting(raw: Dict[str, object]) -> Dict[str, object]:
    return {
        "preorder_button_text": raw.get("a"),
        "settings_enabled": raw.get("b"),
        "preorder_stock": raw.get("c"),
        "out_of_stock_message": raw.get("d"),
        "show_stock_remaining": raw.get("e"),
        "stock_remaining_message": raw.get("f"),
        "preorder_description": raw.get("g"),
        "preorder_description_position": raw.get("h"),
        "badge_enabled": raw.get("i"),
        "badge_text": raw.get("j"),
        "preorder_start_date": raw.get("k"),
        "preorder_end_date": raw.get("l"),
        "settings_type": raw.get("m"),
        "settings_type_id": raw.get("n"),
        "use_default": raw.get("o"),
        "product_id": raw.get("p"),
        "use_stock_management": raw.get("q"),
        "use_shopify_stock_management": raw.get("r"),
        "shopify_inventory": raw.get("s"),
        "shopify_preorder_limit": raw.get("t"),
        "shopify_stock_mgmt_method": raw.get("u"),
        "oversell_enabled": raw.get("v"),
        "badge_shape": raw.get("w"),
        "cart_label_text": raw.get("x"),
        "product_image_src": raw.get("y"),
        "discount_type": raw.get("z"),
        "discount_percentage": raw.get("aa"),
        "discount_fixed_amount": raw.get("ab"),
        "partial_payment_discount_type": raw.get("partial_payment_z"),
        "partial_payment_discount_percentage": raw.get("partial_payment_aa"),
        "partial_payment_discount_fixed_amount": raw.get("partial_payment_ab"),
        "tag": raw.get("tag"),
        "handle": raw.get("handle"),
        "price": raw.get("price"),
        "compare_at_price": raw.get("compare_at_price"),
        "selling_plan_group_id": raw.get("selling_plan_group_id"),
        "selling_plan_group_name": raw.get("selling_plan_group_name"),
        "partial_payment_charge_date": raw.get("partial_payment_charge_date"),
    }


def decode_preorder_settings_map(settings: Dict[str, object]) -> Dict[str, Dict[str, object]]:
    decoded: Dict[str, Dict[str, object]] = {}
    for item in settings.get("single_product_settings", []) if settings else []:
        decoded[str(item.get("n"))] = decode_preorder_setting(item)
    return decoded


def fetch_variant_inventory(session: requests.Session, variant_ids: List[str]) -> Dict[str, Dict[str, object]]:
    inventory: Dict[str, Dict[str, object]] = {}

    def worker(variant_id: str) -> Optional[Dict[str, object]]:
        try:
            resp = requests.get(
                f"https://agolde.com/variants/{variant_id}.json",
                headers=session.headers,
                timeout=30,
                verify=False,
            )
            if resp.status_code != 200:
                logging.warning("Variant %s returned %s", variant_id, resp.status_code)
                return None
            payload = resp.json().get("product_variant", {})
            return {
                "shopify_inventory": payload.get("inventory_quantity"),
                "inventory_policy": payload.get("inventory_policy"),
                "inventory_management": payload.get("inventory_management"),
            }
        except Exception as exc:  # noqa: BLE001
            logging.warning("Variant %s inventory fetch failed: %s", variant_id, exc)
            return None

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {executor.submit(worker, vid): vid for vid in variant_ids}
        for idx, future in enumerate(as_completed(futures), start=1):
            result = future.result()
            if result is not None:
                inventory[futures[future]] = result
            if idx % 200 == 0 or idx == len(futures):
                logging.info("Variant inventory fetched: %s/%s", idx, len(futures))

    return inventory


def fetch_storefront_inventory(session: requests.Session, token: str, variant_ids: List[str]) -> Dict[str, Dict[str, object]]:
    inventory: Dict[str, Dict[str, object]] = {}
    headers = dict(session.headers)
    headers.update({"Content-Type": "application/json", "X-Shopify-Storefront-Access-Token": token})

    def chunked(ids: List[str], size: int = 50) -> Iterable[List[str]]:
        for i in range(0, len(ids), size):
            yield ids[i : i + size]

    for batch in chunked(variant_ids):
        gid_list = [f"gid://shopify/ProductVariant/{vid}" for vid in batch]
        query = {
            "query": "query($ids:[ID!]!){nodes(ids:$ids){... on ProductVariant{id quantityAvailable availableForSale inventoryPolicy inventoryManagement}}}",
            "variables": {"ids": gid_list},
        }
        try:
            resp = session.post(STOREFRONT_ENDPOINT, headers=headers, json=query, timeout=30, verify=False)
            if resp.status_code != 200:
                logging.warning("Storefront batch failed %s: %s", resp.status_code, resp.text[:200])
                continue
            data = resp.json().get("data", {}).get("nodes", [])
            for node in data:
                if not node:
                    continue
                gid = node.get("id", "")
                vid = gid.split("/")[-1] if gid else None
                if not vid:
                    continue
                inventory[vid] = {
                    "shopify_inventory": node.get("quantityAvailable"),
                    "inventory_policy": node.get("inventoryPolicy"),
                    "inventory_management": node.get("inventoryManagement"),
                    "availableForSale": node.get("availableForSale"),
                }
            logging.info("Storefront inventory batch complete (%s variants)", len(batch))
        except Exception as exc:  # noqa: BLE001
            logging.warning("Storefront inventory fetch failed: %s", exc)
            continue
    return inventory


def append_preorder_fields(rows: List[dict], per_variant: Dict[str, Dict[str, object]]) -> List[dict]:
    enriched: List[dict] = []
    for row in rows:
        variant_id = str(row.get("variant.id"))
        extras = {f"app.preorder.{k}": v for k, v in per_variant.get(variant_id, {}).items() if v is not None}

        quantity = extras.get("app.preorder.shopify_inventory")
        if quantity is None:
            quantity = per_variant.get(variant_id, {}).get("preorder_stock")
        if quantity is None:
            quantity = row.get("variant.quantityAvailable")
        if quantity is None:
            quantity = 0

        new_row = dict(row)
        new_row["variant.quantityAvailable"] = quantity
        new_row.update(extras)
        enriched.append(new_row)
    return enriched


def dataframe_from_rows(rows: List[dict]):
    try:
        import pandas as pd
    except ImportError as exc:
        raise SystemExit("pandas is required to write the Excel workbook") from exc

    extra_columns: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in CANONICAL_COLUMNS and key not in extra_columns:
                extra_columns.append(key)
    ordered_cols = CANONICAL_COLUMNS + extra_columns
    return pd.DataFrame(rows, columns=ordered_cols)


def write_workbook(preorder_rows: List[dict]) -> Path:
    import pandas as pd

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / f"AGO_app_probe_{timestamp}.xlsx"
    preorder_df = dataframe_from_rows(preorder_rows)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        preorder_df.to_excel(writer, index=False, sheet_name="PreOrderNow")
    return path


def main() -> None:
    configure_logging()
    session = build_session()
    products = fetch_collection_products(session)
    base_rows = build_base_rows(products)

    preorder_settings_raw = fetch_preorder_settings(session)
    preorder_map = decode_preorder_settings_map(preorder_settings_raw)

    missing_variant_ids = [str(row.get("variant.id")) for row in base_rows]
    missing_variant_ids = [vid for vid in missing_variant_ids if preorder_map.get(vid, {}).get("shopify_inventory") is None]

    token = extract_storefront_token(session)
    if token:
        logging.info("Attempting Storefront inventory for %s variants", len(missing_variant_ids))
        sf_inventory = fetch_storefront_inventory(session, token, missing_variant_ids)
        for vid, fields in sf_inventory.items():
            if vid not in preorder_map:
                preorder_map[vid] = {}
            preorder_map[vid].update({k: v for k, v in fields.items() if v is not None})
        missing_variant_ids = [vid for vid in missing_variant_ids if preorder_map.get(vid, {}).get("shopify_inventory") is None]

    if missing_variant_ids:
        logging.info("Fetching Shopify inventory endpoint for %s variants without counts", len(missing_variant_ids))
        variant_inventory = fetch_variant_inventory(session, missing_variant_ids)
        for vid, fields in variant_inventory.items():
            if vid not in preorder_map:
                preorder_map[vid] = {}
            preorder_map[vid].update({k: v for k, v in fields.items() if v is not None})

    preorder_rows = append_preorder_fields(base_rows, preorder_map)
    workbook_path = write_workbook(preorder_rows)
    logging.info("CSV written: %s", workbook_path.resolve())


if __name__ == "__main__":
    main()
