import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import pandas as pd
import requests
import urllib3

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Brand-specific configuration (update here for reuse)
BRAND = "ASKKNY"
BRAND_SLUG = BRAND.lower()
SHOP_DOMAIN = "askk-ny.myshopify.com"
ONLINE_STORE_BASE = "https://askkny.com"
COLLECTION_URL = "https://askkny.com/collections/jeans/products.json"
MARKET_ID = "2005008642"
RR_HOST = "https://app.restockrocket.io"
DEMAND_HOST = "https://app.stoqapp.com/api/v1/external"
DEMAND_TOKEN_ENV_VARS = ["RR_DEMAND_API_TOKEN", "RESTOCK_ROCKET_DEMAND_TOKEN", "STOQ_API_TOKEN"]
DEMAND_PAGE_SIZE = 500

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)

LOG_PATH = OUTPUT_DIR / f"{BRAND_SLUG}_restockrocket_probe.log"


class ProbeError(Exception):
    pass


def setup_logger() -> logging.Logger:
    logger = logging.getLogger(f"{BRAND_SLUG}_restockrocket_probe")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    try:
        file_handler = logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception:
        fallback = OUTPUT_DIR / f"{BRAND_SLUG}_restockrocket_probe.log"
        sys.stderr.write(f"Primary log path unavailable, falling back to {fallback}\n")
        file_handler = logging.FileHandler(fallback, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def build_session() -> requests.Session:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    session.verify = False
    return session


def request_with_retry(
    session: requests.Session,
    url: str,
    logger: logging.Logger,
    *,
    headers: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    retry_statuses: Optional[Set[int]] = None,
    timeout: float = 30.0,
) -> requests.Response:
    retry_statuses = retry_statuses or {429, 500, 502, 503, 504}
    backoff = 1.0
    for attempt in range(5):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout, verify=False)
            if resp.status_code in retry_statuses:
                raise ProbeError(f"Transient status {resp.status_code}")
            resp.raise_for_status()
            return resp
        except Exception as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            retryable = status in retry_statuses or isinstance(exc, ProbeError)
            logger.warning("Request failed (%s) attempt %s: %s", url, attempt + 1, exc)
            if not retryable:
                raise
            time.sleep(backoff)
            backoff *= 2
    raise ProbeError(f"Failed after retries: {url}")


def paginate_collection(session: requests.Session, logger: logging.Logger) -> List[Dict[str, Any]]:
    products: List[Dict[str, Any]] = []
    page = 1
    while True:
        url = f"{COLLECTION_URL}?limit=250&page={page}"
        resp = request_with_retry(session, url, logger)
        data = resp.json()
        batch = data.get("products", [])
        if not batch:
            break
        products.extend(batch)
        logger.info("Fetched page %s with %s products (running total %s)", page, len(batch), len(products))
        page += 1
    return products


def fetch_rr_settings(session: requests.Session, logger: logging.Logger) -> Dict[str, Any]:
    params = {"shop": SHOP_DOMAIN, "translation_locale": "en_us"}
    resp = request_with_retry(session, f"{RR_HOST}/api/v1/setting.json", logger, params=params)
    logger.info("Fetched Restock Rocket settings (id=%s shop_id=%s)", resp.json().get("id"), resp.json().get("shop_id"))
    return resp.json()


def fetch_rr_preorder_ids(session: requests.Session, logger: logging.Logger) -> List[int]:
    params = {"shop": SHOP_DOMAIN, "shopify_market_id": MARKET_ID}
    resp = request_with_retry(session, f"{RR_HOST}/api/v1/embed/preorder_variant_ids.json", logger, params=params)
    data = resp.json()
    logger.info("Fetched %s preorder variant ids", len(data) if isinstance(data, list) else 0)
    return data if isinstance(data, list) else []


def fetch_rr_shipping_texts(session: requests.Session, logger: logging.Logger) -> Dict[str, Any]:
    params = {"shop": SHOP_DOMAIN, "shopify_market_id": MARKET_ID}
    resp = request_with_retry(session, f"{RR_HOST}/api/v1/embed/preorder_variant_shipping_texts.json", logger, params=params)
    data = resp.json()
    logger.info("Fetched preorder shipping texts (%s variants)", len(data) if isinstance(data, dict) else 0)
    return data if isinstance(data, dict) else {}


def fetch_rr_variant_preorder_limits(session: requests.Session, logger: logging.Logger) -> Dict[str, Any]:
    params = {"shop": SHOP_DOMAIN, "shopify_market_id": MARKET_ID}
    resp = request_with_retry(session, f"{RR_HOST}/api/v1/embed/variant_preorder_limits.json", logger, params=params)
    data = resp.json()
    logger.info("Fetched variant preorder limits keys=%s", list(data.keys()) if isinstance(data, dict) else [])
    return data if isinstance(data, dict) else {}


def fetch_rr_variant_data(
    session: requests.Session, logger: logging.Logger, product_id: str
) -> Dict[str, Dict[str, Any]]:
    params = {
        "product_id": product_id,
        "shopify_market_id": MARKET_ID,
        "shop": SHOP_DOMAIN,
        "include_all_variants": "true",
    }
    resp = request_with_retry(session, f"{RR_HOST}/api/v1/embed/variant_data.json", logger, params=params)
    data = resp.json()
    if isinstance(data, dict):
        logger.info("Variant data for product %s returned %s variants", product_id, len(data))
        return data
    logger.info("Variant data for product %s returned unexpected payload", product_id)
    return {}


def resolve_demand_api_token(settings: Optional[Dict[str, Any]], logger: logging.Logger) -> Optional[str]:
    if settings:
        for key in ["external_api_key", "demand_api_key", "api_token", "apiKey"]:
            token = settings.get(key)
            if token:
                logger.info("Using demand API token from settings key '%s'", key)
                return str(token)

    for env_key in DEMAND_TOKEN_ENV_VARS:
        env_val = os.environ.get(env_key)
        if env_val:
            logger.info("Using demand API token from env %s", env_key)
            return env_val

    logger.warning("No demand API token provided; demand report will be skipped")
    return None


def fetch_demand_report(
    session: requests.Session,
    logger: logging.Logger,
    api_token: Optional[str],
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    per_page: int = DEMAND_PAGE_SIZE,
) -> List[Dict[str, Any]]:
    if not api_token:
        return []

    headers = {"X-Auth-Token": api_token}
    params: Dict[str, Any] = {
        "per_page": per_page,
        "page": 1,
    }
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    results: List[Dict[str, Any]] = []
    while True:
        try:
            resp = request_with_retry(
                session,
                f"{DEMAND_HOST}/intents/products_in_demand",
                logger,
                headers=headers,
                params=params,
            )
        except requests.HTTPError as exc:
            status = getattr(exc.response, "status_code", None)
            logger.error("Demand report request failed (page %s, status %s): %s", params.get("page"), status, exc)
            break
        except ProbeError as exc:
            logger.error("Demand report request failed (page %s): %s", params.get("page"), exc)
            break

        payload = resp.json()
        batch = payload.get("product_variants_in_demand", []) if isinstance(payload, dict) else []
        results.extend(batch)
        logger.info(
            "Demand report page %s returned %s variants (running total %s)",
            params.get("page"),
            len(batch),
            len(results),
        )

        if not payload.get("has_next_page"):
            break
        params["page"] = params.get("page", 1) + 1
    return results


def flatten_products(products: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flattened: List[Dict[str, Any]] = []
    for product in products:
        variants = product.get("variants") or []
        for variant in variants:
            flattened.append({"product": product, "variant": variant})
    return flattened


def build_demand_map(demand_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    demand_map: Dict[str, Dict[str, Any]] = {}
    for entry in demand_rows or []:
        vid = entry.get("shopify_variant_id")
        if vid is None:
            continue
        demand_map[str(vid)] = entry
    return demand_map


def build_cached_sets(
    variant_data_map: Dict[str, Dict[str, Any]],
    preorder_ids: List[int],
) -> Dict[str, Any]:
    preorder_id_set = {str(v) for v in preorder_ids}
    in_stock: List[str] = []
    out_stock: List[str] = []
    for vid, payload in variant_data_map.items():
        if payload.get("available") is True:
            in_stock.append(str(vid))
        elif payload.get("available") is False:
            out_stock.append(str(vid))
    return {
        "cachedPreorderVariantIds": sorted(preorder_id_set),
        "cachedInStockVariantIds": in_stock,
        "cachedOutOfStockVariantIds": out_stock,
    }


def normalize_rows(
    product_variants: List[Dict[str, Any]],
    variant_data_map: Dict[str, Dict[str, Any]],
    preorder_ids: List[int],
    shipping_texts: Dict[str, Any],
    preorder_limits: Dict[str, Any],
    settings: Dict[str, Any],
    demand_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    preorder_id_set = {str(v) for v in preorder_ids}
    shipping_text_map = {str(k): v for k, v in (shipping_texts or {}).items()}

    variant_limit_map: Dict[str, Any] = {}
    if isinstance(preorder_limits, dict):
        variant_limit_map = preorder_limits.get("variant_preorder_limits") or {}
        variant_limit_map = {str(k): v for k, v in variant_limit_map.items()}

    cached_sets = build_cached_sets(variant_data_map, preorder_ids)
    settings_json = json.dumps(settings) if settings else None
    preorder_ids_json = json.dumps(preorder_ids) if preorder_ids is not None else None
    preorder_limits_json = json.dumps(preorder_limits) if preorder_limits is not None else None
    shipping_texts_json = json.dumps(shipping_texts) if shipping_texts is not None else None

    for pv in product_variants:
        product = pv["product"]
        variant = pv["variant"]
        vid = str(variant.get("id"))
        rr_variant = variant_data_map.get(vid, {})
        preorder_limit_entry = variant_limit_map.get(vid, {}) if isinstance(variant_limit_map.get(vid, {}), dict) else {}
        demand_entry = demand_map.get(vid, {})

        demand_product_data = demand_entry.get("product_data") if isinstance(demand_entry, dict) else None
        demand_variant_data = demand_entry.get("variant_data") if isinstance(demand_entry, dict) else None
        demand_product_json = json.dumps(demand_product_data) if demand_product_data is not None else None
        demand_variant_json = json.dumps(demand_variant_data) if demand_variant_data is not None else None

        rows.append(
            {
                "product.id": str(product.get("id")),
                "product.handle": product.get("handle"),
                "product.published_at": product.get("published_at"),
                "product.created_at": product.get("created_at"),
                "product.title": product.get("title"),
                "product.productType": product.get("product_type"),
                "product.tags_all": ",".join(product.get("tags", [])) if isinstance(product.get("tags"), list) else product.get("tags"),
                "product.vendor": product.get("vendor"),
                "product.descriptionHtml": product.get("body_html"),
                "variant.title": variant.get("title"),
                "variant.option1": variant.get("option1"),
                "variant.option2": variant.get("option2"),
                "variant.option3": variant.get("option3"),
                "variant.price": variant.get("price"),
                "variant.compare_at_price": variant.get("compare_at_price"),
                "variant.available": variant.get("available"),
                "variant.quantityAvailable": rr_variant.get("inventory_quantity"),
                "rr.inventory_policy": rr_variant.get("inventory_policy"),
                "rr.variant_available": rr_variant.get("available"),
                "rr.variant_price": rr_variant.get("price"),
                "rr.preorder_count": preorder_limit_entry.get("preorder_count") if isinstance(preorder_limit_entry, dict) else None,
                "rr.preorder_max_count": preorder_limit_entry.get("preorder_max_count")
                if isinstance(preorder_limit_entry, dict)
                else None,
                "rr.preorder_count_market": preorder_limit_entry.get("preorder_count_market")
                if isinstance(preorder_limit_entry, dict)
                else None,
                "rr.preorder_max_count_market": preorder_limit_entry.get("preorder_max_count_market")
                if isinstance(preorder_limit_entry, dict)
                else None,
                "product.totalInventory": None,
                "variant.id": vid,
                "variant.sku": variant.get("sku"),
                "variant.barcode": variant.get("barcode"),
                "product.images[0].src": product.get("images", [{}])[0].get("src") if product.get("images") else None,
                "product.onlineStoreUrl": f"{ONLINE_STORE_BASE}/products/{product.get('handle')}",
                "rr.preorder_flag": vid in preorder_id_set,
                "rr.shipping_text": shipping_text_map.get(vid),
                "rr.shipping_text_market": shipping_text_map.get(vid),
                "rr.preorder_limit": json.dumps(variant_limit_map.get(vid)) if variant_limit_map.get(vid) is not None else None,
                "rr.inventory_source": "variant_data_api" if rr_variant else None,
                "rr.settings": settings_json,
                "rr.cachedSettings": settings_json,
                "rr.cachedPreorderVariantIds": preorder_ids_json,
                "rr.cachedVariantPreorderLimits": preorder_limits_json,
                "rr.cachedVariantShippingTexts": shipping_texts_json,
                "rr.cachedInStockVariantIds": json.dumps(cached_sets.get("cachedInStockVariantIds")),
                "rr.cachedOutOfStockVariantIds": json.dumps(cached_sets.get("cachedOutOfStockVariantIds")),
                "rr.selected_variant_id": vid,
                "rr.variant_payload_raw": json.dumps(rr_variant) if rr_variant else None,
                "demand.total": demand_entry.get("total") if demand_entry else None,
                "demand.pending": demand_entry.get("pending") if demand_entry else None,
                "demand.last_requested_at": demand_entry.get("last_requested_at") if demand_entry else None,
                "demand.shopify_product_id": demand_entry.get("shopify_product_id") if demand_entry else None,
                "demand.shopify_inventory_item_id": demand_entry.get("shopify_inventory_item_id") if demand_entry else None,
                "demand.product_data": demand_product_json,
                "demand.variant_data": demand_variant_json,
                "demand.source": "demand_report_api" if demand_entry else None,
            }
        )
    return rows


def write_excel(rows: List[Dict[str, Any]], demand_entries: List[Dict[str, Any]], logger: logging.Logger) -> Path:
    if not rows:
        raise ProbeError("No rows to write")
    df = pd.DataFrame(rows)
    canonical_cols = [
        "product.id",
        "product.handle",
        "product.published_at",
        "product.created_at",
        "product.title",
        "product.productType",
        "product.tags_all",
        "product.vendor",
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
    extras = [c for c in df.columns if c not in canonical_cols]
    ordered_cols = canonical_cols + extras
    df = df[ordered_cols]

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_path = OUTPUT_DIR / f"{BRAND.upper()}_RESTOCKROCKET_{ts}.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="RestockRocket", index=False)
        if demand_entries:
            pd.DataFrame(demand_entries).to_excel(writer, sheet_name="DemandReport", index=False)
    logger.info("CSV written: %s", out_path.resolve())
    return out_path


def main() -> None:
    logger = setup_logger()
    session = build_session()
    logger.info("Starting Restock Rocket probe for %s (collection-level)", BRAND)

    products = paginate_collection(session, logger)
    if not products:
        raise ProbeError("No products found in collection")

    settings = fetch_rr_settings(session, logger)
    preorder_ids = fetch_rr_preorder_ids(session, logger)
    shipping_texts = fetch_rr_shipping_texts(session, logger)
    preorder_limits = fetch_rr_variant_preorder_limits(session, logger)
    demand_token = resolve_demand_api_token(settings, logger)
    demand_entries = fetch_demand_report(session, logger, demand_token)

    product_variants = flatten_products(products)

    variant_data_map: Dict[str, Dict[str, Any]] = {}
    for product in products:
        pid = str(product.get("id"))
        if not pid:
            continue
        data = fetch_rr_variant_data(session, logger, pid)
        for vid, payload in data.items():
            variant_data_map[str(vid)] = payload

    rows = normalize_rows(
        product_variants,
        variant_data_map,
        preorder_ids,
        shipping_texts,
        preorder_limits,
        settings,
        build_demand_map(demand_entries),
    )
    out_path = write_excel(rows, demand_entries, logger)
    logger.info("Probe completed. Output at %s", out_path.resolve())


if __name__ == "__main__":
    main()
