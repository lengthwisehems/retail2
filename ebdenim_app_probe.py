"""Probe EB Denim pants collection across multiple embedded apps.

This script is a discovery tool (not a production scraper). It enumerates the
`/collections/pants` products and attempts to pull every field exposed by the
Globo, Rebuy, Restock, Avada stock countdown, Bundler, and Postscript apps. The
goal is to report what data exists, whether it is style- or variant-level,
where any required tokens come from, and whether a single request or per-PDP
fetch is needed.

Per the repository playbook, this script only prepares the probing logic. Run
manually once you are ready to inspect the outputs.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_PATH_PRIMARY = BASE_DIR / "ebdenim_probe.log"
LOG_PATH_FALLBACK = OUTPUT_DIR / "ebdenim_probe.log"


def configure_logging() -> None:
    handlers: List[logging.Handler] = []
    try:
        handlers.append(logging.FileHandler(LOG_PATH_PRIMARY))
    except OSError:
        # Fall back to the Output directory and emit a warning to the console so the
        # active destination is clear during execution.
        fallback_handler = logging.FileHandler(LOG_PATH_FALLBACK)
        handlers.append(fallback_handler)
        stream_handler = logging.StreamHandler()
        handlers.append(stream_handler)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=handlers,
        )
        logging.warning("Primary log path unavailable, using Output directory")
        return

    # Preferred path succeeded; also emit to stdout for real-time visibility.
    handlers.append(logging.StreamHandler())
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                " AppleWebKit/537.36 (KHTML, like Gecko)"
                " Chrome/123.0.0.0 Safari/537.36"
            )
        }
    )
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def timestamped_path(stem: str, suffix: str = "json") -> Path:
    ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    return OUTPUT_DIR / f"{stem}_{ts}.{suffix}"


def clean_html(text: str) -> str:
    return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)


def paginate_collection_handles(session: requests.Session) -> List[str]:
    """Enumerate handles from the pants collection feed."""
    handles: List[str] = []
    page = 1
    while True:
        url = f"https://www.ebdenim.com/collections/pants/products.json?limit=250&page={page}"
        logging.info("Fetching collection page %s", url)
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        products = payload.get("products") or []
        if not products:
            break
        for prod in products:
            handle = prod.get("handle")
            if handle:
                handles.append(handle)
        page += 1
        time.sleep(0.5)
    logging.info("Collected %d handles", len(handles))
    return handles


RESTOCK_VARIANT_BLOCK_RE = re.compile(r"variants\s*:\s*\[(?P<body>.*?)\]\s*,?\s*\}", re.S)
RESTOCK_VARIANT_OBJ_RE = re.compile(
    r"\{[^{}]*?\bid\s*:\s*(?P<id>\d+)[^{}]*?\bquantity\s*:\s*(?P<qty>-?\d+)[^{}]*?\}",
    re.S,
)


def extract_restock_quantities(html_text: str) -> Dict[str, Dict[str, int]]:
    if "_ReStockConfig.product" not in html_text:
        return {}
    match = RESTOCK_VARIANT_BLOCK_RE.search(html_text)
    if not match:
        return {}
    body = match.group("body")
    out: Dict[str, Dict[str, int]] = {}
    for mo in RESTOCK_VARIANT_OBJ_RE.finditer(body):
        vid = mo.group("id")
        qty = int(mo.group("qty"))
        out[vid] = {"quantity": qty, "notify_me": max(-qty, 0), "quantity_available": max(qty, 0)}
    return out


AVADA_INV_RE = re.compile(r"AVADA_INVQTY\s*=\s*\{(?P<body>.*?)\}", re.S)
AVADA_PAIR_RE = re.compile(r"(\d+)\s*:\s*(-?\d+)")


def extract_avada_inventory(html_text: str) -> Dict[str, int]:
    m = AVADA_INV_RE.search(html_text)
    if not m:
        return {}
    body = m.group("body")
    out: Dict[str, int] = {}
    for vid, qty in AVADA_PAIR_RE.findall(body):
        out[vid] = int(qty)
    return out


def extract_globo_from_html(html_text: str) -> Dict[str, object]:
    """Capture any Globo config blobs embedded in the page."""
    snippets: Dict[str, object] = {}
    if "globo" not in html_text.lower():
        return snippets
    soup = BeautifulSoup(html_text, "html.parser")
    for script in soup.find_all("script"):
        content = script.string or ""
        if "globo" in content.lower():
            key = f"script_{len(snippets) + 1}"
            snippets[key] = content.strip()
    return snippets


def fetch_rebuy_metadata(session: requests.Session) -> Dict[str, object]:
    """Pull top-level Rebuy configs discovered from the network hints."""
    base: Dict[str, object] = {}
    endpoints = {
        "user_config": "https://cached.rebuyengine.com/api/v1/user/config?shop=ebdenim-com.myshopify.com",
        "widget_settings": "https://cached.rebuyengine.com/api/v1/widgets/settings?id=188811&cache_key=1762912273",
        "theme": "https://rebuyengine.com/api/v1/v1_theme/id/172253053175?key=1b2815331f7a0f6b6adec0f17736d2b7dc52cc8d&cache_key=1762912273",
        "custom": "https://rebuyengine.com/api/v1/custom/id/199459?key=1b2815331f7a0f6b6adec0f17736d2b7dc52cc8d&limit=3&url=https%3A%2F%2Fwww.ebdenim.com%2Fcollections%2Fpants&shopify_product_ids=&shopify_variant_ids=&shopify_collection_ids=&shopify_order_ids=&uuid=probe&cart_token=probe&cart_subtotal=0&cart_count=0&cart_line_count=0&cart_item_count=0&cart%5Btoken%5D=probe&cart%5Bsubtotal%5D=0&cart%5Bline_count%5D=0&cart%5Bitem_count%5D=0&cart%5Battributes%5D=%257B%2522_source%2522%253A%2522Rebuy%2522%252C%2522_attribution%2522%253A%2522Smart%2520Cart%25202.0%2522%257D&cart%5Bnotes%5D=&cache_key=1762912273&product_groups=yes",
    }
    for label, url in endpoints.items():
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            base[label] = resp.json()
            logging.info("Fetched Rebuy %s", label)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Rebuy %s failed: %s", label, exc)
    return base


def fetch_bundler_status(session: requests.Session) -> Dict[str, object]:
    url = "https://bundler.nice-team.net/app/shop/status/ebdenim-com.myshopify.com.js?1763665492"
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        text = resp.text
        # The status endpoint returns JS; attempt to extract JSON object literal
        m = re.search(r"=\s*(\{.*\})\s*;?\s*$", text, re.S)
        if m:
            return json.loads(m.group(1))
        return {"raw": text}
    except Exception as exc:  # noqa: BLE001
        logging.warning("Bundler status fetch failed: %s", exc)
        return {}


def fetch_postscript_metadata(session: requests.Session) -> Dict[str, object]:
    out: Dict[str, object] = {}
    endpoints = {
        "config": "https://sdk-api-proxy.postscript.io/sdk/config?shop_id=575770",
        "desktop": "https://sdk.postscript.io/desktop?shopId=575770&shopShop=ebdenim-com&origin=https%3A%2F%2Fwww.ebdenim.com%2Fcollections%2Fpants",
        "popups": "https://sdk-api-proxy.postscript.io/v2/public/popups/575770/desktop",
    }
    for label, url in endpoints.items():
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            # Some endpoints return JS; attempt JSON decode with fallback to raw text
            try:
                out[label] = resp.json()
            except json.JSONDecodeError:
                out[label] = resp.text
            logging.info("Fetched Postscript %s", label)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Postscript %s failed: %s", label, exc)
    return out


def fetch_pdp_html(session: requests.Session, handle: str) -> str:
    url = f"https://www.ebdenim.com/collections/pants/products/{handle}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def probe_product(session: requests.Session, handle: str) -> Dict[str, object]:
    html = fetch_pdp_html(session, handle)
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("title")
    page_title = clean_html(title_tag.get_text()) if title_tag else ""
    restock = extract_restock_quantities(html)
    avada = extract_avada_inventory(html)
    globo = extract_globo_from_html(html)
    return {
        "handle": handle,
        "page_title": page_title,
        "restock": restock,
        "avada_inventory": avada,
        "globo_scripts": globo,
    }


def main() -> None:
    configure_logging()
    session = build_session()
    handles = paginate_collection_handles(session)

    aggregate: Dict[str, object] = {
        "handles": handles,
        "rebuy_metadata": fetch_rebuy_metadata(session),
        "bundler_status": fetch_bundler_status(session),
        "postscript_metadata": fetch_postscript_metadata(session),
        "products": [],
    }

    for idx, handle in enumerate(handles, 1):
        logging.info("Probing %s (%d/%d)", handle, idx, len(handles))
        try:
            aggregate["products"].append(probe_product(session, handle))
        except Exception as exc:  # noqa: BLE001
            logging.warning("Probe failed for %s: %s", handle, exc)
        time.sleep(0.5)

    out_path = timestamped_path("ebdenim_app_probe")
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(aggregate, f, indent=2)
    logging.info("Probe summary written: %s", out_path.resolve())


if __name__ == "__main__":
    main()
