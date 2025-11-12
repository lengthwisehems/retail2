# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import html
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse, urlunparse

import requests

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "triarchy_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "triarchy_run.log"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def configure_logging() -> logging.Logger:
    logger = logging.getLogger("triarchy")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    selected_path: Path | None = None
    fallback_used = False

    for path in (LOG_PATH, FALLBACK_LOG_PATH):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(path, encoding="utf-8")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            selected_path = path
            if path != LOG_PATH:
                fallback_used = True
            break
        except (OSError, PermissionError) as exc:
            print(
                f"WARNING: Unable to open log file {path}: {exc}. Continuing without this destination.",
                flush=True,
            )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if selected_path is None:
        logger.warning("File logging disabled; continuing with console logging only.")
    elif fallback_used:
        logger.warning("Primary log path %s unavailable. Using fallback log at %s.", LOG_PATH, selected_path)

    return logger


LOGGER = configure_logging()

PRIMARY_HOSTS = [
    "https://triarchy.com",
    "https://triarchydev.myshopify.com",
]
BASE = PRIMARY_HOSTS[0]
COLL = "/collections/jeans"

HEADERS = {"User-Agent": "inventory-research/1.0 (+length-wise)"}
TIMEOUT = 30
RETRIES = 2
SLEEP = 0.25
MAX_PAGES = 10  # collection pages to try for handles and products.json

CSV_HEADERS = [
    "Style Id","Handle","Product","Product Type","Vendor","Description",
    "Variant Title","Color","Size","Rise","Inseam","Leg Opening",
    "Price","Compare at Price","Available for Sale","Quantity Available",
    "Quantity of style","SKU","Image URL","SKU URL"
]

def log(message: str, level: int = logging.INFO) -> None:
    LOGGER.log(level, message)


def _candidate_urls(url: str) -> Iterable[str]:
    parsed = urlparse(url)
    if parsed.scheme and parsed.netloc:
        for host in PRIMARY_HOSTS:
            host_parsed = urlparse(host)
            yield urlunparse(
                (
                    host_parsed.scheme,
                    host_parsed.netloc,
                    parsed.path,
                    parsed.params,
                    parsed.query,
                    parsed.fragment,
                )
            )
    else:
        for host in PRIMARY_HOSTS:
            yield urljoin(host, url)


def http_get(url: str, params=None, accept: str | None = None) -> str:
    headers = dict(HEADERS)
    if accept:
        headers["Accept"] = accept
    last_exc: Exception | None = None
    for candidate in _candidate_urls(url):
        for attempt in range(1, RETRIES + 2):
            try:
                response = requests.get(candidate, params=params, headers=headers, timeout=TIMEOUT)
                if response.status_code == 200:
                    if candidate != url:
                        log(f"Fetched {url} via fallback host {candidate}")
                    return response.text
                log(
                    f"GET {candidate} returned {response.status_code} (attempt {attempt}/{RETRIES + 1})",
                    level=logging.WARNING,
                )
            except requests.RequestException as exc:  # pragma: no cover - network failures
                last_exc = exc
                log(
                    f"GET {candidate} failed: {exc} (attempt {attempt}/{RETRIES + 1})",
                    level=logging.WARNING,
                )
            time.sleep(0.4)
        log(f"Switching host for {url}", level=logging.INFO)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"Unable to fetch {url}")


def http_get_json(url, params=None):
    text = http_get(url, params=params, accept="application/json")
    return json.loads(text)

# ---------------------------------------
# Discover handles from collection HTML + collection JSON
# ---------------------------------------
HREF_RE = re.compile(r'href="(/products/[^"?#]+)"')

def handles_from_collection_html():
    found = set()
    for page in range(1, MAX_PAGES + 1):
        url = urljoin(BASE, COLL) + (f"?page={page}" if page > 1 else "")
        txt = http_get(url)
        handles = [m.group(1).split("/")[-1] for m in HREF_RE.finditer(txt)]
        handles = [h for h in handles if h]
        if not handles and page > 1:
            break
        found.update(handles)
        log(f"[HTML] page={page} handles={len(handles)}")
        time.sleep(SLEEP)
    return found

def products_from_collection_json():
    product_map = {}
    for page in range(1, MAX_PAGES + 1):
        data = http_get_json(urljoin(BASE, COLL + "/products.json"), params={"limit": 250, "page": page})
        products = data.get("products") or data.get("items") or []
        if not products:
            break
        for p in products:
            if not p.get("handle"):
                continue
            product_map[p["handle"]] = p
        log(f"[JSON] page={page} products={len(products)}")
        time.sleep(SLEEP)
    return product_map

# ---------------------------------------
# PDP parsing
# ---------------------------------------
TAG_RE = re.compile(r"<[^>]+>")

def strip_html(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    s = TAG_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def fraction_to_decimal(text):
    s = (text or "").strip().replace("\xa0", " ")
    m = re.match(r"^\s*(\d+)(?:\s+(\d+)\s*/\s*(\d+))?\s*$", s)
    if m:
        whole = int(m.group(1))
        if m.group(2) and m.group(3):
            num = int(m.group(2)); den = int(m.group(3)) or 1
            return round(whole + num/den, 4)
        return float(whole)
    try:
        return float(s)
    except Exception:
        return text

def find_inline_variant_json(pdp_html: str):
    """
    Look for <variant-selects> ... <script>JSON</script> ... </variant-selects>
    Return: dict with "variants" list if found, else None
    """
    # narrow to the variant-selects block first
    vs = re.search(r"<variant-selects[^>]*>.*?</variant-selects>", pdp_html, flags=re.S|re.I)
    if not vs:
        # fallback: any script that contains inventory_quantity and looks like a variants array
        for s in re.finditer(r"<script\b[^>]*>(.*?)</script>", pdp_html, flags=re.S|re.I):
            block = s.group(1) or ""
            if "inventory_quantity" not in block and '"variants"' not in block:
                continue
            # try to extract a product object with "variants":[...]
            m_obj = re.search(r"\{[\s\S]{0,200}\"variants\"\s*:\s*\[[\s\S]*?\}\s*\]", block)
            if m_obj:
                try:
                    return json.loads(m_obj.group(0))
                except Exception:
                    pass
            # fallback: first JSON array
            m_arr = re.search(r"\[\{[\s\S]*?\}\]", block)
            if m_arr:
                try:
                    arr = json.loads(m_arr.group(0))
                    if isinstance(arr, list) and arr and isinstance(arr[0], dict):
                        return {"variants": arr}
                except Exception:
                    pass
        return None

    chunk = vs.group(0)
    # find the first <script> inside the variant-selects
    sc = re.search(r"<script\b[^>]*>(.*?)</script>", chunk, flags=re.S|re.I)
    if not sc:
        return None
    txt = sc.group(1) or ""

    # try as object with "variants":[...]
    m_obj = re.search(r"\{[\s\S]{0,200}\"variants\"\s*:\s*\[[\s\S]*?\}\s*\]", txt)
    if m_obj:
        try:
            return json.loads(m_obj.group(0))
        except Exception:
            pass

    # fallback: array of variants
    m_arr = re.search(r"\[\{[\s\S]*?\}\]", txt)
    if m_arr:
        try:
            arr = json.loads(m_arr.group(0))
            if isinstance(arr, list) and arr and isinstance(arr[0], dict):
                return {"variants": arr}
        except Exception:
            pass

    return None

def find_description(pdp_html: str) -> str:
    # try to capture the accordion content wrapper first paragraph
    m = re.search(r'<div[^>]*class="[^"]*accordion-content-wrapper[^"]*"[^>]*>([\s\S]*?)</div>', pdp_html, flags=re.I)
    if m:
        block = m.group(1)
        # first paragraph text
        p = re.search(r"<p[^>]*>([\s\S]*?)</p>", block, flags=re.I)
        if p:
            return strip_html(p.group(1))[:2000]
        return strip_html(block)[:2000]
    # fallback: meta description
    md = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', pdp_html, flags=re.I)
    if md:
        return html.unescape(md.group(1)).strip()
    return ""

def find_measure(pdp_html: str, label_regex: str):
    # search label then the nearest number after it
    m = re.search(label_regex, pdp_html, flags=re.I)
    if not m:
        return ""
    window = pdp_html[m.end(): m.end()+400]
    nm = re.search(r"(\d+\s+\d+/\d+|\d+\.\d+|\d+)", window)
    return nm.group(1).strip() if nm else ""

def format_price(value):
    # accept int cents or string dollars
    if value is None:
        return ""
    try:
        # if already a string with $, return
        if isinstance(value, str):
            v = value.strip()
            if v.startswith("$"):
                return v
            # looks like plain dollars string
            f = float(v)
            return f"${f:,.2f}"
        # assume integer cents
        ival = int(value)
        return f"${ival/100.0:,.2f}"
    except Exception:
        # fallback plain string
        return str(value)

def last_word_product_type(title):
    if not title:
        return ""
    parts = [w for w in re.split(r"\s+", title.strip()) if w]
    return parts[-1] if parts else ""

# ---------------------------------------
# Main
# ---------------------------------------
def main():
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"TRIARCHY_{ts}.csv"
    log(f"=== Run {ts} ===")

    handles_html = handles_from_collection_html()
    products_map = products_from_collection_json()
    handles = sorted(set(list(handles_html) + list(products_map.keys())))

    log(f"[DISC] total handles={len(handles)}")

    with output_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        w.writeheader()

        for handle in handles:
            try:
                # Product level fields from products.json if available
                meta = products_map.get(handle)
                if not meta:
                    # fallback to /products/<handle>.js for minimal info
                    try:
                        js = http_get_json(urljoin(BASE, f"/products/{handle}.js"))
                        meta = {
                            "id": js.get("id"),
                            "handle": js.get("handle"),
                            "title": js.get("title"),
                            "vendor": js.get("vendor"),
                            "product_type": js.get("type") or js.get("product_type"),
                            "images": [{"src": (js.get("featured_image") or {}).get("src")}] if js.get("featured_image") else []
                        }
                    except Exception as e:
                        log(f"[MISS] meta not found for {handle}: {e}")
                        meta = {"handle": handle}

                product_id   = meta.get("id")
                product_title= meta.get("title") or ""
                vendor       = meta.get("vendor") or ""
                product_type = meta.get("product_type") or last_word_product_type(product_title)
                image_url    = ""
                if meta.get("images"):
                    # try first product image
                    img = meta["images"][0]
                    if isinstance(img, dict) and img.get("src"):
                        image_url = img["src"]

                # Fetch PDP HTML once
                pdp_html = http_get(urljoin(BASE, f"/products/{handle}"), accept="text/html")

                # Inline variant JSON with inventory_quantity
                var_obj = find_inline_variant_json(pdp_html)
                variants = (var_obj or {}).get("variants") or []
                if not variants:
                    log(f"[WARN] no inline variants for {handle}")
                    continue

                # Description and measurements
                desc = find_description(pdp_html)
                rise_txt   = find_measure(pdp_html, r"Front\s*Rise\s*:?")
                inseam_txt = find_measure(pdp_html, r"Inseam\s*:?")
                leg_txt    = find_measure(pdp_html, r"(?:Leg\s*Opening|Leg opening)\s*:?")
                rise_val   = fraction_to_decimal(rise_txt) if rise_txt else ""
                inseam_val = fraction_to_decimal(inseam_txt) if inseam_txt else ""
                leg_val    = fraction_to_decimal(leg_txt) if leg_txt else ""

                # --- compute style-level total first ---
                norm_variants = []
                style_total = 0
                has_qty = False

                for v in variants:
                    inv_val = v.get("inventory_quantity")
                    inv_int = None
                    if inv_val is not None:
                        try:
                            inv_int = int(inv_val)
                            style_total += inv_int
                            has_qty = True
                        except Exception:
                            inv_int = None
                    norm_variants.append((v, inv_int))
                # --- then write rows, reusing the same total on each row ---
                for v, inv_int in norm_variants:
                    vid = str(v.get("id") or "")
                    vtitle = v.get("title") or v.get("name") or ""
                    sku = v.get("sku") or ""
                    available = v.get("available") if "available" in v else None

                    color = v.get("option1") or ""
                    size  = v.get("option2") or ""

                    price_cents = v.get("price")
                    compare_cents = v.get("compare_at_price")

                    w.writerow({
                        "Style Id": product_id,
                        "Handle": handle,
                        "Product": product_title,
                        "Product Type": product_type,
                        "Vendor": vendor,
                        "Description": desc,
                        "Variant Title": vtitle,
                        "Color": color,
                        "Size": size,
                        "Rise": rise_val,
                        "Inseam": inseam_val,
                        "Leg Opening": leg_val,
                        "Price": format_price(price_cents),
                        "Compare at Price": format_price(compare_cents),
                        "Available for Sale": available,
                        "Quantity Available": inv_int,
                        "Quantity of style": style_total if has_qty else "",
                        "SKU": vid,
                        "Image URL": image_url,
                        "SKU URL": f"{BASE}/collections/jeans/products/{handle}"
                    })

                log(f"[OK ] {handle} variants={len(variants)} qty_total={'NA' if not has_qty else style_total}")
                time.sleep(SLEEP)

            except Exception as e:
                log(f"[ERR] handle={handle} -> {e}")

    log(f"[DONE] file={output_path}")
    print(f"Wrote {output_path}")

if __name__ == "__main__":
    main()
