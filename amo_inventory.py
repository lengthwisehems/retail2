import csv, json, re, time
from datetime import datetime
from html import unescape
from pathlib import Path
from urllib.parse import urlparse

import requests

# ------------- config -------------
BASE_URL = "https://amodenim.com"
PRIMARY_HOST = urlparse(BASE_URL).netloc or "amodenim.com"
HOST_FALLBACKS = {
    "amodenim.com": ["www.amodenim.com"],
    "www.amodenim.com": ["amodenim.com"],
}
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "AMO_run.log"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_SIZES = {
    "21","22","23","24","25","26","27","28","29","30","31","32",
    "XS","S","M","L","XXS","XXL","1XL","2XL","3XL","4XL","5XL",
}

CSV_COLS = [
    "Style Id","Handle","Published At","Product","Style Name","Product Type","Tags","Vendor",
    "Description","Variant Title","Color","Size","Rise","Back Rise","Inseam","Leg Opening",
    "Price","Compare at Price","Quantity Price Breaks","Available for Sale","Quantity Available",
    "Old Quantity Available","Quantity of style","SKU - Shopify","SKU - Brand","Barcode","Image URL",
    "SKU URL"
]
# ----------------------------------

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/127.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": UA}

session = requests.Session()
session.headers.update(HEADERS)

TRANSIENT_STATUSES = {429, 500, 502, 503, 504}
DNS_ERROR_KEYWORDS = [
    "failed to resolve",
    "name or service not known",
    "temporary failure in name resolution",
    "getaddrinfo failed",
    "nodename nor servname provided",
    "unreachable network",
]

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    except Exception:
        pass


def iter_host_candidates(primary):
    seen = set()
    queue = [primary]
    queue.extend(HOST_FALLBACKS.get(primary.lower(), []))
    for host in queue:
        if not host:
            continue
        key = host.lower()
        if key in seen:
            continue
        seen.add(key)
        yield host


def is_name_resolution_error(exc):
    parts = []
    current = exc
    visited = set()
    while current and id(current) not in visited:
        visited.add(id(current))
        parts.append(str(current))
        parts.append(repr(current))
        current = getattr(current, "__cause__", None)
    combined = " ".join(parts).lower()
    return any(keyword in combined for keyword in DNS_ERROR_KEYWORDS)


def request_with_retries(path, *, params=None, timeout=30):
    if not path.startswith("/"):
        path = "/" + path
    last_exc = None
    hosts = list(iter_host_candidates(PRIMARY_HOST))
    for idx, host in enumerate(hosts):
        url = f"https://{host}{path}"
        for attempt in range(5):
            try:
                resp = session.get(url, params=params, timeout=timeout)
                if resp.status_code in TRANSIENT_STATUSES:
                    raise requests.HTTPError(f"transient status {resp.status_code}")
                resp.raise_for_status()
                if idx > 0:
                    log(f"[fallback] switched host to {url}")
                return resp
            except Exception as exc:
                last_exc = exc
                if attempt < 4:
                    sleep_for = min(8.0, 1.0 * (2 ** attempt))
                    log(f"[retry] {url} ({exc}); sleeping {sleep_for:.1f}s")
                    time.sleep(sleep_for)
                    continue
                log(f"[error] {url} failed after retries: {exc}")
                break
        if idx + 1 < len(hosts) and last_exc and is_name_resolution_error(last_exc):
            next_host = hosts[idx + 1]
            log(f"[retry] switching host for {path} -> https://{next_host}{path}")
            continue
        break
    if last_exc:
        raise last_exc
    raise RuntimeError("unreachable retry loop")


def dollars_from_cents(val):
    """
    Super-lazy extractor:
      - Takes whatever the source is (int/float/string like 21800, 218.00, "$218.00")
      - Returns the first number it finds, as a plain number string (no $)
      - If nothing numeric is present, returns "".
    """
    if val in (None, ""):
        return ""
    s = str(val)
    m = re.search(r'[-+]?\d[\d,]*(?:\.\d+)?', s)
    return m.group(0).replace(",", "") if m else ""


def clean_text(html):
    if not html:
        return ""
    s = unescape(html)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def pubdate_mmddyy(iso):
    if not iso:
        return ""
    try:
        # Handles offsets like 2025-09-16T08:50:39-07:00 and trailing Z
        dt = datetime.fromisoformat(iso.replace("Z", ""))
        return dt.strftime("%m/%d/%y")
    except Exception:
        # Fallback: just extract the date portion
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})", iso)
        if m:
            y, mo, d = m.groups()
            return f"{mo}/{d}/{y[-2:]}"
        return ""

def product_type_from_title_or_field(title, product_type_field):
    # Spec says take product_type from products.json (DENIM).
    # Use that directly, but keep fallback from title if blank.
    if product_type_field:
        return product_type_field
    if not title:
        return ""
    before_dash = title.split(" - ")[0]
    parts = before_dash.split()
    return parts[-1] if parts else ""

def color_from_handle(handle):
    # take the word(s) after the first hyphen. Example: libby-smokin -> Smokin
    if not handle or "-" not in handle:
        return ""
    right = handle.split("-", 1)[1]
    return right.replace("-", " ").strip().title()

def parse_size_from_sku(sku):
    if not sku:
        return ""

    tokens = sku.strip().split()
    if len(tokens) < 3:
        return ""

    remainder = " ".join(tokens[2:]).strip()
    if remainder in ALLOWED_SIZES:
        return remainder

    for token in reversed(tokens[2:]):
        candidate = token.strip()
        if candidate in ALLOWED_SIZES:
            return candidate

    return ""

def fetch_all_products():
    all_items = []
    page = 1
    while True:
        params = {"page": page, "limit": 250}
        resp = request_with_retries("/collections/denim/products.json", params=params)
        data = resp.json()
        products = data.get("products") or []
        if not products:
            break
        all_items.extend(products)
        log(f"[collections/denim] page {page} -> {len(products)} products")
        page += 1
        time.sleep(0.2)
    log(f"[collections/denim] TOTAL products: {len(all_items)}")
    return all_items

def fetch_pdp_html(handle):
    resp = request_with_retries(f"/products/{handle}")
    return resp.text

def fetch_product_json(handle):
    resp = request_with_retries(f"/products/{handle}.json")
    return resp.json().get("product") or {}

def frac_to_decimal(txt):
    """
    Accepts: 14, 14.5, 14 1/2, 10 1/4 etc.
    Returns a string like '14.5'. If no match return ''.
    """
    if not txt:
        return ""
    txt = txt.strip()
    # number with optional fractional part "a b/c"
    m = re.search(r"(\d+)(?:\s+(\d+)\s*/\s*(\d+))?(?:\.(\d+))?", txt)
    if not m:
        return ""
    whole = int(m.group(1))
    num = m.group(2)
    den = m.group(3)
    dot = m.group(4)
    val = float(whole)
    if num and den:
        try:
            val += int(num)/int(den)
        except Exception:
            pass
    if dot:
        # if both fraction and dot appear, dot wins
        try:
            val = float(f"{whole}.{dot}")
        except Exception:
            pass
    return f"{val:.2f}".rstrip("0").rstrip(".")

def extract_measurements(html):
    """
    The details text contains a sentence like:
    Front rise = 13, Back rise = 14.5, Inseam = 28, Leg opening = 19
    We will parse with case-insensitive regex.
    """
    # get all text in Details block to increase odds
    # simple approach: collapse html to text then search
    text = clean_text(html).lower()

    def grab(label):
        m = re.search(label + r"\s*=\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+)?)", text, re.I)
        return frac_to_decimal(m.group(1)) if m else ""

    front = grab(r"front\s*rise")
    back  = grab(r"back\s*rise")
    inseam = grab(r"inseam")
    leg = grab(r"leg\s*opening")
    return front, back, inseam, leg

def run():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") # unique run marker
    csv_path = OUTPUT_DIR / f"AMO_{timestamp}.csv"

    products = fetch_all_products()

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS)
        w.writeheader()

        for idx, p in enumerate(products, 1):
            style_id = p.get("id")
            handle = p.get("handle") or ""
            published_at = pubdate_mmddyy(p.get("published_at") or "")
            title = p.get("title") or ""
            vendor = p.get("vendor") or ""
            product_type = product_type_from_title_or_field(title, p.get("product_type"))
            description = clean_text(p.get("body_html") or "")
            color = color_from_handle(handle)

            tags_field = p.get("tags") or ""
            if isinstance(tags_field, list):
                tags = ", ".join(tags_field)
            else:
                tags = tags_field

            images = p.get("images") or []
            image_url = images[0].get("src") if images else ""
            sku_url = f"{BASE_URL}/products/{handle}"

            try:
                product_json = fetch_product_json(handle)
            except Exception as e:
                log(f"[WARN] product json fetch failed for {handle}: {e}")
                product_json = {}

            quantity_price_breaks_map = {}
            qty_map = {}
            old_qty_map = {}
            barcode_map = {}

            if product_json:
                variants_json = product_json.get("variants") or []
                for vj in variants_json:
                    vid = str(vj.get("id") or "")
                    if not vid:
                        continue
                    qty_map[vid] = vj.get("inventory_quantity")
                    old_qty_map[vid] = vj.get("old_inventory_quantity")
                    barcode_map[vid] = vj.get("barcode") or ""
                    quantity_price_breaks_map[vid] = vj.get("quantity_price_breaks")

            try:
                html = fetch_pdp_html(handle)
            except Exception as e:
                log(f"[WARN] PDP fetch failed for {handle}: {e}")
                html = ""

            rise = back_rise = inseam = leg_open = ""
            if html:
                try:
                    rise, back_rise, inseam, leg_open = extract_measurements(html)
                except Exception:
                    pass

            style_total = 0
            style_has_qty = False

            variants = p.get("variants") or []
            provisional = []

            product_display = handle.replace("-", " ").strip().title()
            style_name = title

            for v in variants:
                vid = str(v.get("id") or "")
                price = dollars_from_cents(v.get("price"))
                compare_at = dollars_from_cents(v.get("compare_at_price"))
                available = v.get("available")
                brand_sku = v.get("sku") or ""

                size = parse_size_from_sku(brand_sku)

                qty_raw = qty_map.get(vid)
                qty = None if qty_raw in (None, "") else qty_raw
                if qty is not None:
                    style_has_qty = True
                    try:
                        style_total += int(qty)
                    except Exception:
                        pass

                qpb_val = quantity_price_breaks_map.get(vid)
                if isinstance(qpb_val, (dict, list)):
                    quantity_price_breaks = json.dumps(qpb_val)
                else:
                    quantity_price_breaks = "" if qpb_val in (None, "") else str(qpb_val)

                qty_display = "" if qty is None else qty
                old_qty_val = old_qty_map.get(vid)
                old_qty_display = "" if old_qty_val in (None, "") else old_qty_val

                row = {
                    "Style Id": style_id,
                    "Handle": handle,
                    "Published At": published_at,
                    "Product": product_display,
                    "Style Name": style_name,
                    "Product Type": product_type,
                    "Tags": tags,
                    "Vendor": vendor,
                    "Description": description,
                    "Variant Title": f"{product_display} {size}".strip(),
                    "Color": color,
                    "Size": size,
                    "Rise": rise,
                    "Back Rise": back_rise,
                    "Inseam": inseam,
                    "Leg Opening": leg_open,
                    "Price": price,
                    "Compare at Price": compare_at,
                    "Quantity Price Breaks": quantity_price_breaks,
                    "Available for Sale": "TRUE" if available else "FALSE" if available is not None else "",
                    "Quantity Available": qty_display,
                    "Old Quantity Available": old_qty_display,
                    "Quantity of style": "",  # fill later
                    "SKU - Shopify": vid,
                    "SKU - Brand": brand_sku,
                    "Barcode": barcode_map.get(vid, ""),
                    "Image URL": image_url,
                    "SKU URL": sku_url
                }
                provisional.append(row)

            for r in provisional:
                r["Quantity of style"] = style_total if style_has_qty else ""
                w.writerow(r)

            log(f"[{idx}/{len(products)}] {handle} -> {len(provisional)} variants "
                f"(style_total={'N/A' if not style_has_qty else style_total})")
            time.sleep(0.15)

    log(f"CSV: {csv_path}")

if __name__ == "__main__":
    run()