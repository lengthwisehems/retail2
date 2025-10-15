import os, re, csv, json, time, html, requests
from datetime import datetime
from urllib.parse import urljoin

# ---------- Config ----------
BASE = "https://edyson.com"
OUT_DIR = r"C:\Users\carri\OneDrive - Length Wise\data scraping\Edyson\Output"
LOG     = r"C:\Users\carri\OneDrive - Length Wise\data scraping\Edyson\edyson_all_layout.log"

HEADERS = {"User-Agent": "inventory-research/1.0 (+length-wise)"}
TIMEOUT = 30
RETRIES = 2
DELAY_S = 0.2
MAX_PAGES = 80  # safety cap for full catalog

CSV_HEADERS = [
    "Style Id","Handle","Product","Product Type","Vendor","Description",
    "Variant Title","Color","Size","Rise","Inseam","Leg Opening",
    "Price","Compare at Price","Available for Sale","Quantity Available",
    "Quantity of style","SKU","Image URL","SKU URL"
]

os.makedirs(OUT_DIR, exist_ok=True)

# ---------- Utilities ----------
def log(msg: str):
    with open(LOG, "a", encoding="utf-8") as f:
        f.write(msg.rstrip() + "\n")

def http_get(url, params=None):
    for _ in range(RETRIES + 1):
        r = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.text
        time.sleep(0.4)
    r.raise_for_status()

def http_get_json(url, params=None):
    for _ in range(RETRIES + 1):
        r = requests.get(url, params=params, headers={**HEADERS, "Accept":"application/json"}, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.json()
        time.sleep(0.4)
    r.raise_for_status()

def html_to_text(s: str) -> str:
    if not s:
        return ""
    s = html.unescape(s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def fraction_to_decimal(text):
    """Convert '10 1/4' -> 10.25. Keep plain '28' or '10.25' as float. If not numeric, return original."""
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


def last_word_product_type(title: str) -> str:
    """Product Type = last word of the title (e.g., '...Wide Leg Jeans' -> 'Jeans')."""
    if not title:
        return ""
    words = [w for w in re.split(r"\s+", title.strip()) if w]
    return words[-1] if words else ""

# ---------- 1) Discover ALL products from /products.json ----------
def discover_all_products():
    all_products = []
    for page in range(1, MAX_PAGES + 1):
        data = http_get_json(urljoin(BASE, "/products.json"), params={"limit": 2500, "page": page})
        products = data.get("products") or data.get("items") or []
        if not products:
            break
        all_products.extend(products)
        log(f"[ALL ] page={page} products={len(products)}")
        time.sleep(DELAY_S)
    log(f"[ALL ] total products={len(all_products)}")
    return all_products

# ---------- 2) Storefront token + domain (from Flair) ----------
TOKEN_RE = re.compile(r'storefront_token"\s*:\s*"([a-zA-Z0-9_]+)"')
DOM_RE   = re.compile(r'"shopify_domain"\s*:\s*"([^"]+)"')

def get_storefront_creds():
    for path in ["/products", "/"]:
        html_text = http_get(urljoin(BASE, path))
        m = TOKEN_RE.search(html_text)
        d = DOM_RE.search(html_text)
        token  = m.group(1) if m else None
        domain = d.group(1) if d else None
        if token and domain:
            return token, domain
    return None, None

# ---------- 3) GraphQL for quantityAvailable ----------
GQL = """
query ProductByHandle($handle: String!) @inContext {
  product(handle: $handle) {
    id
    variants(first: 250) { nodes { id quantityAvailable } }
  }
}
"""

def gql_request(domain, token, query, variables):
    url = f"https://{domain}/api/2025-07/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Storefront-Access-Token": token,
        **HEADERS
    }
    for _ in range(RETRIES + 1):
        r = requests.post(url, headers=headers, json={"query": query, "variables": variables}, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.json()
        time.sleep(0.5)
    r.raise_for_status()

def gid_to_numeric(gid):
    # gid://shopify/ProductVariant/44509141369144 -> "44509141369144"
    return str(gid).split("/")[-1] if gid else None

# ---------- 4) PDP measurements from /products/<handle> (loose selector) ----------
def scrape_measurements(handle: str):
    html_text = http_get(urljoin(BASE, f"/products/{handle}"))

    def near_number(label_regex):
        # Find the label text anywhere, then look up to 300 chars ahead for a number or fraction
        m = re.search(rf"(?i){label_regex}[^<]{{0,300}}", html_text)
        if not m:
            return ""
        window = html_text[m.end(): m.end() + 300]
        nm = re.search(r"(\d+\s+\d+/\d+|\d+\.\d+|\d+)", window)
        return html.unescape(nm.group(1)).strip() if nm else ""

    rise_txt   = near_number(r"Rise")
    inseam_txt = near_number(r"Inseam")
    leg_txt    = near_number(r"(?:Leg\s*Opening|Leg opening|Leg Opening \(at opening\))")

    rise   = fraction_to_decimal(rise_txt) if rise_txt else ""
    inseam = fraction_to_decimal(inseam_txt) if inseam_txt else ""
    leg    = fraction_to_decimal(leg_txt) if leg_txt else ""
    return rise, inseam, leg

# ---------- 5) Main ----------
def main():
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    outfile = os.path.join(OUT_DIR, f"edyson_jeans_{ts}.csv")
    log(f"=== Run {ts} ===")

    products = discover_all_products()
    log(f"[ALL ] total products={len(products)}")

    token, domain = get_storefront_creds()
    if not token or not domain:
        log("[CREDS] storefront token/domain not found - Quantity Available & Quantity of style will be blank.")
    else:
        log(f"[CREDS] domain={domain} token_len={len(token)}")

    with open(outfile, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        w.writeheader()

        for p in products:
            try:
                style_id      = p.get("id")
                handle        = p.get("handle") or ""
                product_title = p.get("title") or ""
                vendor        = p.get("vendor") or ""
                # Product Type = last word of title
                product_type  = last_word_product_type(product_title)
                desc          = html_to_text(p.get("body_html") or "")
                images        = p.get("images") or []
                image_url     = images[0].get("src") if images else ""
                sku_url       = f"{BASE}/products/{handle}"

                # Measurements per product from /products/<handle>
                try:
                    rise, inseam, leg = scrape_measurements(handle)
                except Exception as e:
                    log(f"[MEAS] {handle} -> {e}")
                    rise = inseam = leg = ""

                # Quantity map and total by style from Storefront
                qty_map = {}
                qty_total = None
                if token and domain:
                    try:
                        data = gql_request(domain, token, GQL, {"handle": handle})
                        prod = (data or {}).get("data", {}).get("product")
                        if prod:
                            nodes = (prod.get("variants") or {}).get("nodes") or []
                            for node in nodes:
                                num_id = gid_to_numeric(node.get("id"))
                                qty_map[num_id] = node.get("quantityAvailable")
                            qty_total = sum((q or 0) for q in qty_map.values())
                    except Exception as e:
                        log(f"[GQL ] {handle} -> {e}")

                # Write one row per variant
                for v in p.get("variants") or []:
                    var_id        = str(v.get("id"))
                    variant_title = v.get("title") or ""
                    color         = v.get("option2") or ""
                    size          = v.get("option1") or ""
                    # IMPORTANT: do not divide price — use as provided
                    price         = v.get("price")
                    compare_at    = v.get("compare_at_price")
                    available     = bool(v.get("available"))
                    qty_avail     = qty_map.get(var_id) if qty_map else None

                    w.writerow({
                        "Style Id": style_id,
                        "Handle": handle,
                        "Product": product_title,
                        "Product Type": product_type,
                        "Vendor": vendor,
                        "Description": desc,
                        "Variant Title": variant_title,
                        "Color": color,
                        "Size": size,
                        "Rise": rise,
                        "Inseam": inseam,
                        "Leg Opening": leg,
                        "Price": price,
                        "Compare at Price": compare_at,
                        "Available for Sale": available,
                        "Quantity Available": qty_avail,
                        "Quantity of style": qty_total,
                        "SKU": var_id,
                        "Image URL": image_url,
                        "SKU URL": sku_url
                    })

                time.sleep(DELAY_S)

            except Exception as e:
                log(f"[ERR ] handle={p.get('handle')} -> {e}")

    log(f"[DONE] file={outfile}")
    print(f"Wrote {outfile}")

if __name__ == "__main__":
    main()
