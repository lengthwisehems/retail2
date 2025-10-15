#!/usr/bin/env python3
import os, re, csv, json, time
from datetime import datetime
from typing import Dict, Any, List, Optional

import requests
from bs4 import BeautifulSoup

BASE = "https://www.ramybrook.com"

# Output folder next to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(SCRIPT_DIR, "Output")
os.makedirs(OUT_DIR, exist_ok=True)

LOG_PATH = os.path.join(OUT_DIR, "ramybrook_run.log")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0 Safari/537.36"
}

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def polite_get(url: str, max_retries: int = 6, backoff: float = 1.0) -> requests.Response:
    for i in range(max_retries):
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return r
        if r.status_code in (429, 503):
            sleep_for = backoff * (2 ** i)
            log(f"[wait] {r.status_code} on {url} -> sleeping {sleep_for:.1f}s")
            time.sleep(sleep_for)
            continue
        r.raise_for_status()
    r.raise_for_status()

def dollars_from_cents(val) -> str:
    try:
        if val is None:
            return ""
        c = int(val)
        return f"${c/100:.2f}"
    except Exception:
        s = str(val or "").strip()
        if not s:
            return ""
        if s.startswith("$"):
            return s
        try:
            n = float(s)
            if n > 999:  # looks like cents
                return f"${n/100:.2f}"
            return f"${n:.2f}"
        except:
            return s

def parse_iso_to_mdy(iso_str: Optional[str]) -> str:
    if not iso_str:
        return ""
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(iso_str, fmt)
            return dt.strftime("%m/%d/%y")
        except Exception:
            pass
    # Fallback: first 10 chars as date
    try:
        dt = datetime.strptime(iso_str[:10], "%Y-%m-%d")
        return dt.strftime("%m/%d/%y")
    except:
        return ""

def clean_html_text(html: str) -> str:
    """Collapse HTML into a single whitespace-normalized text string."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text)


def extract_number_after(label: str, text: str) -> Optional[str]:
    """Return the first number that appears after a given label in text."""
    if not label or not text:
        return None
    pattern = rf"{re.escape(label)}[^0-9]*(\d+(?:\.\d+)?)"
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def _brace_matched_json(source: str, anchor: str) -> Optional[str]:
    """Return the JSON object that immediately follows an anchor string."""
    idx = source.find(anchor)
    if idx == -1:
        return None
    eq_idx = source.find("=", idx)
    if eq_idx == -1:
        return None
    brace_start = source.find("{", eq_idx)
    if brace_start == -1:
        return None

    depth = 0
    for i in range(brace_start, len(source)):
        ch = source[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return source[brace_start : i + 1]
    return None


def extract_barrel_product_from_html(html: str) -> Optional[Dict[str, Any]]:
    """Find and parse window.BARREL.product = {...} from PDP HTML."""
    if not html:
        return None

    try:
        blob = _brace_matched_json(html, "window.BARREL.product")
        if blob:
            return json.loads(blob)

        # As a fallback, iterate individual <script> tags (helps with malformed markup).
        soup = BeautifulSoup(html, "html.parser")
        for script in soup.find_all("script"):
            txt = script.string or script.text or ""
            if "window.BARREL.product" not in txt:
                continue
            blob = _brace_matched_json(txt, "window.BARREL.product")
            if blob:
                return json.loads(blob)
        return None
    except Exception as e:
        log(f"[BARREL] parse error: {e}")
        return None

def fallback_inventory_from_script(html: str):
    """
    Very forgiving pull: scan the PDP <script> text for variant id + inventoryQuantity pairs.
    Returns (inventory_map, nextIncomingDate_map) where keys are variant_id strings.
    """
    inv = {}
    next_incoming = {}
    if not html:
        return inv, next_incoming

    # Collapse whitespace to simplify regex
    s = re.sub(r"\s+", " ", html)

    # id: 40661401075776 ... inventoryQuantity: 10
    for m in re.finditer(r'"id"\s*:\s*(\d+)[^}]*?"inventoryQuantity"\s*:\s*(\d+)', s):
        vid, qty = m.group(1), m.group(2)
        try:
            inv[vid] = int(qty)
        except Exception:
            pass

    # optional: nextIncomingDate (null or "YYYY-MM-DD")
    for m in re.finditer(r'"id"\s*:\s*(\d+)[^}]*?"nextIncomingDate"\s*:\s*(null|"(.*?)")', s):
        vid = m.group(1)
        if m.group(2) == "null":
            next_incoming[vid] = "null"
        else:
            next_incoming[vid] = m.group(3) or ""

    return inv, next_incoming

def _normalize_fractional_measure(value: str) -> str:
    """Convert values like `9 1/2"` to `9.5` while leaving plain numbers alone."""
    if not value:
        return ""
    cleaned = value.strip().replace('"', "")
    # e.g. "9 1/2" or "9 3/4"
    frac_match = re.match(r"^(\d+)\s+(\d+)/(\d+)$", cleaned)
    if frac_match:
        whole, num, den = map(int, frac_match.groups())
        try:
            return f"{whole + num / den:.2f}".rstrip("0").rstrip(".")
        except ZeroDivisionError:
            return cleaned
    return cleaned


def extract_front_rise_from_body_html(body_html: str) -> str:
    """Pull the measurement immediately following `Rise:` from body_html."""
    if not body_html:
        return ""

    text = clean_html_text(body_html)
    if not text:
        return ""

    # Primary: numeric with optional decimal
    match = re.search(r"Rise:\s*(\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
    if match:
        return match.group(1)

    # Secondary: fractional formats such as "9 1/2"
    match = re.search(r"Rise:\s*([0-9]+\s+[0-9]/[0-9])", text, flags=re.IGNORECASE)
    if match:
        return _normalize_fractional_measure(match.group(1))

    return ""


def extract_measurements_from_pdp_html(html: str):
    """
    Fallback for Front Rise / Inseam / Leg Opening directly from PDP HTML
    (if not present in products.json body_html). We strip tags and search labels.
    """
    if not html:
        return "", "", ""

    # reuse your HTML cleaner
    text = clean_html_text(html)

    fr = extract_number_after("Front Rise", text) or extract_number_after("Rise", text)
    inseam = extract_number_after("Inseam", text)
    leg = extract_number_after("Leg Opening", text)

    return fr or "", inseam or "", leg or ""


def fetch_handle_json(handle: str) -> Optional[Dict[str, Any]]:
    """Fetch /products/<handle>.json for barcodes."""
    url = f"{BASE}/products/{handle}.json"
    try:
        r = polite_get(url)
        return r.json().get("product")
    except Exception as e:
        log(f"[handle.json] fail {handle}: {e}")
        return None

def discover_denim_pages(limit: int = 250, max_pages: int = 50) -> List[List[Dict[str, Any]]]:
    """Paginate /collections/denim/products.json until empty or max_pages reached."""
    pages = []
    for page in range(1, max_pages + 1):
        url = f"{BASE}/collections/denim/products.json?limit={limit}&page={page}"
        try:
            r = polite_get(url)
            data = r.json()
            products = data.get("products", [])
            if not products:
                break
            pages.append(products)
            log(f"[page {page}] total items={len(products)}")
            time.sleep(0.6)  # be gentle
        except Exception as e:
            log(f"[ERROR] denim page {page}: {e}")
            break
    return pages

def main():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    csv_path = os.path.join(OUT_DIR, f"RAMY_{timestamp}.csv")

    FIELDNAMES = [
        "Style Id","Handle","Published At","Product","Product Type","Vendor","Description",
        "Variant Title","Color","Size",
        "Front Rise","Inseam","Leg Opening",
        "Price","Compare at Price","Available for Sale",
        "Quantity Available","Quantity of style","Next Shipment",
        "SKU","Barcode","Image URL","SKU URL"
    ]

    pages = discover_denim_pages(limit=250, max_pages=50)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()

        for products in pages:
            # Keep only Pants / Jean / Jeans
            page_products = [
                p for p in products
                if p.get("product_type","").strip().upper() in {"PANTS","JEAN","JEANS"}
            ]

            for p in page_products:
                style_id = p.get("id")
                handle = p.get("handle","")
                title = p.get("title","")
                product_type = p.get("product_type","")
                vendor = p.get("vendor","")

                published_raw = p.get("published_at") or ""
                published_at = parse_iso_to_mdy(published_raw) if published_raw else ""

                body_html = p.get("body_html") or ""
                description = BeautifulSoup(body_html, "html.parser").get_text(" ", strip=True)

                body_text = clean_html_text(body_html)
                front_rise = extract_front_rise_from_body_html(body_html) or ""
                if not front_rise:
                    front_rise = extract_number_after("Front Rise", body_text) or extract_number_after("Rise", body_text) or ""
                inseam = extract_number_after("Inseam", body_text) or ""
                leg_open = extract_number_after("Leg Opening", body_text) or ""

                variants = p.get("variants", []) or []
                images = p.get("images", []) or []
                first_image_src = images[0].get("src","") if images else ""

                # PDP scrape for inventory + nextIncomingDate
                pdp_url = f"{BASE}/products/{handle}"
                try:
                    html = polite_get(pdp_url).text
                except Exception as e:
                    log(f"[PDP] fetch fail {handle}: {e}")
                    html = ""

                barrel = extract_barrel_product_from_html(html) if html else None
                qty_map: Dict[str, int] = {}
                next_map: Dict[str, str] = {}
                if barrel:
                    for v in barrel.get("variants", []) or []:
                        vid = str(v.get("id"))
                        inv = v.get("inventoryQuantity")
                        if inv is not None:
                            try:
                                qty_map[vid] = int(inv)
                            except:
                                pass
                        nd = v.get("nextIncomingDate")
                        if nd is None:
                            next_map[vid] = "null"
                        elif isinstance(nd, str):
                            next_map[vid] = nd.strip() or ""
                        else:
                            next_map[vid] = str(nd)

                # --- Regex fallback supplements any missing BARREL values ---
                if html:
                    inv_fallback, next_fallback = fallback_inventory_from_script(html)
                    for vid, qty in inv_fallback.items():
                        qty_map.setdefault(vid, qty)
                    for vid, nxt in next_fallback.items():
                        next_map.setdefault(vid, nxt)

                # --- Fallback for measurements if body_html didn't have them ---
                if not front_rise and not inseam and not leg_open and html:
                    fr2, inseam2, leg2 = extract_measurements_from_pdp_html(html)
                    front_rise = front_rise or fr2
                    inseam     = inseam or inseam2
                    leg_open   = leg_open or leg2


                # Barcode via handle.json
                handle_json = fetch_handle_json(handle)
                barcode_map: Dict[str, str] = {}
                if handle_json:
                    for vj in handle_json.get("variants", []) or []:
                        vidj = str(vj.get("id"))
                        bc = vj.get("barcode") or ""
                        if bc:
                            barcode_map[vidj] = bc

                # style-level sum
                style_total = 0
                has_any_qty = False
                for v in variants:
                    vid = str(v.get("id"))
                    if vid in qty_map:
                        style_total += qty_map[vid]
                        has_any_qty = True

                for v in variants:
                    vid = str(v.get("id"))
                    vtitle = v.get("title","")

                    # Ramy often uses option1=style, option2=color, option3=size
                    opt1 = v.get("option1") or ""
                    opt2 = v.get("option2") or ""
                    opt3 = v.get("option3") or ""
                    color = opt2 or ""
                    size = opt3 or ""

                    price = dollars_from_cents(v.get("price"))
                    compare = dollars_from_cents(v.get("compare_at_price"))
                    available = v.get("available")

                    inv = qty_map.get(vid, "")
                    next_ship = next_map.get(vid, "")

                    image_url = first_image_src
                    if barrel:
                        for bv in barrel.get("variants", []) or []:
                            if str(bv.get("id")) == vid:
                                img = bv.get("image") or ""
                                if img:
                                    image_url = "https:" + img if img.startswith("//") else img
                                break
                    if not image_url and images:
                        image_url = images[0].get("src","") or ""

                    if image_url.startswith("//"):
                        image_url = "https:" + image_url

                    row = {
                        "Style Id": style_id,
                        "Handle": handle,
                        "Published At": published_at,
                        "Product": title,
                        "Product Type": product_type,
                        "Vendor": vendor,
                        "Description": description,
                        "Variant Title": vtitle,
                        "Color": color,
                        "Size": size,
                        "Front Rise": front_rise,
                        "Inseam": inseam,
                        "Leg Opening": leg_open,
                        "Price": price,
                        "Compare at Price": compare,
                        "Available for Sale": available,
                        "Quantity Available": inv,
                        "Quantity of style": style_total if has_any_qty else "",
                        "Next Shipment": next_ship,
                        "SKU": vid,
                        "Barcode": barcode_map.get(vid, ""),
                        "Image URL": image_url,
                        "SKU URL": f"{BASE}/products/{handle}",
                    }
                    w.writerow(row)

                time.sleep(0.5)  # pacing between PDPs

    log(f"CSV written: {csv_path}")

if __name__ == "__main__":
    main()