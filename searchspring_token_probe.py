#!/usr/bin/env python3
"""Searchspring SnapUI token probe."""
from __future__ import annotations

import dataclasses
import json
import logging
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import ssl

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_PATH = OUTPUT_DIR / "searchspring_token_probe.log"
HELPER_DIR = OUTPUT_DIR / "_snapui_eval"
HELPER_DIR.mkdir(exist_ok=True)

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

LOGGER = logging.getLogger("searchspring_token_probe")


@dataclasses.dataclass
class BundleTarget:
    url: str
    referer: Optional[str] = None
    note: Optional[str] = None
    site_id: Optional[str] = None
    origin: Optional[str] = None


@dataclasses.dataclass
class BrandConfig:
    name: str
    targets: List[BundleTarget]


BRANDS: List[BrandConfig] = [
    BrandConfig(
        name="WarpWeft-reference",
        targets=[
            BundleTarget(
                url="https://snapui.searchspring.io/dkc5xr/bundle.js",
                referer="https://warpandweftworld.com/collections/jeans",
                note="SnapUI runtime",
                site_id="dkc5xr",
                origin="https://warpandweftworld.com",
            )
        ],
    ),
    BrandConfig(
        name="Redone-reference",
        targets=[
            BundleTarget(
                url="https://snapui.searchspring.io/w7x7sx/bundle.js",
                referer="https://shopredone.com/collections/denim",
                note="SnapUI runtime",
                site_id="w7x7sx",
                origin="https://shopredone.com",
            )
        ],
    ),
    BrandConfig(
        name="DL1961-reference",
        targets=[
            BundleTarget(
                url="https://snapui.searchspring.io/8176gy/bundle.js",
                referer="https://dl1961.com/collections/women-view-all-fits",
                note="SnapUI runtime",
                site_id="8176gy",
                origin="https://dl1961.com",
            )
        ],
    ),
    BrandConfig(
        name="GoodAmerican",
        targets=[
            BundleTarget(
                url="https://snapui.searchspring.io/5ojqb3/bundle.js",
                referer="https://www.goodamerican.com/collections/womens-jeans",
                note="SnapUI runtime",
                site_id="5ojqb3",
                origin="https://www.goodamerican.com",
            ),
            BundleTarget(
                url="https://www.goodamerican.com/api/searchspring?bgfilter.collection_handle=womens-jeans",
                note="Searchspring API payload",
            ),
        ],
    ),
    BrandConfig(
        name="Mother",
        targets=[
            BundleTarget(
                url="https://snapui.searchspring.io/00svms/bundle.js",
                referer="https://www.motherdenim.com/collections/denim",
                note="SnapUI runtime",
                site_id="00svms",
                origin="https://www.motherdenim.com",
            ),
            BundleTarget(
                url="https://00svms.a.searchspring.io/api/search/search.json?siteId=00svms&resultsFormat=native&resultsPerPage=40&bgfilter.collection_name=Denim&page=1",
                note="Searchspring API payload",
            ),
        ],
    ),
    BrandConfig(
        name="Frame",
        targets=[
            BundleTarget(
                url="https://snapui.searchspring.io/v1j77y/bundle.js",
                referer="https://frame-store.com/collections/denim-women",
                note="SnapUI runtime",
                site_id="v1j77y",
                origin="https://frame-store.com",
            ),
            BundleTarget(
                url="https://v1j77y.a.searchspring.io/api/search/search.json?siteId=v1j77y&resultsFormat=native&resultsPerPage=40&bgfilter.collection_id=914751490&page=1",
                note="Searchspring API payload",
            ),
        ],
    ),
]

HEX32 = re.compile(r"[0-9a-f]{32}")
TOKEN_CONTEXT_CHARS = 120

LIST_SCRIPT = r"""
const fs = require('fs');
const vm = require('vm');
const bundlePath = process.argv[2];
const siteId = process.argv[3] || '';
const origin = process.argv[4] || '';
const bundle = fs.readFileSync(bundlePath, 'utf8');
const scriptUrls = [];
const noop = () => {};
const dataset = new Proxy({}, {
  get: (_target, prop) => {
    const key = String(prop || '').toLowerCase();
    if (key.includes('site')) return siteId;
    if (key.includes('env')) return 'prod';
    if (key.includes('domain')) return origin.replace(/^(https?:\/\/)+/, '');
    return siteId;
  },
});
const currentScript = {
  getAttribute: (name) => {
    if (!name) return null;
    const key = String(name).toLowerCase();
    if (key.includes('site')) return siteId;
    if (key.includes('env')) return 'prod';
    if (key.includes('domain')) return origin.replace(/^(https?:\/\/)+/, '');
    return siteId;
  },
  src: `https://snapui.searchspring.io/${siteId || 'probe'}/bundle.js`,
  dataset,
  tagName: 'SCRIPT',
  parentElement: { insertBefore: noop },
};
const makeElement = () => {
  const node = {
    setAttribute: (name, value) => { node[name] = value; },
    getAttribute: (name) => node[name],
    addEventListener: noop,
    removeEventListener: noop,
  };
  return node;
};
const originHref = origin || (siteId ? `https://${siteId}.snapui.local/collections/test` : 'https://snapui.local/');
const context = {
  window: {},
  document: {
    currentScript,
    createElement: makeElement,
    getElementsByTagName: () => [],
    head: { appendChild: (el) => { if (el && el.src) { scriptUrls.push(el.src); } } },
    body: { appendChild: noop },
    querySelector: () => null,
    addEventListener: noop,
    readyState: 'complete',
  },
  navigator: { userAgent: 'Mozilla/5.0' },
  location: { origin: originHref, href: originHref },
  console,
  setTimeout: () => 0,
  clearTimeout: noop,
  setInterval: () => 0,
  clearInterval: noop,
  performance: { now: () => Date.now() },
  Shopify: { shop: { permanent_domain: siteId ? `${siteId}.myshopify.com` : '' } },
};
context.window = context;
context.window.document = context.document;
context.window.Searchspring = {};
context.window.addEventListener = noop;
context.window.dispatchEvent = noop;
context.self = context.window;
vm.createContext(context);
vm.runInContext(bundle, context, { filename: bundlePath, displayErrors: false });
console.log(JSON.stringify(scriptUrls));
"""

RUN_SCRIPT = r"""
const fs = require('fs');
const vm = require('vm');
const bundlePath = process.argv[2];
const siteId = process.argv[3] || '';
const origin = process.argv[4] || '';
const chunkPaths = process.argv.slice(5);
const bundle = fs.readFileSync(bundlePath, 'utf8');
const noop = () => {};
const dataset = new Proxy({}, {
  get: (_target, prop) => {
    const key = String(prop || '').toLowerCase();
    if (key.includes('site')) return siteId;
    if (key.includes('env')) return 'prod';
    if (key.includes('domain')) return origin.replace(/^(https?:\/\/)+/, '');
    return siteId;
  },
});
const currentScript = {
  getAttribute: (name) => {
    if (!name) return null;
    const key = String(name).toLowerCase();
    if (key.includes('site')) return siteId;
    if (key.includes('env')) return 'prod';
    if (key.includes('domain')) return origin.replace(/^(https?:\/\/)+/, '');
    return siteId;
  },
  src: `https://snapui.searchspring.io/${siteId || 'probe'}/bundle.js`,
  dataset,
  tagName: 'SCRIPT',
  parentElement: { insertBefore: noop },
};
const makeElement = () => {
  const node = {
    setAttribute: (name, value) => { node[name] = value; },
    getAttribute: (name) => node[name],
    addEventListener: noop,
    removeEventListener: noop,
  };
  return node;
};
const originHref = origin || (siteId ? `https://${siteId}.snapui.local/collections/test` : 'https://snapui.local/');
const context = {
  window: {},
  document: {
    currentScript,
    createElement: makeElement,
    getElementsByTagName: () => [],
    head: { appendChild: noop },
    body: { appendChild: noop },
    querySelector: () => null,
    addEventListener: noop,
    readyState: 'complete',
  },
  navigator: { userAgent: 'Mozilla/5.0' },
  location: { origin: originHref, href: originHref },
  console,
  setTimeout: () => 0,
  clearTimeout: noop,
  setInterval: () => 0,
  clearInterval: noop,
  performance: { now: () => Date.now() },
  Shopify: { shop: { permanent_domain: siteId ? `${siteId}.myshopify.com` : '' } },
};
context.window = context;
context.window.document = context.document;
context.window.Searchspring = {};
context.window.addEventListener = noop;
context.window.dispatchEvent = noop;
context.self = context.window;
vm.createContext(context);
vm.runInContext(bundle, context, { filename: bundlePath, displayErrors: false });
for (const chunkPath of chunkPaths) {
  const chunk = fs.readFileSync(chunkPath, 'utf8');
  vm.runInContext(chunk, context, { filename: chunkPath, displayErrors: false });
}
const snap = context.Searchspring && context.Searchspring.snap;
if (snap && (snap.storefrontAccessToken || snap.storefrontToken)) {
  console.log(JSON.stringify({
    token: snap.storefrontAccessToken || snap.storefrontToken,
    domain: snap.shopDomain || snap.domain || null,
  }));
} else {
  console.log(JSON.stringify({ token: null }));
}
"""


def ensure_helper_scripts() -> Tuple[Path, Path]:
    list_path = HELPER_DIR / "list_chunks.js"
    run_path = HELPER_DIR / "run_bundle.js"
    if not list_path.exists():
        list_path.write_text(LIST_SCRIPT, encoding="utf-8")
    if not run_path.exists():
        run_path.write_text(RUN_SCRIPT, encoding="utf-8")
    return list_path, run_path


def configure_logging() -> None:
    LOGGER.setLevel(logging.INFO)
    handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    LOGGER.addHandler(console)


def fetch_text(target: BundleTarget) -> Tuple[Optional[str], Optional[bytes]]:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
    if target.referer:
        headers["Referer"] = target.referer
    req = Request(target.url, headers=headers)
    try:
        with urlopen(req, context=CTX, timeout=8) as resp:
            data = resp.read()
    except HTTPError as err:
        LOGGER.warning("%s (%s) HTTP %s", target.url, target.note or "bundle", err.code)
        return None, None
    except URLError as err:
        LOGGER.warning("%s (%s) URLError %s", target.url, target.note or "bundle", err.reason)
        return None, None
    try:
        text = data.decode("utf-8", "replace")
    except Exception:
        text = None
    return text, data


def scan_for_tokens(name: str, target: BundleTarget, text: Optional[str], data: Optional[bytes]) -> Set[str]:
    found: Set[str] = set()
    if not text and data:
        try:
            text = data.decode("utf-8", "replace")
        except Exception:
            text = None
    if not text and not data:
        return found
    LOGGER.info("Scanning %s - %s", name, target.url)
    if text:
        for token in HEX32.findall(text):
            if not token.isdigit():
                LOGGER.info("  candidate token: %s", token)
                found.add(token)
        lower = text.lower()
        if "storefront" in lower:
            LOGGER.info("  found 'storefront' keyword")
        for keyword in ("storefrontToken", "storefrontAccessToken", "X-Shopify-Storefront-Access-Token"):
            idx = text.find(keyword)
            if idx != -1:
                snippet = text[max(0, idx - TOKEN_CONTEXT_CHARS): idx + TOKEN_CONTEXT_CHARS]
                LOGGER.info("  context for %s: %s", keyword, snippet)
    return found


def run_node(script: Path, args: Sequence[str]) -> Tuple[str, str, int]:
    if shutil.which("node") is None:
        return "", "node executable not found", 127
    result = subprocess.run(
        ["node", str(script), *map(str, args)],
        capture_output=True,
        text=True,
        cwd=str(HELPER_DIR),
    )
    return result.stdout, result.stderr, result.returncode


def enumerate_chunks(bundle_path: Path, site_id: str, origin: Optional[str]) -> List[str]:
    list_script, _ = ensure_helper_scripts()
    stdout, stderr, code = run_node(list_script, [bundle_path, site_id, origin or ""])
    if code != 0:
        LOGGER.info("  Node chunk enumeration failed: %s", stderr.strip())
        return []
    try:
        urls = json.loads(stdout.strip() or "[]")
    except json.JSONDecodeError:
        LOGGER.info("  Unexpected chunk listing output: %s", stdout.strip())
        return []
    clean: List[str] = []
    for url in urls:
        if not url:
            continue
        clean.append(str(url))
    if clean:
        LOGGER.info("  SnapUI queued %d chunk(s)", len(clean))
    return clean


def fetch_chunk(url: str, referer: Optional[str], dest: Path) -> bool:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
    if referer:
        headers["Referer"] = referer
    req = Request(url, headers=headers)
    try:
        with urlopen(req, context=CTX, timeout=8) as resp:
            dest.write_bytes(resp.read())
        return True
    except Exception as exc:
        LOGGER.info("  failed to fetch %s (%s)", url, exc)
        return False


def execute_snapui(bundle_path: Path, site_id: str, origin: Optional[str], chunk_paths: Iterable[Path]) -> Optional[str]:
    _, run_script = ensure_helper_scripts()
    args = [bundle_path, site_id, origin or "", *chunk_paths]
    stdout, stderr, code = run_node(run_script, args)
    if code != 0:
        LOGGER.info("  Node execution failed: %s", stderr.strip())
        return None
    try:
        payload = json.loads(stdout.strip() or "{}")
    except json.JSONDecodeError:
        LOGGER.info("  Unexpected execution output: %s", stdout.strip())
        return None
    token = payload.get("token")
    if token:
        LOGGER.info("  SnapUI runtime exposed token %s (domain=%s)", token, payload.get("domain"))
    return token


def attempt_snapui_execution(name: str, target: BundleTarget, text: Optional[str]) -> Set[str]:
    if not target.site_id or not text:
        return set()
    if shutil.which("node") is None:
        LOGGER.info("  skipping SnapUI evaluation (node not available)")
        return set()
    bundle_path = HELPER_DIR / f"{target.site_id}_bundle.js"
    bundle_path.write_text(text, encoding="utf-8")
    chunk_urls = enumerate_chunks(bundle_path, target.site_id, target.origin or target.referer)
    chunk_paths: List[Path] = []
    for idx, url in enumerate(chunk_urls):
        dest = HELPER_DIR / f"{target.site_id}_chunk_{idx}.js"
        if fetch_chunk(url, target.referer or target.origin, dest):
            chunk_paths.append(dest)
    token = execute_snapui(bundle_path, target.site_id, target.origin or target.referer, chunk_paths)
    return {token} if token else set()


def process_target(brand: BrandConfig, target: BundleTarget) -> None:
    text, data = fetch_text(target)
    tokens = scan_for_tokens(brand.name, target, text, data)
    if target.site_id:
        extra = attempt_snapui_execution(brand.name, target, text)
        for token in extra:
            if token not in tokens:
                LOGGER.info("  SnapUI derived token: %s", token)


def main() -> None:
    configure_logging()
    for brand in BRANDS:
        LOGGER.info("===== %s =====", brand.name)
        for target in brand.targets:
            process_target(brand, target)


if __name__ == "__main__":
    main()
