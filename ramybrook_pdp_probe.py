# -*- coding: utf-8 -*-
"""Check how ramybrook.com answers product-page requests from this machine.

Requests 3 product pages with each header set ramybrook_inventory.py can use
(plus the plain requests default), 9 requests in total spaced 3s apart, and
prints how the site answered each. Run it from the same folder as
ramybrook_inventory.py:

    python ramybrook_pdp_probe.py

Then send back the table it prints.
"""
from __future__ import annotations

import platform
import ssl
import sys
import time

import requests

import ramybrook_inventory as rb

HANDLES = [
    "liv-high-rise-wide-leg-jean-black",
    "helena-high-rise-flare-jean",
    "willa-embellished-straight-leg-jean",
]
PROFILES = {
    "browser": rb.BROWSER_HEADERS,
    "minimal": rb.MINIMAL_HEADERS,
    "requests-default": {},
}
GAP_SECONDS = 3.0


def main() -> None:
    print(f"Python {platform.python_version()} | requests {requests.__version__} | "
          f"{ssl.OPENSSL_VERSION} | {platform.system()} {platform.release()}")
    print(f"{'headers':<17} {'handle':<38} {'result'}")
    print("-" * 100)
    first = True
    for name, headers in PROFILES.items():
        session = requests.Session()
        session.headers.update(headers)
        for handle in HANDLES:
            if not first:
                time.sleep(GAP_SECONDS)
            first = False
            url = f"{rb.PDP_HOST}/products/{handle}"
            try:
                resp = session.get(url, timeout=30)
            except requests.RequestException as exc:
                result = f"request failed: {exc.__class__.__name__}: {exc}"
            else:
                if resp.status_code == 200:
                    product = rb.pdp_main_product(resp.text, handle)
                    result = ("HTTP 200, product record found, nextIncomingDate="
                              f"{product.get('nextIncomingDate')!r}" if product
                              else "HTTP 200 but product record NOT found")
                else:
                    result = rb.describe_block(resp)
            print(f"{name:<17} {handle:<38} {result}")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
