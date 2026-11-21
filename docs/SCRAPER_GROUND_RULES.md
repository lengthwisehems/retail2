# Brand scraper ground rules

How to write a new brand scraper, or change an existing one, so that every brand lands in
the database the same shape. Written from the scrapers already in the repo (Good American,
Ramy Brook, AMO, AG Jeans, Paige, Edyson, AYR, Citizens of Humanity).

`Database_build_outline` is the source of truth for **what** the database expects - field
names, formats, which table each lands in. This document says **how** a scraper should
produce it. Each brand also has its own instructions, which say the things that genuinely
differ by brand; where those disagree with this document, the brand instructions win.

---

# 0. Ground rules (non-negotiable)

**Test everything before you share it.** Run the code, read the output, and check the
output is *correct* - not just that it ran. Only then hand it over, saying what you ran
and what it showed. List anything you could not test. **Never write "should work".**

**If unsure, ask Carrie. Never assume or guess.** But do not ask what the brand
instructions already answer, or what you could look up in the database or the code.

**If your output does not match the correction file you were given, ask for a decision.**
It will not be obvious whether the code or the file is right, and guessing picks one at
random. Two things in those files are instructions, not values:

* `REMOVE` - the item should not be in the output at all.
* `FILL WITH LATEST VALUES` - fill from the GraphQL/JSON response. Never write the literal
  text `FILL WITH LATEST VALUES`.

**Never print, echo, log or save credentials** - SQL password, blob SAS URL or connection
string. Read them from environment variables or `getpass`. Not into a log file, not into
an error message, not into a committed file.

**Script usability.** These are standing preferences, not nice-to-haves:

* runs from Windows Command Prompt with one command - paste the file path and go
* every log line starts with a **Central-time** timestamp
* long steps show **progress and an ETA**
* **dry run first**: the real run only happens on a second, explicit command

---

# 1. The output contract

One CSV per run, **one row per variant (SKU) per run**. Do not collapse sizes; style-level
values repeat on every row of the style.

Every column the database accepts is below, grouped as `Database_build_outline` groups
them. A brand emits what it actually has - a column it cannot source is left out, not
invented. Never rename a column to suit one brand: the database loads them by name.

**Style identifier** - `Style Id`, `Handle`, `Product`, `Style Name`,
`Style Name - Grouping`, `Color`

**Variant identifier** - `SKU - Shopify`, `SKU - Brand`, `Barcode`, `Size`, `Variant Title`

**Website info** - `SKU URL`, `Image URL`, `Description`, `Tags`, `Vendor`, `Created At`,
`Published At`, `Updated At`, `Promo`, `Size Chart`

**Measurements** - `Inseam`, `Rise`, `Back Rise`, `Knee`, `Leg Opening`

**Product filter** - `Jean Style`, `Inseam Label`, `Inseam Style`, `Rise Label`,
`Hem Style`, `Product Line`, `Product Type`, `Stretch`, `Gender`, `Color - Simplified`,
`Color - Standardized`

**Production** - `Country Produced`, `Fabric Source`

**Data** - `Price`, `Compare at Price`, `Available for Sale`, `Quantity Available`,
`Quantity Available (Instore Inventory)`, `Quantity Available (Online Inventory)`,
`Quantity of style`, `Daily Purchase`, `Returns`, `Notify Me Signups`,
`Purchase Reset Date`, `Next Shipment`, `Production Cost`, `Review AVG`, `Review Count`,
`Wishlist count`, `Google Analytics Purchases`, `Google Analytics AVG Revenue`,
`Google Analytics Total Revenue`

The Data group is where availability varies most. Every brand has price and inventory;
reviews, wishlist counts, returns and the Google Analytics figures depend entirely on what
that brand's site exposes, and several brands have none of them. They still come from the
scraper's file where they exist - they are not loaded from anywhere else - so take them
when the brand publishes them and leave the column out when it does not.

**`Updated At` should be produced for every brand.** Some of the earlier scrapers do not
emit it yet; that is an oversight, not a convention.

The database derives `Brand`, `Captured Date`, `Captured Date and Time` and
`Source File Name` from the **file name**, and generates the ID columns and `Imported At`
itself. A scraper does not emit those.

---

# 2. The naming model

Four name fields do four different jobs. Confusing them is the most common source of bad
data.

## The separator

**The separator varies by brand.** The brand instructions say what it should be, and it
usually matches whatever the brand itself uses in its variant title. Whatever it is, use
the same one everywhere in that scraper, with consistent spacing.

The examples below use ` | ` because that is what most brands use.

## Variant Title

```
Product Name (or Alt Title where there is one) | color | inseam | size
```

The **inseam segment is present only when the style has more than one inseam.** A
single-inseam style goes straight from colour to size.

For a superseded run, `OLD` takes the inseam slot - appended to the inseam where there is
one, standing on its own where there is not:

```
... | color | inseam OLD | size        multi-inseam style
... | color | OLD | size               single-inseam style
```

## Product

The brand's own product name, with the inseam inserted **after the last word "Jean"/
"Jeans" but before the colour**, then the colour:

```
NEVER FADE GOOD STRAIGHT JEANS PETITE | BLACK
```

`Product` keeps the brand's wording even where it looks wrong. If the brand writes the
colour as `BLACK` and the normalised colour is `BLACK001`, `Product` says `BLACK` and the
alt title says `BLACK001`. That is correct, not a bug.

## Product Title Alt

A **heavily cleaned-up** version of the product name, then colour, then inseam:

```
GOOD LEGS STRAIGHT JEANS | BLACK001 | PETITE
```

This is the normalised name - the one used for matching and reporting - so it takes the
cleaned colour and the standard length wording rather than the brand's own phrasing.

## Style Name

The derived style: ALL CAPS, no colour, no size, no "JEANS".

## OLD

`OLD` marks a run of a style that has been superseded by a replacement.

* On **Variant Title and Product Title Alt** it takes the inseam slot: appended to the
  inseam where there is one, on its own where there is not.

  ```
  GOOD LEGS FLARE JEANS | BLUE004 | LONG OLD
  VINTAGE STRAIGHT ANKLE JEANS | HONEY | OLD
  ```

* On **`Product` it is never added** - unless the old SKU is still being sold at the same
  time as the new one, in which case the two must stay distinguishable.

## Style identity

**How a style is identified varies by brand.** The brand instructions name the style key.
Do not assume one brand's key works for another - they are built from whatever that
brand's data actually supports, and plenty of brands use effectively random SKUs.

What is true everywhere: **`product_name` is a reporting label, not an identifier.** Two
different runs of the same jean - one being sold off, its replacement - can share a
product_name on purpose so their sales report together. **Never key or group on
product_name alone.**

## Quantity of style

Take `Quantity Available` as an integer; blank, missing or non-numeric is **0**; negatives
are **kept negative** (oversold reduces the total). Sum across all rows sharing the same
**style identity for that brand**, within the same capture. Write the group total onto
every row in the group; a row with no style key gets blank.

**This value is a repeated group total, so it must never be SUMmed across rows** by
anything reading the output.

---

# 3. Derived fields: exact values

These are compared across brands, so **spelling and capitalisation must match exactly**.
Use these strings and no variants:

```
Jean Style     Skinny, Tapered, Straight from Knee, Straight from Knee/Thigh,
               Straight from Thigh, Bootcut, Flare, Wide Leg, Barrel, Baggy,
               Boyfriend
Inseam Label   Regular, Petite, Long
Inseam Style   Cropped, Ankle, Full Length
Rise Label     Low, Mid, High
```

Note **"Straight from Knee"** - lower-case "from".

## Straight-leg buckets

From Leg Opening, in inches. The lower boundary is 15.5 everywhere; **the upper boundary
varies by brand and is expected to** - brands cut a "straight" differently, so the number
is tuned per brand against their actual fits.

```
< 15.5         Straight from Knee
15.5 - <upper> Straight from Knee/Thigh
> <upper>      Straight from Thigh
```

What the existing scrapers use:

|brand|upper boundary|
|-|-|
|Ramy Brook|`<= 17.0`|
|Paige|`<= 17.0`|
|Good American|`<= 17.0`|
|AMO|`< 17.5`|
|Edyson|`< 17.5`|

For a new brand, start at 17.0, then check the result against a dozen styles on their site
and move it if their straights run wide. **Say in a comment which it is and why.**

## Inseam Label

Decide in this order, first hit wins:

1. a length word in the **handle**
2. the variant's own **Regular / Long / Petite option**
3. the **brand's product title**
4. the **inseam number** as a last resort

Then, **after the alt title is final, make the label agree with it.** If the alt title
carries a length segment and the label disagrees, the title wins - a stated length beats a
measured one. This matters because the length segment can be set by the multi-inseam rule
long after the label was derived; without this pass a 33.5" flare reads `Long` while its
own title says `REGULAR`.

Rows whose title has no length segment keep whatever the four-step order produced.

## Multi-inseam detection ("Inseam Label Multi")

`Multi` = sold in more than one length. Everything starts `Single`; two rules promote it;
**nothing ever demotes it.**

1. **Title collision.** Strip the length segment off every alt title. If one base shows
   more than one distinct length, every row with that base is Multi. A *missing* length
   counts as a distinct value, which is how the unlabelled regular member gets caught.
2. **The product page.** For rows still Single that *do* carry a length segment, read the
   page's length-switcher (the Regular / Petite / Long buttons near the size picker). If
   it lists sibling pages, **mark every page in that family Multi**, including ones never
   checked. An empty switcher is a real answer meaning "one length only".

Then give every Multi row an explicit length: any Multi row with no length segment gets a
`REGULAR` segment appended. That REGULAR means *the member that is not the petite or long
one* - a position in the family, not a measurement.

**This rule can only ever miss a Multi, never invent one** - a blocked page and a
single-length page look identical, and a family cannot form if the sibling is absent from
the run. That is why the database treats Multi as sticky.

---

# 4. Formatting

|thing|rule|
|-|-|
|separator|per brand (see 2). Consistent spacing, never doubled, never missing|
|`90s`|lower-case `s`, always. Never `90S`|
|tags|joined with `, `|
|dates|`MM/DD/YYYY` - zero-padded|
|date + time|`MM/DD/YYYY HH:MM:SS` - zero-padded, **24-hour, no AM/PM**|
|prices|`$178.00` - leading `$`, always 2 decimals (`$1,234.56` above a thousand)|
|measurements|plain decimals, no units and no inch marks: `27.125`, `9.87`, `19.125`|
|quantities|plain integers, no separators: `123`, `3425`|
|quotes and dashes|straight ASCII only - replace curly quotes and en/em dashes|
|accents|strip them (`KHLOÉ` -> `KHLOE`)|
|whitespace|collapse runs of spaces; strip leading/trailing; no non-breaking spaces|
|sizes|**always text, never numeric.** `00` must not become `0`; both are real sizes|

## Brackets

**A bracketed phrase inside a name is deliberate** - it is there because another item would
otherwise share that name and is a different style, e.g.
`GOOD LEGS FLARE JEANS [CLEAN HEM] | BLUE004`. **Never strip brackets**, and never write a
regex that sweeps `\[.*\]`.

---

# 5. Fetching product pages (PDPs)

Most brands are Shopify behind Cloudflare. Nearly every scraper that reads product pages
has had trouble here; Good American is the one that runs smoothly, and this is what it
does. Follow it literally - each rule below exists because its absence caused a specific
failure.

## 5.1 One cached fetch per page, shared by everything

**Fetch each product page once per run and let every field read the same copy.** Good
American reads four different things off the PDP - a size-guide heading, sister-variant
links, inseam siblings, and whether the page still exists. Fetching per-lookup instead of
per-page tripled the request count, which is what provoked the blocking in the first
place.

```python
_PDP_PAGE_CACHE   = {}   # handle -> html (or None)
_PDP_STATUS_CACHE = {}   # handle -> HTTP status, so 404 != failure
_PDP_THROTTLED    = set()# handles blocked by the challenge, deliberately uncached
_PDP_ATTEMPTS     = {}   # handle -> requests spent, across the whole run
```

## 5.2 Only fetch when the cheaper source has already failed

The PDP is the last resort, not the first. Gate every lookup on the field still being
empty after the collection/GraphQL data has been used:

```python
pending = sorted({r["Handle"] for r in rows if not r["Inseam"].strip()})
```

Good American ends up fetching ~150 pages for ~900 products because of this. Fetching
every product's page is the difference between a two-minute run and a blocked one.

## 5.3 Pace every request globally

One pacer for **all** requests to the site, not one per phase - the limit is per-IP and
every phase draws on the same budget.

```python
PDP_MIN_INTERVAL   = 0.5   # floor: seconds between any two requests
PDP_MAX_INTERVAL   = 6.0   # ceiling once challenges have forced the pace down
PDP_INTERVAL_STEP  = 0.75  # added after each 429
PDP_INTERVAL_DECAY = 0.9   # multiplier after each clean response
PDP_429_MAX_WAIT   = 15.0  # cap on any single wait
PDP_HANDLE_ATTEMPTS = 2    # requests per handle for the whole run

def _pace_request():
    wait = _PACE["last"] + _PACE["interval"] - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _PACE["last"] = time.monotonic()
```

**The pace must adapt in both directions.** Slow down after a challenge, and decay back
toward the floor after clean responses. A one-way ratchet is what turned one bad page into
a 27-minute run: the pace climbed to 5.6s and stayed there for the remaining 150 requests.

## 5.4 A 429 is usually a bot challenge, not a rate limit

Check the body. If it contains Cloudflare's interstitial, waiting will not clear it:

```python
def _is_bot_challenge(response):
    return "Verifying your connection" in (response.text or "")[:2000]
```

This distinction drives everything else:

* **Cap the wait.** The server's `Retry-After` assumes a rate limit it will forgive - a
  challenge is not forgiven on a timer. One run honoured `Retry-After: 60` six times on a
  single handle and got the challenge back every time. Honour the header, but capped.
* **Cap attempts per handle at two,** across the whole run. One retry catches a lifted
  challenge; a third never helped, and without the cap a blocked page gets re-requested by
  every phase that wants it.
* **Never add concurrency to get around it.** The limit is per-IP, so parallel requests
  hit the wall faster.

```python
def _get_with_backoff(session, url, **kwargs):
    response = None
    for attempt in range(RETRIES):
        _pace_request()
        response = session.get(url, timeout=30, **kwargs)
        if response.status_code != 429:
            _PACE["interval"] = max(PDP_MIN_INTERVAL,
                                    _PACE["interval"] * PDP_INTERVAL_DECAY)
            return response
        _PACE["interval"] = min(PDP_MAX_INTERVAL,
                                _PACE["interval"] + PDP_INTERVAL_STEP)
        if attempt + 1 >= RETRIES:
            break
        retry_after = response.headers.get("Retry-After", "").strip()
        wait = (float(retry_after) if retry_after.replace(".", "", 1).isdigit()
                else BASE_SLEEP * (attempt + 1))
        wait = min(wait, PDP_429_MAX_WAIT)
        logging.warning("%s on %s; waiting %.1fs (pace now %.1fs)",
                        "Bot challenge (429)" if _is_bot_challenge(response)
                        else "Rate limited (429)", url, wait, _PACE["interval"])
        time.sleep(wait)
    return response
```

## 5.5 Tell the four outcomes apart

A page that could not be read is **not** a page that says nothing. Keep the status, not
just the HTML:

|status|meaning|what to do|
|-|-|-|
|200|read it|cache the HTML|
|404|the product is genuinely gone|cache it; safe to act on|
|429|blocked - unknown|**do not cache**, so a later phase can retry|
|other / timeout|unknown|cache nothing conclusive; never read as "gone"|

**Never cache a 429 as "missing".** Caching the empty result loses the handle for the
whole run even after the pacer has backed off far enough to read it.

**Never infer deletion from an empty result.** Good American drops delisted products by
checking the PDP, and that check requires *exactly* 404:

```python
def pdp_is_missing(session, handle):
    fetch_product_page(session, handle)
    return _PDP_STATUS_CACHE.get(handle) == 404
```

Before that, a transient 500 or timeout silently dropped live styles from the output.

## 5.6 Send browser headers

A lone `User-Agent` is a known trigger for the managed challenge - a real Chrome never
sends one without the rest:

```python
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none", "Sec-Fetch-User": "?1",
    "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": '"Windows"',
}
```

## 5.7 Lazy-loaded content: use the section endpoint, not a browser

Content behind a click - a size guide, a measurements drawer - is usually not in the page
HTML. On Shopify it can be fetched directly with the Section Rendering API rather than
driving a browser:

```
https://<site>/products/<handle>?sections=<section-id>
```

Find the section id in the page's own loader markup. **Do not send
`Accept: application/json`** - Shopify answers that with an empty section body; the
browser's default Accept is what returns the rendered markup. Count these as requests and
pace them like any other.

## 5.8 Report what the run actually did

End every run with the real numbers and, separately, the losses - a degraded run must not
look like a clean one:

```
Product pages downloaded: 153 (plus 27 size-guide section requests); 182 HTTP GETs
in total, 2 of them rate-limited; final pace 0.5s between requests
```

```python
if _PDP_THROTTLED:
    logging.warning("PDP never read for %s handle(s) (blocked by the bot challenge, "
                    "so their derived fields fall back to weaker evidence): %s",
                    len(_PDP_THROTTLED), ", ".join(sorted(_PDP_THROTTLED)))
```

That second line is how you tell a clean run from one that quietly lost data. Check it
before trusting any field that comes off the PDP.

## 5.9 Make the pace tunable

How readily the challenge fires depends on the IP, so the floor is a starting point, not a
constant. Expose it:

```python
parser.add_argument("--pdp-interval", type=float, default=PDP_MIN_INTERVAL)
```

If the log still shows challenges, raise it. If the brand never challenges, lower it.

## 5.10 What not to do

* **Do not rotate hosts to dodge the limit.** Good American's `checkout.` and
  `.myshopify.com` hosts both redirect to `www`, so they share one limiter. Check before
  assuming alternates are independent.
* **Do not let a failure write a blank.** An unreadable page must leave the field
  untouched and be reported, never fill it with "".
* **Do not keep a global ratchet.** See 5.3.
* **Do not copy an unthrottled scraper's approach because it looks faster.** One scraper
  in this repo makes an unpaced request per product with no 429 handling at all; measured
  against its own site it silently dropped ~7% of pages, because a raised exception there
  returns an empty record and the run carries on looking fine.

# 6. Housekeeping

* `Output/` beside the script; file named `BRAND_YYYY-MM-DD_HH-MM-SS.csv`, brand
  upper-case. The database reads this as `source_file_name`, so the shape is fixed.
* A log file per brand. Every line starts with a Central-time timestamp. Log phase
  boundaries, counts, and anything skipped.
* Keep brand-specific word lists (filter words, excluded product types, style keywords)
  **in named constants at the top of the file**, not buried in the logic.
* Match on **whole words**, case-insensitively, when filtering by keyword. A plain
  substring match turns "Top" into a hit on the colour "Topaz".
* Comment the **why**, not the what - especially where a rule looks wrong but is
  deliberate.

# 7. Things not to do

* **Do not guess.** Where the evidence does not resolve, leave the field blank and report
  the row. A wrong value is worse than an empty one.
* **Do not silently drop a product.** If something is excluded, log it.
* **Do not trust a cached spreadsheet column.** Recompute from source; formula columns go
  stale.
* **Do not treat absence as deletion.** A row missing from one run is not a dead product.
* **Do not store calculation helpers** in the database. If a value exists only to group
  rows during a calculation, it stays out of the output.

# 8. Before you call it done

Run it twice and diff the two outputs. Only volatile columns (quantities, availability,
tags, prices, image URLs, timestamps) should differ. Then check:

```
any 90S (capital S) anywhere                                 none
double spaces, or inconsistent separator spacing             none
Variant Title follows the order in 2, OLD in the inseam slot  every row
Inseam Label Multi = Multi but no length segment             none
Style Name in ALL CAPS, no colour, no size                   every row
derived values spelled exactly as 3                          every row
size '00' still '00', not '0'                                every row
dates MM/DD/YYYY, datetimes MM/DD/YYYY HH:MM:SS              every row
prices formatted $###.##                                     every row
rows where a page could not be read                          reported, not blank
the run's final pace is back near the floor                  (a stuck pace means 5.3)
product pages fetched vs products in the run                 far fewer (see 5.2)
credentials anywhere in the log or output                    none
```

Then spot-check ten styles against the live site by hand, including one multi-inseam style
and one whose name the brand has recently changed. If you were given a correction file,
check your output against it and raise anything that disagrees rather than resolving it
yourself.
