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

# 5. Fetching

Most brands are Shopify behind Cloudflare. The rules below came out of debugging a run
that took 27 minutes and lost 20 styles.

**Send browser headers, not a bare User-Agent.** A lone UA is a known trigger for
Cloudflare's managed challenge. Send `Accept`, `Accept-Language`,
`Upgrade-Insecure-Requests`, the `Sec-Fetch-*` set and `sec-ch-ua`.

**A 429 is usually a bot challenge, not a rate limit.** Check the body: if it is the
"Verifying your connection" interstitial, waiting does not clear it. Specifically:

* **Cap the wait.** Do not honour a long `Retry-After` in full - the server assumes a rate
  limit it will forgive, and a challenge is not forgiven on a timer.
* **Pace in both directions.** Slow down after a challenge, and **decay back to the floor**
  after clean responses. A one-way ratchet turns one bad page into a slow whole run.
* **Cap attempts per page** across the run. Two is enough: one retry catches a lifted
  challenge, a third never helps.
* **Do not cache a blocked page as "missing".** A later phase should be able to retry it.
* **Pace globally.** The limit is per-IP and shared by every lookup, so one pacer covers
  all requests, not one per phase.
* **Never add concurrency to work around a 429.** The limit is per-IP; parallel requests
  hit the wall faster.

**Never let a failed request look like an answer.** A 404 means gone; a 429, a 500 or a
timeout means unknown. Distinguish them, and never write a blank value that a successful
read would have filled.

**Report what was lost.** End the run with a line naming any page that could not be read,
so a degraded run does not look like a clean one. Also log the real request count and how
many were challenged.

**Fetch a page once and share it.** If several derived fields read the same product page,
fetch it once and cache it for the run.

---

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
credentials anywhere in the log or output                    none
```

Then spot-check ten styles against the live site by hand, including one multi-inseam style
and one whose name the brand has recently changed. If you were given a correction file,
check your output against it and raise anything that disagrees rather than resolving it
yourself.
