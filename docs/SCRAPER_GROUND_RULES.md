# Brand scraper ground rules

How to write a new brand scraper, or change an existing one, so that every brand lands in
the database the same shape. Written from the eight scrapers already in the repo
(Good American, Ramy Brook, AMO, AG Jeans, Paige, Edyson, AYR, Citizens of Humanity).

Read Part 6 before you start on a brand that is already live: several conventions are
currently inconsistent between scrapers, and those need a decision rather than a guess.

---

# 1. The output contract

One CSV per run, one row per variant (SKU) per run. These 28 columns appear in every
existing scraper and are the minimum:

```
Style Id        Handle          Published At    Created At      Product
Style Name      Product Type    Tags            Vendor          Description
Variant Title   Color           Size            Inseam          Price
Compare at Price                Available for Sale              Quantity Available
Quantity of style               SKU - Shopify   SKU - Brand     Barcode
Image URL       SKU URL         Jean Style      Inseam Label    Inseam Style
Rise Label
```

Add these when the brand exposes them; most scrapers carry them:

```
Rise            Leg Opening     Product Line    Hem Style       Stretch
Color - Simplified              Color - Standardized            Country Produced
Returns         Daily Sales     Purchase Reset Date
Quantity Available (Instore Inventory)          Quantity Available (Online Inventory)
Production Cost                 Site Exclusive
```

**Use the column order above**, then the optional ones. Never rename an existing column to
suit one brand - the database loads them by name.

**One row per variant, always.** Do not collapse sizes. Style-level values (Quantity of
style, Style Name, Product Line) repeat on every row of the style.

---

# 2. The naming model

Four name fields do four different jobs. Getting these confused is the single most common
source of bad data.

|field|what it is|
|-|-|
|**Product**|the brand's own title, lightly cleaned. Keeps their wording. The length word sits before the pipe: `NEVER FADE GOOD STRAIGHT JEANS PETITE \| BLACK`|
|**Style Name**|the derived style, ALL CAPS, no colour, no size, no "JEANS"|
|**Product Title Alt**|the normalised piped title: `NAME \| COLOUR` or `NAME \| COLOUR \| LENGTH`|
|**Variant Title**|exactly `Product Title Alt` + ` \| ` + `Size`|

Rules that follow from this:

* **Variant Title is never built independently.** It is the alt title plus the size. If
  they disagree, the alt title is right.
* **The length segment of the alt title is the last segment**, and only `PETITE`, `LONG`
  or `REGULAR` count. A trailing colour or size means there is no length segment.
* **`Product` keeps the brand's wording even when it looks wrong.** If the brand calls it
  `BLACK` and the normalised colour is `BLACK001`, Product says BLACK and the alt title
  says BLACK001. That is correct, not a bug.

## Style identity

**`SKU_BRAND_NO_SIZE` identifies a style**, not `product_name`. Two different runs of the
same jean (an old one being sold off and its replacement) can share a product_name on
purpose so their sales report together - the stem is what still tells them apart.
`product_name` is a reporting label. **Never key or group on product_name alone.**

`SKU_BRAND_NO_SIZE` is a **calculation helper and is never stored in the database.** There
is no column for it and none should be added: the derivation depends on how a given brand
structures its SKUs, and plenty of brands use effectively random numbers.

### Deriving it

The goal is the SKU with its size part removed, so every size of one style collapses to a
single key. The trap: the SKU's trailing segment often **is not** the size. Sizes
`00-4` ... `28-32 Plus` can appear as `-A` ... `-E`; `XS` ... `3XL` as `-0` ... `-8`.

1. **Group the product's variants by their non-size options** (Colour, and Inseam where
   present). A style sold in Regular and Long forms **two** groups, not one.
2. **Take the longest common prefix of each group's SKUs**, cut it back to the last `-`,
   and drop trailing dashes. Grouping first is what preserves a Long run's inseam token,
   e.g. the `-35` in `...-35-00-4`.
3. **Single-SKU group fallback** - no sibling means no shared prefix, so strip the size by
   text. Try, longest first: the size as-is; spaces removed; spaces as dashes; and for a
   size ending PLUS/PETITE/LONG, the stem plus `P`/`P`/`L` (so `14-18 PLUS` also tries
   `14-18P`). If none match, peel the size's tokens off the end one at a time. If nothing
   matches, keep the SKU unchanged.

This path is common - roughly one group in eight - and a prefix can never produce the
right answer for it. Test case: `GLSTAF739T-I571-6-12` must give `GLSTAF739T-I571`, not
`GLSTAF739T-I571-6`.

## Quantity of style

Take `Quantity Available` as an integer; blank, missing or non-numeric is **0**; negatives
are **kept negative** (oversold reduces the total). Sum across all rows sharing the same
`SKU_BRAND_NO_SIZE`, regardless of handle or Style Id, **within the same capture**. Write
the group total onto every row in the group; a row with no stem gets blank.

Because grouping is by stem, a Regular run and a Long run of one style total separately
while all sizes within a run total together.

**This value is a repeated group total, so it must never be SUMmed across rows** by
anything reading the output.

---

# 3. Derived fields: exact values

The derived values are compared across brands, so the **spelling and capitalisation must
match exactly**. Use these strings and no variants:

```
Jean Style     Skinny, Slim, Straight from Knee, Straight from Knee/Thigh,
               Straight from Thigh, Bootcut, Flare, Wide Leg, Barrel, Baggy,
               Boyfriend
Inseam Label   Regular, Petite, Long
Inseam Style   Cropped, Ankle, Full Length
Rise Label     Low, Mid, High
```

Note **"Straight from Knee"** - lower-case "from". Good American currently writes
"Straight **F**rom" and is the only one; see Part 6.

## Straight-leg buckets

From Leg Opening, in inches:

```
< 15.5        Straight from Knee
15.5 - 17.0   Straight from Knee/Thigh
> 17.0        Straight from Thigh
```

The upper bound currently differs between scrapers (17.0 vs 17.5). See Part 6.

## Inseam Label

Decide in this order, first hit wins:

1. a length word in the **handle**
2. the variant's own **Regular / Long / Petite option**
3. the **brand's product title**
4. the **inseam number** as a last resort

Then, **after the alt title is final, make the label agree with it.** If the alt title
carries a length segment and the label disagrees, the title wins. A stated length beats a
measured one. This matters because the length segment can be set by the multi-inseam rule
long after the label was derived - without this pass a 33.5" flare reads `Long` while its
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

Then give every Multi row an explicit length: any Multi row with no length segment gets
`| REGULAR` appended. That REGULAR means *the member that is not the petite or long one* -
a position in the family, not a measurement.

**This rule can only ever miss a Multi, never invent one** - a blocked page and a
single-length page look identical, and a family cannot form if the sibling is absent from
the run. That is why the database treats Multi as sticky.

---

# 4. Formatting

|thing|rule|
|-|-|
|pipe separator|always ` \| ` - one space either side, never doubled, never missing|
|`90s`|lower-case `s`, always. Never `90S`|
|tags|joined with `, `|
|dates|`M/D/YYYY`|
|date + time|`M/D/YYYY H:MM:SS AM/PM`, **US Central**|
|quotes and dashes|straight ASCII only - replace curly quotes and en/em dashes|
|accents|strip them (`KHLOÉ` -> `KHLOE`)|
|whitespace|collapse runs of spaces; strip leading/trailing; no non-breaking spaces|
|sizes|**always text, never numeric.** `00` must not become `0`; both are real sizes|

Price format currently differs between scrapers. See Part 6.

## Brackets and markers

* **A bracketed phrase inside a name is deliberate** - it is there because another item
  would otherwise share that name and is a different style, e.g.
  `GOOD LEGS FLARE JEANS [CLEAN HEM] | BLUE004`. **Never strip brackets** and never write
  a regex that sweeps `\[.*\]`.
* **`OLD`** appended to an alt title marks a superseded run of a style that has been
  replaced. It follows the length word when there is one: `... | BLUE004 | LONG OLD`.

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
  all requests to `/products/`, not one per phase.
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

# 6. Decisions needed - currently inconsistent

These genuinely differ between the existing scrapers. Pick one of each and apply it
everywhere; do not copy whichever neighbour you happened to open.

|thing|current state|
|-|-|
|**Straight bucket upper bound**|`<= 17.0` in Ramy Brook and Paige; `< 17.5` in AMO and Edyson. Good American uses `15.5 - 17`|
|**"Straight from" casing**|seven scrapers write `Straight from Knee`; **Good American writes `Straight From Knee`** and is the outlier|
|**Price format**|`$1,234.56` in Ramy Brook and Edyson; `1234.56` in AMO; raw value in Paige|
|**Date zero-padding**|`M/D/YYYY` in Ramy Brook, Edyson, AG Jeans; `MM/DD/YYYY` in Paige|
|**Leg Opening / Rise**|carried by most brands, absent from Good American|

---

# 7. Housekeeping

* `Output/` beside the script; file named `BRAND_YYYY-MM-DD_HH-MM-SS.csv`, brand upper-case.
* A log file per brand; log phase boundaries, counts, and anything skipped.
* Keep brand-specific word lists (filter words, excluded product types, style keywords)
  **in named constants at the top of the file**, not buried in the logic.
* Match on **whole words**, case-insensitively, when filtering by keyword. A plain
  substring match turns "Top" into a hit on the colour "Topaz".
* Comment the **why**, not the what - especially where a rule looks wrong but is
  deliberate.

# 8. Things not to do

* **Do not guess.** Where the evidence does not resolve, leave the field blank and report
  the row. A wrong value is worse than an empty one.
* **Do not silently drop a product.** If something is excluded, log it.
* **Do not trust a cached spreadsheet column.** Recompute from source; formula columns go
  stale.
* **Do not treat absence as deletion.** A row missing from one run is not a dead product.
* **Do not store helper values** (`SKU_BRAND_NO_SIZE`) in the database.

# 9. Before you call it done

Run it twice and diff the two outputs. Only volatile columns (quantities, availability,
tags, prices, image URLs, timestamps) should differ. Then check:

```
any 90S (capital S) anywhere                                 none
double spaces, or pipe spacing other than ' | '              none
Variant Title = Product Title Alt + ' | ' + Size             every row
Inseam Label Multi = Multi but no PETITE/LONG/REGULAR        none
Style Name in ALL CAPS, no colour, no size                   every row
derived values spelled exactly as Part 3                     every row
size '00' still '00', not '0'                                every row
rows where a page could not be read                          reported, not blank
SKU_BRAND_NO_SIZE written to any output column               none
```

Spot-check ten styles against the live site by hand, including one multi-inseam style and
one whose name the brand has recently changed.
