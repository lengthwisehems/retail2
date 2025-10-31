# Inventory Scraping Playbook

This document captures the conventions and brand-specific notes for the denim inventory scrapers maintained in this repository. It is intended to let a new contributor ramp quickly, understand where each CSV field comes from, and avoid breaking the existing data paths.

## Shared conventions (read this slowly – skipping any bullet will break the feeds)

- **Runtime environment**: All scrapers use Python 3.11+ with `requests` and, when needed, `beautifulsoup4`. We standardize on a single `requests.Session` per run with a desktop User-Agent and exponential backoff around transient HTTP errors (429/5xx). **Always** log major milestones (collection page counts, Searchspring/Algolia pagination, fallbacks) in the command prompt while the script is running so stalled runs are obvious.
- **Output layout**: Every script defines `BASE_DIR = Path(__file__).resolve().parent`, `OUTPUT_DIR = BASE_DIR / "Output"`, and a brand-specific `LOG_PATH`. `OUTPUT_DIR` is created up front and all exports are timestamped `BRAND_YYYY-MM-DD_HH-MM-SS.csv` (24-hour clock). Logs append to `[brand]_run.log` in the same directory and gracefully fall back if the preferred path is unavailable.
- **Logging fallbacks**: When configuring logging handlers, always attempt to open the primary log file inside a try/except block and fall back to `OUTPUT_DIR / "[brand]_run.log"` (or stream-only logging) if the primary path is locked. Emit a warning so the automation log explains which destination is active. Apply this pattern to every new scraper.
- **CSV schema**: Start from the baseline schema
  `Style Id, Handle, Published At, Product, Style Name, Product Type, Tags,
  Vendor, Description, Variant Title, Color, Size, Rise, Inseam, Leg Opening,
  Price, Compare at Price, Available for Sale, Quantity Available, Quantity of
  style, SKU - Shopify, SKU - Brand, Barcode, Image URL, SKU URL` and adjust per
  brand instructions. Depending on the information available more headers may be added and some maybe deleted. Where the information for each of these headers can be sourced is detailed in the prompt for when scraping a new brand. The format of the mapping is pasted as a table in prompt. The layout goes:
Header for CSV: this is what should be put as the label in the csv output. This will change from brand to brand so pay attention to what is an isn’t listed in the mapping
Label in Data/json/html: this is usually the word to look for in order to find the data that should be scraped. This will change from brand to brand so pay attention to what is listed in the mapping.
Example: show an example of the type and formatting of the information that should be pulled in
Where found: the URL or name of page for where the information can be found. This will change from brand to brand so pay attention to the source.
Where Found (details): additional help in the event the information may be difficult to find, has specific rules for what should/shouldn’t be pulled, or requires additional formatting details. This may not always be filled. What is/isn’t filled and what instructions are given will change from brand to brand so pay attention to the mapping in the prompt.

- **Logging**: Log major milestones (collection page counts, Searchspring/Algolia pagination, fallbacks) and any retries so production runs can be audited.
- **Retry policy**: Treat 429/5xx as transient, sleep with exponential backoff, log successful fallbacks, and rotate through host fallbacks by adding alternate domain (e.g., amodenim.com vs www.amodenim.com).
- **Do not regress working code**: When adding features, leave the validated inventory path untouched. New behavior should sit behind clearly documented flags or separate functions.
- **Pre-emptive fixes**: Refer to each brand's **Edits made to fix repeated scraping failures** to implement preventative fixes when writing new code
- **Timeout Errors**: If a brand keeps getting time out errors while trying to visit the HTTP site, create 2 files: brand_inventory_with_measurements.py and brand_inventory.py. brand_inventory_with_measurements includes values that can only come from the HTML like Rise, Inseam, and Leg Opening. brand_inventory.py does not pull values that can only come from the HTML like Rise, Inseam, and Leg Opening but does keep the headers on the csv output for layout consistency. brand_inventory.py is run every day with the outputfile name of `BRAND_YYYY-MM-DD_HH-MM-SS.csv` while brand_inventory_with_measurements.py is run monthly with a file output name of `BRAND_Measurements_YYYY-MM-DD_HH-MM-SS.csv`. First build out the brand_inventory_with_measurements.py. Then once given confirmation that the code is good, and have no more edits, then you can write brand_inventory.py and add it to run_all.bat

### Data hygiene rules (non-negotiable)

1. **Treat every identifier as text.** Shopify product ids, variant ids, SKUs, and barcodes must stay as strings. Casting to `int` will chop leading or trailing zeros (e.g., `56622797685120` → `5662279768512`) and corrupt downstream reconciliations. Call `str(value)` if the API returns a number.
2. **Measurements must keep decimals.** The shared `parse_measurement` helper handles whole numbers, decimals, and fractions (`10 3/4`). Always convert centimetres to inches with `Decimal` math and round using `quantize(Decimal("0.01"), ROUND_HALF_UP)` so `11.25` does not become `11`.
3. **Respect option fallbacks.** If `option2` or `option3` is missing, reuse `option1` so Color/Size are still populated. Never leave the column blank because you forgot to branch.
4. **Follow the exclusion rules exactly.** Each prompt calls out handles, tags, or product types to skip. Implement those tests verbatim and add asserts covering the sample handles supplied in the prompt before you ship changes.
5. **Use the key specified for joins.** Searchspring merges are keyed by `ss_id`, Nosto merges by Shopify variant id, etc. Do not attempt fuzzy joins—if a record is missing, log it for manual review.
6. **Format dates as `MM/DD/YYYY`.** Convert Shopify ISO timestamps with `datetime.fromisoformat(...).strftime("%m/%d/%Y")`. Locale defaults will vary between machines.
7. **Sanitize HTML before parsing.** Replace `&nbsp;` with spaces, strip tags with BeautifulSoup, and normalise whitespace before searching for measurement tokens.

If you are tempted to “just cast to int” or “round the measurement”, stop and build the proper helper. The last person who ignored this instruction shipped CSVs with missing decimals and truncated ids—do not repeat that mistake.


## Brand notes

### Haikure (`haikure_inventory.py`)
- **Catalog source**: Shopify collection feed `https://haikure.com/collections/denim/products.json?limit=250&page=n` for all style-level fields (Style Id, Handle, Published At, Product, Product Type, Tags, Vendor, Variant Title, Size seed, Price, Compare at Price, Available for Sale, SKU - Shopify, Image URL, SKU URL).
- **PDP parsing**: Fetch `view-source:https://haikure.com/products/<handle>` once per style. We pull:
  - Description block (HTML cleaned to text) and Color bullet list.
  - `window.inventories` script for per-variant `quantity`, `incoming`, and `next_incoming_date`; aggregate `Quantity Available`, `Next Shipment`, and style-level totals.
  - Sixth list item of the description bullet for the 18-character `SKU - Brand`; skip if the token does not match the alphanumeric length requirement.
- **Barcode**: `https://haikure.com/products/<handle>.json` (Shopify product
  JSON) still exposes `variants[].barcode`.
- **Sizing**: Use Shopify `option1` only as a fallback; some drops encode sizes in brand SKU strings.
- **Measurements**: parse Rise/Back Rise/Inseam/Leg Opening from the PDP description list.
- **Outputs**: Keep `Quantity Price Breaks` and other legacy columns even when empty to preserve downstream workbook formulas.

### Paige (`paige_inventory.py`)
- **Catalog & metadata**: Algolia Search API (`production_products` index). One pass with `distinct=true` and `filters=collections:women-denim` for style-level hits (Style Id, Handle, Product, Style Name via `styleGroup`, Product Type via `clothingType`, Tags, Vendor, Description (`body_html_safe`), Price/Compare at Price/Range, Quantity of style, Product Line, Image URL, SKU URL, Jean Style (`fit`), Inseam Label (`length` + `sizeType`), Rise Label (`rise`), Color fields (`wash`, `colorCategory`), Country Produced (`country` tag), Stretch, Production Cost, Site Exclusive).
- **Variant detail**: For each style id, issue a second query with `distinct=false` and `filters=id=<style>` to retrieve every variant. Pull Variant Title, Size (from `options.size`), Color, Published At, Availability, Quantity Available, Google Analytics Purchases, SKU - Brand, Barcode, and the Shopify variant id for `SKU - Shopify`.
- **Measurements**: Paige’s PDP measurements were unreliable behind bot protection; we removed Rise/Inseam/Leg Opening columns entirely to keep exports stable.
- **Output flow**: Build all Algolia-driven rows first, then (optionally) hydrate extra PDP fields in a second pass. Logging is suppressed when the PDP fetch fails so the main CSV still completes.

### Pistola (`pistola_inventory.py`)
- **Catalog**: Shopify collection feed `collections/all-denim/products.json?limit=250&page=n` to enumerate variants, published dates, tags, vendor, compare-at pricing, availability, images, and product handles. Deduplicate handles when the same style appears in multiple pages.
- **Nosto GraphQL**: The category page exposes `accountId` and `categoryId` in the embedded script payload. Query `https://search.nosto.com/v1/graphql` with those ids to retrieve:
  - Style metadata: description copy, price, price range, ss facets (Fit, Length, Rise, Wash, Stretch), product line items.
  - Variant-level fields inside `skus[]`: Shopify variant id (`id`), SKU, barcode (`customFields.gtin`), color (`customFields.color`), size (`customFields.size`), `inventoryLevel` (Quantity Available).
  - `product` level `inventoryLevel` gives Quantity of style.
  - De-duplicate handles when merging the two feeds.
- **Measurements**: Parse `info-tab1` strings returned in Nosto metadata (Rise, Inseam, Leg Opening) using a fraction-aware helper.
- **Shopify-specific**: Because the Shopify feed keeps `product_type` blank for some products, we normalize using Nosto `CATEGORY` tags.
- **Sizes**: normalize size strings (strip trailing `P`, set inseam label to `Petite` when applicable).
- **Edits made to fix repeated scraping failures**: Logging initialization originally pointed at a locked OneDrive file, raising `PermissionError` before scraping started. Configuration lives at the top of the file; logging now targets the base directory and gracefully falls back if the preferred path is unavailable.


### RE/DONE (`redone_inventory.py`)
- **Catalog union**: Merge the standard and sale Shopify collection feeds (`https://shopredone.com/collections/denim/products.json?limit=250&page=n` and `https://shopredone.com/collections/sale-denim-all/products.json?limit=250&page=n`). Drop duplicate handles but preserve the earliest Published At date.
- **Searchspring**: Call `https://w7x7sx.a.searchspring.io/api/search/search.json` with both `bgfilter.collection_handle=denim` and `sale-denim-all` to collect Searchspring product payloads. Each hit supplies:
  - Product Type via `tags_sub_class` / `product_type_unigram` mapping.
  - Quantity of style (`ss_inventory_count`).
  - Jean Style, Inseam Label, Rise Label, color groupings, and Stretch via the various `tags_*` lists (see `determine_product_type`, `derive_jean_style`, `derive_inseam_label`).
  - `ss_size_json` contains per-variant `id` (Shopify variant id) and `available` quantity—use this as the authoritative `Quantity Available` to avoid the 50-unit cap in the PDP JSON.
- **Barcodes**: Shopify product JSON `products/<handle>.json` furnishes variant barcodes and quantities but note the 50-unit cap—Searchspring is the primary quantity source.
- **Variant basics**: Pull SKU - Brand, barcode, etc. from Shopify `variants[]`, and use `clean_html` to flatten `body_html` into Description text.
- **Edits made to fix repeated scraping failures**: Logging initialization originally pointed at a locked OneDrive file, raising `PermissionError` before scraping started. The logger now prefers a base-directory log file, only falling back to the output directory if necessary.

### AG Jeans (`agjeans_inventory.py`)
- **Catalog**: Shopify women’s jeans collection feed  (`https://www.agjeans.com/collections/womens-jeans/products.json?limit=250&page=n`)
for Style Id, Handle, Published At, Product, Tags, Vendor, Variant Title (Product + variant title), base options, Price/Compare at Price, Available for Sale, SKU - Shopify, image gallery, and SKU URL.
- **Algolia**:  Query the Search API to find the index (`shopify_main_products`). Use the `shopify_main_products` index via `https://ao8siisku6-dsn.algolia.net/1/indexes/.../query` to pull style-level metadata (Style Name via `product`, Product Type via `named_tags.Category`, Price Range, inventory aggregates, Jean Style, Hem Style, Inseam/Rise labels, Wash/Fabric/Stretch tags, barcodes, quantity counts). Filter to the women-denim collection and page with `page=0..N`.
- **Variants**: For each handle, issue `distinct=false` Algolia queries to collect per-SKU inventory and GA metrics. Match entries by SKU - Brand to populate the variant rows.
- **Measurements**: Fallback to PDP HTML (`view-source`) for Rise and Leg Opening when not exposed in Algolia’s `info-tab1` data. Convert fractions to decimals.
- **Variant title**: combine product title and size (`name` + size) as instructed.
- - **2 versions** As we kept getting errors during the scrape like "GET https://www.agjeans.com/products/<handle> (ConnectionError(MaxRetryError("HTTPSConnectionPool(host='www.agjeans.com', port=443): Max retries exceeded with url: /products/<handle> (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x000001F599F0BA10>: Failed to establish a new connection: [WinError 10051] A socket operation was attempted to an unreachable network'))"))) -> sleep" We will create 2 codes: agjeans_inventory_with_measurements.py and agjeans_inventory.py. agjeans_inventory_with_measurements includes values for Rise, Inseam, and Leg Opening. agjeans_inventory.py does not pull values for Rise, Inseam, and Leg Opening but does keep the headers on the csv output for layout consistency. agjeans_inventory.py is run every day with the outputfile name of `AGJEANS_YYYY-MM-DD_HH-MM-SS.csv` while agjeans_inventory_with_measurements.py is run monthly with a file output name of `AGJEANS_Measurements_YYYY-MM-DD_HH-MM-SS.csv`

### Mother Denim (`motherdenim_inventory.py`)
- **Catalog**: Merge `collections/denim/products.json` and `collections/denim-sale/products.json` (Shopify). Keep unique handles and prefer the earliest `published_at`. Columns: Style Id, Handle, Published At, Product, Tags, Vendor, Description, Variant Title, Color/Size, Price, Compare at Price, Available for Sale, SKU - Shopify, SKU - Brand, Image URL, SKU URL.
- **Searchspring**: Use `https://00svms.a.searchspring.io/api/search/autocomplete.json?siteId=00svms&resultsPerPage=400&q=$jeans&q=$womens&page=n` to collect style metadata (Style Name, Product Type, Rise/Inseam/Leg Opening, GA purchases, Quantity of style, Jean/Hem/Inseam/Rise labels, price range) and inventory. `variant_id` strings map to Shopify variant ids; when counts are missing, parse the `variants` JSON and `ss_bundle_variants` block for `quantity` per SKU. Metafields can backfill missing inventory quantities.
- **Barcode**: `/products/<handle>.json` per variant.
- **Measurements**: style-level Rise/Inseam/Leg Opening come from Searchspring’s `mfield_product_details`, converting fractions to decimals.
- **Quantity Available**: Preference order is `ss_bundle_variants.quantity` → Searchspring `variants` inventory → cached metafield JSON → PDP fallback. The script merges these into a per-variant map before writing rows.
- **Edits made to fix repeated scraping failures**: Encountered repeated DNS failures for `www.motherdenim.com`. Enhanced retry logic now swaps between `www.motherdenim.com` and `motherdenim.com`, logging successful fallbacks.


### Ramy Brook (`ramybrook_pants_inventory.py`)
- **Catalog**: Shopify collection feed filtered to denim pants (see script for the `products.json?limit` pagination). Pull baseline fields plus published date conversion.
- **Inventory & metadata**: Parse PDP HTML for `window.BARREL.product`. It contains `variants[].inventoryQuantity`, `nextIncomingDate`, size/color option text, and measurement copy. A regex fallback scans script blobs for `"inventoryQuantity"` when BARREL is missing.
- **Measurements**: Look inside `body_html` / PDP detail sections for Front/Back Rise, Inseam, Leg Opening.
- **Barcode**: `/products/<handle>.json` keyed by variant id.
- **Style totals**: sum variant quantities once per product.
- **Logging**: Writes inside `Output/ramybrook_run.log`; consider aligning with the shared pattern on future refactors.

### Staud (`staud_inventory.py`)
- **Catalog**: Shopify women’s jeans collection feed (`collections/staud-jeans/products.json?limit=250&page=n`). Provides Style Id, Handle, Published At, Product, Style Name (derived from title before " |"), Product Type, Tags, Vendor, Description, Variant Title, Color, Size, Inseam, Price, Compare at Price, Available for Sale, SKUs, image links.
- **Variant enrichment**: `/products/<handle>.json` yields Quantity Price Breaks, Quantity Available (current and previous), style totals, barcodes, image URLs. Map by variant id to the collection feed entries.
- **Measurements**: Inseam, Rise labels are already present as variant options—no PDP scraping required.
- **Inventory handling**: Trust Shopify’s `inventory_quantity`/`old_inventory_quantity` per variant; sum totals once per product for the style-level quantity column.
- **Edits made to fix repeated scraping failures**: DNS lookups for `staud.clothing` were failing, causing complete run aborts. Request helper iterates between `staud.clothing` and `www.staud.clothing`, capturing and reporting fallback successes.

### AMO (`amo_inventory.py`)
- **Edits made to fix repeated scraping failures**: Previously failed when `amodenim.com` DNS lookups broke, and partial runs left product rows missing details. Now uses a shared requests session with fallback host handling, centralized retry logging, and writes CSV output plus a run log resolved from the script directory.
- **2 versions** As we kept getting errors during the scrape like "(HTTPSConnectionPool(host='amodenim.com', port=443): Max retries exceeded with url: /products/<handle> (Caused by NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x000002012714A210>: Failed to resolve 'amodenim.com' ([Errno 11001] getaddrinfo failed)"))); sleeping" We will create 2 codes: amo_inventory_with_measurements.py and amo_inventory.py. amo_inventory_with_measurements includes values for Rise, Back Rise, Inseam, and Leg Opening. amo_inventory.py does not pull values for Rise, Back Rise, Inseam, and Leg Opening but does keep the headers on the csv output for layout consistency. amo_inventory.py is run every day with the outputfile name of `AMO_YYYY-MM-DD_HH-MM-SS.csv` while amo_inventory_with_measurements.py is run monthly with a file output name of `AMO_Measurements_YYYY-MM-DD_HH-MM-SS.csv`


### Frame (`frame_inventory.py` / `frame_inventory_with_measurements.py`)
- **Catalog source**: Combine the women’s denim and sale collection feeds (`https://frame-store.com/collections/denim-women/products.json?limit=250&page=n` and `https://frame-store.com/collections/sale-denim/products.json?limit=250&page=n`). Only keep `product_type == "Jeans"`, except when the tags include `collection::skirts & shorts`—in that case set Product Type to `Skirt/Short`.
- **Searchspring enrichment**: Query `https://v1j77y.a.searchspring.io/api/search/search.json?siteId=v1j77y&resultsFormat=json&q=women&ss_category=Jeans&resultsPerPage=100&page=n`. Join on Shopify variant id to ingest Quantity Available (`inventory_quantity`), style totals (`ss_inventory_count`), barcode, jean style tags (`filterleg::`), inseam/rise labels, stretch, and color families.
- **Measurements**: The daily scraper leaves Rise/Inseam/Leg Opening blank (headers remain). The monthly `_with_measurements` script fetches `https://frame-store.com/products/<handle>?modals=details_modal`, parses the hidden `measurement-image__list`, and writes the inch values as decimals.
- **Pricing**: Shopify already returns dollar strings. Do **not** divide by 100.
- **Logging**: `initialize_logging()` tries the base directory log and falls back to `OUTPUT_DIR/frame_run.log` if OneDrive locks the file.

### L’Agence (`lagence_inventory.py` / `lagence_inventory_with_measurements.py`)
- **Shopify collections**: Iterate over `collections/jeans` and `collections/sale` JSON feeds. Filter to `product_type == "jean"`.
- **Nosto GraphQL**: Query the category ids `626045911412` (denim) and `160218808423` (sale) and join on Shopify variant id. Nosto supplies Quantity Available, style totals, barcodes, jean style tags, inseam/rise labels, stretch, simplified colors, and measurements (`custom-detail_spec_*`).
- **Monthly measurements**: Same pipeline as Frame—cache PDP modal HTML when Nosto omits Rise/Inseam/Leg Opening.
- **Price normalization**: Keep Shopify `price`/`compare_at_price` verbatim.

### Rolla’s (`rollas_inventory.py` / `rollas_inventory_with_measurements.py`)
- **GraphQL filter**: `collection:women AND tag:'category:Jeans'`. Skip any product tagged `gender:Guys`.
- **Inseam + size**: Option3 stores centimetre inseams; convert to inches (two decimals). If missing, scrape the PDP measurement list (monthly script).
- **Jean style**: Use the helper in `rollas/style_rules.py` which maps `fit`, `fit_swatch`, and title keywords (e.g., `Heidi Low`) to canonical styles. Do not hand-roll mappings.
- **Inseam label**: Apply the Skinny/Baggy/Straight + inseam matrix via `determine_inseam_label()`.
- **Stretch**: Prefer tags (`stretch:Rigid`, `stretch:Super`); fall back to description keywords.

### Abrand (`abrand_inventory.py` / `abrand_inventory_with_measurements.py`)
- **GraphQL scope**: Use the Storefront API with `tag:'gender:Girls'`. Men’s SKUs must never appear.
- **Measurement parsing**: Convert option3 centimetres to inches. The monthly script scrapes the PDP `Details` accordion and supports both `cm` and `in` strings.
- **Jean style**: Map `fit`, `fit_swatch`, and title keywords to Straight/Flare/Boot/Barrel/Baggy exactly as described in the prompt matrix (Heidi Low counts as Straight).
- **Stretch**: Normalize to exactly `Rigid`, `comfort-stretch`, or `stretch` using tags first, then description keywords.

### DL1961 (`dl1961_inventory.py` / `dl1961_inventory_with_measurements.py` / `dl1961_source_snapshot.py`)
- **Searchspring authoritative list**: Query `https://8176gy.a.searchspring.io/...` (legacy) or `https://dkc5xr.a.searchspring.io/...` (Warp + Weft). The Searchspring handles define the SKU list, `ga_unique_purchases`, promos, and in-stock percentages. If a handle is not present, skip it entirely.
- **Storefront GraphQL**: Enumerate all products and apply the prompt’s exclusion rules afterward (`product_type_unigram` filters, bad tags like `fabriccrochet`, titles containing `short`, etc.).
- **Measurements**: Parse `Rise`, `Inseam`, and `Leg Opening` from the description. When blank, scrape PDP HTML `pro-benefits` spans (monthly script). The helper accepts inch-only strings.
- **Pricing**: Shopify provides dollar strings; compare-at price comes from Searchspring `msrp`.
- **Analytics**: Convert `ga_unique_purchases` strings to integers before writing the CSV.

### Icon Denim (`icon_inventory.py`)
- **Collection feeds**: Pull women’s bottoms and last-chance jeans with `currency=USD`. Filter for titles containing `Jean`.
- **Variant options**: If `option2` is `null`, leave Color blank and treat `option1` as Size. Otherwise `option1` = Color, `option2` = Size.
- **GraphQL enrichment**: Fetch descriptions, SKUs, barcodes, inventory counts, and PDP URLs from `https://icondenimlosangeles.com/api/2025-04/graphql.json` using the provided token.
- **Stretch**: Scan the description for `Rigid` and populate the column when present.

### Neuw (`neuw_inventory.py` / `neuw_inventory_with_measurements.py`)
- **GraphQL filter**: `collection:womens-jeans` with `tag:'gender:Girls'`.
- **Size & inseam**: Extract from SKU strings (`A43J96-3130-MID BLUE-23/30`). Split on `-` and `/` but preserve trailing zeros by keeping everything as strings.
- **Inseam label**: Follow the jean-style/inseam matrix (e.g., Skinny + 32 → Long, Baggy + 30 → Petite). The helper in `neuw/labels.py` already encodes the rules.
- **Stretch**: Tags expose `stretch:` tokens; fallback to description keywords `comfort-stretch`, `stretch`, or `Rigid`.
- **Monthly measurements**: Scrape PDP `Details` items when Storefront omits measurements. Support both centimetre and inch units.

### Rolla’s / Abrand / Neuw / DL1961 / Icon monthly measurement scripts
- All `_with_measurements.py` scripts reuse the daily data pipeline and augment it with PDP HTML fallbacks. Cache PDP responses by handle and always return measurement strings with two decimal places.

### Warp + Weft (`warpweft_inventory.py`)
- **GraphQL scope**: Request all product types `"Women's Jeans"`, `"Women's Plus Size Jeans"`, and `"Women's Regular Size Jeans"`. Immediately drop any title containing `dress`, `short`, `skirt`, `jacket`, `shirt`, `vest`, or `tee`.
- **Measurements**: Extract `Rise`, `Inseam`, and `Leg Opening` from the description using the regex helpers that mirror the Excel formulas provided in the prompt. Validate that the inseam falls within the allowed list; raise if it does not so bugs are caught early.
- **Searchspring merge**: Join on Shopify product id (`ss_id`). Use Searchspring for `msrp` (Compare at Price), promo badges, in-stock percentage (append `%`), and primary image URLs.
- **Labels**: Tags include `fit:Bootcut`, `Length:long`, `Rise:high`, etc. Parse them with the shared helper in `warpweft/attribute_maps.py`. When multiple tags qualify, prefer the one whose keyword appears in the product title.
- **Stretch**: Tags contain `stretch:high`, `stretch:low`, or `stretch:rigid`. Map directly to `High Stretch`, `Low Stretch`, or `Rigid`. If absent but description mentions `sculpting denim`, fill `Sculpting Denim`.
- **Promo badges**: Output the Searchspring strings exactly as provided—no trimming or reformatting.

## Maintenance checklist

- Always sanity-check the latest CSV to ensure all values are filled.
- If a network vendor blocks scripted access, capture raw JSON (e.g., from DevTools) so we can build offline fixtures.
- When adding a new brand, replicate the shared directory/log scaffolding and document the mapping in this file.
- If a site migrates or the API schema changes, update the relevant section here so future maintainers know which fallbacks exist and what was tried previously.
- **Data cleanliness**: Whenever a source provides fractions (e.g., `10 3/4`), use the shared helper to convert to decimal (10.75). Normalize price fields to plain numbers without currency symbols. Always compute style totals outside the variant loop and reuse the value for each row.

Keep this handbook synchronized with future scraper additions so new teammates can get up to speed quickly.


