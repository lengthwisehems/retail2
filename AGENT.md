# Inventory Scraping Playbook

This document captures the conventions and brand-specific notes for the denim inventory scrapers maintained in this repository. It is intended to let a new contributor ramp quickly, understand where each CSV field comes from, and avoid breaking the existing data paths.

## Shared conventions

- **Runtime environment**: All scrapers use Python 3.11+ with `requests` and, when needed, `beautifulsoup4`. We standardize on a single `requests.Session` per run with a desktop User-Agent and exponential backoff around transient HTTP errors (429/5xx).Log major milestones (collection page counts, Searchspring/Algolia pagination, fallbacks) in the command prompt while the script is running.
- **Output layout**: Every script defines `BASE_DIR = Path(__file__).resolve().parent`, `OUTPUT_DIR = BASE_DIR / "Output"`, and a brand-specific `LOG_PATH`. `OUTPUT_DIR` is created up front and all exports are timestamped `BRAND_YYYY-MM-DD_HH-MM-SS.csv` (24-hour clock). Logs append to `[brand]_run.log` in the same directory and gracefully falls back if the preferred path is unavailable.
- **Logging fallbacks**: When configuring logging handlers, always attempt to open the primary log file inside a try/except block and fall back to `OUTPUT_DIR / "[brand]_run.log"` (or stream-only logging) if the primary path is locked. Emit a warning so the automation log explains which destination is active. Apply this pattern to every new scraper.
For each brand Python script:
- Log the absolute output path after writing the CSV (using Path.resolve()).
- Standardize the message format: "CSV written: <full_path>".
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
Where Found (details): additional help in the event the information may be difficult to find, has specific rules for what should/shouldn’t be pulled, or requires additional formatting details. This may not always be filled. What is/isn’t filled and what instructions are given  will change from brand to brand so pay attention to the mapping in the prompt.

- **Logging**: Log major milestones (collection page counts, Searchspring/Algolia pagination, fallbacks) and any retries so production runs can be audited.
- **Retry policy**: Treat 429/5xx as transient, sleep with exponential backoff, log successful fallbacks, and rotate through host fallbacks by adding alternate domain (e.g., amodenim.com vs www.amodenim.com).
- **Do not regress working code**: When adding features, leave the validated inventory path untouched. New behavior should sit behind clearly documented flags or separate functions.
- **Pre-emptive fixes**: Refer to each brand's **Edits made to fix repeated scraping failures** to implement preventative fixes when writing new code
- **Timeout Errors**: If a brand keeps getting time out errors while trying to visit the HTTP site, create 2 files: brand_inventory_with_measurements.py and brand_inventory.py. brand_inventory_with_measurements includes values that can only come from the HTML like Rise, Inseam, and Leg Opening. brand_inventory.py does not pull values that can only come from the HTML like Rise, Inseam, and Leg Opening but does keep the headers on the csv output for layout consistency. brand_inventory.py is run every day with the outputfile name of `BRAND_YYYY-MM-DD_HH-MM-SS.csv` while brand_inventory_with_measurements.py is run monthly with a file output name of `BRAND_Measurements_YYYY-MM-DD_HH-MM-SS.csv`. First build out the brand_inventory_with_measurements.py. Then once given confirmation that the code is good, and have no more edits, then you can write brand_inventory.py and add it to run_all.bat


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

## Stipulations for "probe" code creation

### Example files: retail_data_probe.py and retail_data_probe_additional.py

### Excel/Workbook formatting
- Canonical column order is fixed across sheets: product.id, product.handle, product.published_at, product.created_at, product.title, product.productType, product.tags_all, product.vendor, product.description, product.descriptionHtml, variant.title, variant.option1, variant.option2, variant.option3, variant.price, variant.compare_at_price, variant.available, variant.quantityAvailable, product.totalInventory, variant.id, variant.sku, variant.barcode, product.images[0].src, product.onlineStoreUrl. Any columns not in this list are appended after product.onlineStoreUrl, with optional priority columns inserted ahead of the alphabetical extras.
- Every product/variant combination is a separate row in JSON, Apps, and Storefront tabs; style-level rows are only used when a product lacks variants.
- Only the first product image is retained; all other image fields (positions, height/width, IDs, variant_ids, etc.) are dropped. Variant featured_image is collapsed to featured_image.src when present.
- Option values are concatenated so option values land in a single cell (option1/option2/option3), and Shopify option position fields are removed.
- Name/value pairs are converted into columns titled with the name while the redundant “name” column is removed.
- In only the JSON tab, Tags are grouped by prefix (e.g., fit-, denim-, wash-) into tags_group_* columns, joined with commas, and the tag columns are sorted by how frequently they appear so the most common tag groups sit closest to the base columns. This should not be done for Graphql or any other Apps
- Put the output for each source (JSON, Graphql, Globo, Rebuyengine, Restock, Avada, bundler.app, postscript etc) in its own tab and make the tab label the name of the app

### GraphQL behavior
- X_SHOPIFY_STOREFRONT_ACCESS_TOKEN may be a single string or a list; tokens are normalized (trimmed, deduplicated, order preserved) before use.
- Provided tokens are probed first against all configured GraphQL endpoints via a lightweight shop query; only endpoints that return OK with a given token are used for full collection/product pulls. Probes and outcomes are recorded in the Storefront_access sheet. If fields: {'Product': ['totalInventory'], 'ProductVariant': ['quantityAvailable']} are made available with the given token, stop token discovery and reuse that token for data collection.
- After provided tokens, any tokens discovered in HTML/script content are probed and, if successful, reused for data collection. If no token works, the script attempts unauthenticated calls, then falls back to simplified collection/product queries that exclude restricted fields.
- When STOREFRONT_COLLECTION_HANDLES is set, collection-based pagination is used; otherwise a products query runs. Variant connections are normalized so nodes or edges are both accepted, and forbidden fields (selling plan allocations, groupedBy, components, etc.) are stripped to avoid scope errors.
- Successful Storefront pulls populate the Storefront tab with per-variant rows following the canonical column order. Access attempts (endpoint, token source, status, success flag, notes) are always written to the Storefront_access tab, even when only fallback/unauthenticated data is available.

### App-specific output guidance (Globo, Rebuy, Searchspring etc)
- Scope: These rules govern helper probes that capture third-party app payloads (e.g., Globo filter JSONP, Rebuy custom widgets, Searchspring search feeds etc) when those feeds are exported alongside Shopify data in the probe workbook.
- Column inclusion: Always start from the canonical product/variant columns above. Add app-specific fields only when they carry product- or variant-level values (inventory counts, source collection/placement, badge text, merchandising tags, title). Exclude transport-only metadata such as signatures, analytics beacons, widget titles, or pagination cursors unless they directly explain a data value. If any cache keys, tokens, or access codes are discovered that fit the regex of \b[0-9a-f]{32}\b add it to the list of tokens discovered in HTML/script and try it against all configured GraphQL endpoints via a lightweight shop query.
- Recursion/flattening: Walk nested dictionaries/arrays to capture all product/variant fields; serialize complex substructures only when the keys describe the SKU (e.g., custom size attributes). Drop wrapper layers that exist solely for transport (callbacks, response envelopes, pagination cursors).
- Relevance filter: Before adding a field, ask whether it describes the product/variant itself (identifier, merchandising attributes, inventory, pricing, URLs, tags, title). If it only explains how the app delivered the data (timing, experiment IDs, request context), omit it. Keep the schema portable so brand-specific, app-specific columns can be appended after the base order without breaking downstream formulas.
- Prefer stable payloads such as metadata.input_products over volatile recommendation lists.
- Capture per-variant blocks regardless of whether they arrive under edges, nodes, or custom variant arrays.

## Maintenance checklist

- Always sanity-check the latest CSV to ensure all values are filled.
- Always test the code before sharing
- If a network vendor blocks scripted access, capture raw JSON (e.g., from DevTools) so we can build offline fixtures.
- When adding a new brand, replicate the shared directory/log scaffolding and document the mapping in this file.
- If a site migrates or the API schema changes, update the relevant section here so future maintainers know which fallbacks exist and what was tried previously.
- **Data cleanliness**: Whenever a source provides fractions (e.g., `10 3/4`), use the shared helper to convert to decimal (10.75). Normalize price fields to plain numbers without currency symbols. Always compute style totals outside the variant loop and reuse the value for each row.

Keep this handbook synchronized with future scraper additions so new teammates can get up to speed quickly.


# Shopify Notes
---
## title: GraphQL Admin API reference
## description: >-
  ### The Admin API lets you build apps and integrations that extend and enhance the
  ### Shopify admin. Learn how to get started using efficient GraphQL queries.
## api_version: unstable
## api_name: admin
## source_url:
  ### html: 'https://shopify.dev/docs/api/admin-graphql/unstable'
  ### md: 'https://shopify.dev/docs/api/admin-graphql/unstable.md'
---

## GraphQL Admin API reference

The Admin API lets you build apps and integrations that extend and enhance the Shopify admin.

This page will help you get up and running with Shopify’s GraphQL API.

## Client libraries

Use Shopify’s officially supported libraries to build fast, reliable apps with the programming languages and frameworks you already know.

React Router

The official package for React Router applications.

* [Docs](https://shopify.dev/docs/api/shopify-app-react-router)
* [npm package](https://www.npmjs.com/package/@shopify/shopify-app-react-router)
* [GitHub repo](https://github.com/Shopify/shopify-app-js/tree/main/packages/apps/shopify-app-react-router#readme)

Node.js

The official client library for Node.js apps. No framework dependencies—works with any Node.js app.

* [Docs](https://github.com/Shopify/shopify-app-js/tree/main/packages/apps/shopify-api#readme)
* [npm package](https://www.npmjs.com/package/@shopify/shopify-api)
* [GitHub repo](https://github.com/Shopify/shopify-app-js/tree/main/packages/apps/shopify-api)

Ruby

The official client library for Ruby apps.

* [Docs](https://shopify.github.io/shopify-api-ruby/)

* [Ruby gem](https://rubygems.org/gems/shopify_api)

* [GitHub repo](https://github.com/Shopify/shopify-api-ruby)

cURL

Use the [curl utility](https://curl.se/) to make API queries directly from the command line.

Other

Need a different language? Check the list of [community-supported libraries](https://shopify.dev/apps/tools/api-libraries#third-party-admin-api-libraries).

##### React Router

```bash
npm install -g @shopify/cli@latest
shopify app init
```

##### Node.js

```ts
npm install --save @shopify/shopify-api
# or
yarn add @shopify/shopify-api
```

##### Ruby

```ruby
bundle add shopify_api
```

##### cURL

```bash
# cURL is often available by default on macOS and Linux.
# See http://curl.se/docs/install.html for more details.
```

***

## Authentication

All GraphQL Admin API requests require a valid Shopify access token. If you use Shopify’s [client libraries](https://shopify.dev/apps/tools/api-libraries), then this will be done for you. Otherwise, you should include your token as a `X-Shopify-Access-Token` header on all GraphQL requests.

Public and custom apps created in the Dev Dashboard generate tokens using [OAuth](https://shopify.dev/apps/auth/oauth), and custom apps made in the Shopify admin are [authenticated in the Shopify admin](https://shopify.dev/apps/auth/admin-app-access-tokens).

To keep the platform secure, apps need to request specific [access scopes](https://shopify.dev/api/usage/access-scopes) during the install process. Only request as much data access as your app needs to work.

Learn more about [getting started with authentication](https://shopify.dev/apps/auth) and [building apps](https://shopify.dev/apps/getting-started).

##### React Router

```js
import { authenticate } from "../shopify.server";

export async function loader({request}) {
  const { admin } = await authenticate.admin(request);
  const response = await admin.graphql(
    `query { shop { name } }`,
  );
}
```

##### Node.js

```ts
const client = new shopify.clients.Graphql({session});
const response = await client.query({data: 'query { shop { name } }'});
```

##### Ruby

```ruby
session = ShopifyAPI::Auth::Session.new(
  shop: 'your-development-store.myshopify.com',
  access_token: access_token,
)
client = ShopifyAPI::Clients::Graphql::Admin.new(
  session: session,
)
response = client.query(query: 'query { shop { name } }')
```

##### cURL

```bash
# Replace {SHOPIFY_ACCESS_TOKEN} with your actual access token
  curl -X POST \
  https://{shop}.myshopify.com/admin/api/unstable/graphql.json \
  -H 'Content-Type: application/json' \
  -H 'X-Shopify-Access-Token: {SHOPIFY_ACCESS_TOKEN}' \
  -d '{
  "query": "query { shop { name } }"
  }'
```

***

## Endpoints and queries

GraphQL queries are executed by sending `POST` HTTP requests to the endpoint:

`https://{store_name}.myshopify.com/admin/api/unstable/graphql.json`

Queries begin with one of the objects listed under [QueryRoot](https://shopify.dev/api/admin-graphql/unstable/objects/queryroot). The QueryRoot is the schema’s entry-point for queries.

Queries are equivalent to making a `GET` request in REST. The example shown is a query to get the ID and title of the first three products.

Learn more about [API usage](https://shopify.dev/api/usage).

***

Note

Explore and learn Shopify's Admin API using [GraphiQL Explorer](https://shopify.dev/apps/tools/graphiql-admin-api). To build queries and mutations with shop data, install [Shopify’s GraphiQL app](https://shopify-graphiql-app.shopifycloud.com/).

POST

## https://{store\_name}.myshopify.com/admin/api/unstable/graphql.json

##### React Router

```ts
import { authenticate } from "../shopify.server";

export async function loader({request}) {
  const { admin } = await authenticate.admin(request);
  const response = await admin.graphql(
    `#graphql
    query getProducts {
      products (first: 3) {
        edges {
          node {
            id
            title
          }
        }
      }
    }`,
  );
  const json = await response.json();
  return { products: json?.data?.products?.edges };
}
```

##### Node.js

```ts
const queryString = `{
  products (first: 3) {
    edges {
      node {
        id
        title
      }
    }
  }
}`

// `session` is built as part of the OAuth process
const client = new shopify.clients.Graphql({session});
const products = await client.query({
  data: queryString,
});
```

##### Ruby

```ruby
query = <<~GQL
  {
    products (first: 3) {
      edges {
        node {
          id
          title
        }
      }
    }
  }
GQL

# session is built as part of the OAuth process
client = ShopifyAPI::Clients::Graphql::Admin.new(
  session: session
)
products = client.query(
  query: query,
)
```

##### cURL

```bash
# Get the ID and title of the three most recently added products
curl -X POST   https://{store_name}.myshopify.com/admin/api/unstable/graphql.json \
  -H 'Content-Type: application/json' \
  -H 'X-Shopify-Access-Token: {access_token}' \
  -d '{
  "query": "{
    products(first: 3) {
      edges {
        node {
          id
          title
        }
      }
    }
  }"
}'
```

***

## Rate limits

The GraphQL Admin API is rate-limited using calculated query costs, measured in cost points. Each field returned by a query costs a set number of points. The total cost of a query is the maximum of possible fields selected, so more complex queries cost more to run.

Learn more about [rate limits](https://shopify.dev/api/usage/limits#graphql-admin-api-rate-limits).

{}

## Request

```graphql
{
  products(first: 1) {
    edges {
      node {
        title
      }
    }
  }
}
```

{}

## Response

```json
{
  "data": {
    "products": {
      "edges": [
        {
          "node": {
            "title": "Hiking backpack"
          }
        }
      ]
    }
  },
  "extensions": {
    "cost": {
      "requestedQueryCost": 3,
      "actualQueryCost": 3,
      "throttleStatus": {
        "maximumAvailable": 1000.0,
        "currentlyAvailable": 997,
        "restoreRate": 50.0
      }
    }
  }
}
```

***

## Status and error codes

All API queries return HTTP status codes that contain more information about the response.

### 200 OK

GraphQL HTTP status codes are different from REST API status codes. Most importantly, the GraphQL API can return a `200 OK` response code in cases that would typically produce 4xx or 5xx errors in REST.

### Error handling

The response for the errors object contains additional detail to help you debug your operation.

The response for mutations contains additional detail to help debug your query. To access this, you must request `userErrors`.

#### Properties

* errorsarray

A list of all errors returned

* errors\[n].​messagestring

Contains details about the error(s).

* errors\[n].​extensionsobject

Provides more information about the error(s) including properties and metadata.

* errors\[n].​extensions.​codestring

Shows error codes common to Shopify. Additional error codes may also be shown.

* THROTTLED

The client has exceeded the [rate limit](#rate-limits). Similar to 429 Too Many Requests.

* ACCESS\_​DENIED

The client doesn’t have correct [authentication](#authentication) credentials. Similar to 401 Unauthorized.

* SHOP\_​INACTIVE

The shop is not active. This can happen when stores repeatedly exceed API rate limits or due to fraud risk.

* INTERNAL\_​SERVER\_​ERROR

Shopify experienced an internal error while processing the request. This error is returned instead of 500 Internal Server Error in most circumstances.

***

### 4xx and 5xx status codes

The 4xx and 5xx errors occur infrequently. They are often related to network communications, your account, or an issue with Shopify’s services.

Many errors that would typically return a 4xx or 5xx status code, return an HTTP 200 errors response instead. Refer to the [200 OK section](#200-ok) above for details.

{}

## Sample 200 error responses

##### Throttled

```json
{
"errors": [
  {
    "message": "Query cost is 2003, which exceeds the single query max cost limit (1000).

See https://shopify.dev/concepts/about-apis/rate-limits for more information on how the
cost of a query is calculated.

To query larger amounts of data with fewer limits, bulk operations should be used instead.
See https://shopify.dev/tutorials/perform-bulk-operations-with-admin-api for usage details.
",
    "extensions": {
      "code": "MAX_COST_EXCEEDED",
      "cost": 2003,
      "maxCost": 1000,
      "documentation": "https://shopify.dev/api/usage/limits#rate-limits"
    }
  }
]
}
```

##### Internal

```json
{
"errors": [
  {
    "message": "Internal error. Looks like something went wrong on our end.
Request ID: 1b355a21-7117-44c5-8d8b-8948082f40a8 (include this in support requests).",
    "extensions": {
      "code": "INTERNAL_SERVER_ERROR",
      "requestId": "1b355a21-7117-44c5-8d8b-8948082f40a8"
    }
  }
]
}
```

### 4xx and 5xx status codes

The 4xx and 5xx errors occur infrequently. They are often related to network communications, your account, or an issue with Shopify’s services.

Many errors that would typically return a 4xx or 5xx status code, return an HTTP 200 errors response instead. Refer to the [200 OK section](#200-ok) above for details.

***

#### 400 Bad Request

The server will not process the request.

***

#### 402 Payment Required

The shop is frozen. The shop owner will need to pay the outstanding balance to [unfreeze](https://help.shopify.com/en/manual/your-account/pause-close-store#unfreeze-your-shopify-store) the shop.

***

#### 403 Forbidden

The shop is forbidden. Returned if the store has been marked as fraudulent.

***

#### 404 Not Found

The resource isn’t available. This is often caused by querying for something that’s been deleted.

***

#### 423 Locked

The shop isn’t available. This can happen when stores repeatedly exceed API rate limits or due to fraud risk.

***

#### 5xx Errors

An internal error occurred in Shopify. Check out the [Shopify status page](https://www.shopifystatus.com) for more information.

***

Info

Didn’t find the status code you’re looking for? View the complete list of [API status response and error codes](https://shopify.dev/api/usage/response-codes).

{}

## Sample error codes

##### 400

```
HTTP/1.1 400 Bad Request
{
  "errors": {
    "query": "Required parameter missing or invalid"
  }
}
```

##### 402

```
HTTP/1.1 402 Payment Required
{
  "errors": "This shop's plan does not have access to this feature"
}
```

##### 403

```
HTTP/1.1 403 Access Denied
{
  "errors": "User does not have access"
}
```

##### 404

```
HTTP/1.1 404 Not Found
{
  "errors": "Not Found"
}
```

##### 423

```
HTTP/1.1 423 Locked
{
  "errors": "This shop is unavailable"
}
```

##### 500

```
HTTP/1.1 500 Internal Server Error
{
  "errors": "An unexpected error occurred"
}
```

***
---
title: About client secrets
description: Learn how to acquire and use a client secret to authenticate your app.
source_url:
  html: >-
    https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets
  md: >-
    https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets.md
---

ExpandOn this page

* [Retrieve your app's client credentials](https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets.md#retrieve-your-apps-client-credentials)
* [Rotate or revoke your app's client credentials](https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets.md#rotate-or-revoke-your-apps-client-credentials)

# About client secrets

Your app's client secret is a unique key that authenticates your app when it requests access to a store's data. You can use your client secret to retrieve an access token for a store, or to verify a webhook request is genuine.

***

## Retrieve your app's client credentials

You can retrieve your app's client credentials in the [Dev Dashboard](https://shopify.dev/docs/apps/build/dev-dashboard).

1. Open the [Dev Dashboard](https://shopify.dev/docs/apps/build/dev-dashboard).
2. Click **Apps**.
3. Select your app.
4. Click **Settings**.
5. View or copy your Client ID and Secret

***

## Rotate or revoke your app's client credentials

You should rotate the client credentials for your app on a regular basis. To learn how to rotate your app's client secret, refer to [Rotate or revoke client credentials](https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets/rotate-revoke-client-credentials).

***

* [Retrieve your app's client credentials](https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets.md#retrieve-your-apps-client-credentials)
* [Rotate or revoke your app's client credentials](https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets.md#rotate-or-revoke-your-apps-client-credentials)

---
title: Storefront API reference
description: >-
  The Storefront API provides commerce primitives to build custom, scalable, and
  performant shopping experiences. Learn how to get started using efficient
  GraphQL queries.
api_version: unstable
api_name: storefront
source_url:
  html: 'https://shopify.dev/docs/api/storefront/unstable'
  md: 'https://shopify.dev/docs/api/storefront/unstable.md'
---

# GraphQL Storefront API

Create unique customer experiences with the Storefront API on any platform, including the web, apps, and games. The API offers a full range of commerce options making it possible for customers to view [products](https://shopify.dev/custom-storefronts/products-collections/getting-started) and [collections](https://shopify.dev/custom-storefronts/products-collections/filter-products), add products to a [cart](https://shopify.dev/custom-storefronts/cart/manage), and [check out](https://shopify.dev/custom-storefronts/checkout).

Explore [Hydrogen](https://shopify.dev/custom-storefronts/hydrogen), Shopify’s official React-based framework for building headless commerce at global scale.

## Development frameworks and SDKs

Use Shopify’s officially supported libraries to build fast, reliable apps with the programming languages and frameworks you already know.

cURL

Use the [curl utility](https://curl.se/) to make API queries directly from the command line.

Hydrogen

A React-based framework for building custom storefronts on Shopify, Hydrogen has everything you need to build fast, and deliver personalized shopping experiences.

* [Docs](https://github.com/Shopify/hydrogen#readme)
* [npm package](https://www.npmjs.com/package/@shopify/hydrogen)
* [GitHub repo](https://github.com/Shopify/hydrogen)

Storefront API Client

The official lightweight client for any Javascript project interfacing with Storefront API and our recommended client for building custom storefronts without Hydrogen.

* [Docs](https://github.com/Shopify/shopify-app-js/tree/main/packages/api-clients/storefront-api-client#readme)
* [npm package](https://www.npmjs.com/package/@shopify/storefront-api-client)
* [GitHub repo](https://github.com/Shopify/shopify-app-js/tree/main/packages/api-clients/storefront-api-client)

React Router Apps

The official package for React Router apps.

* [Docs](https://shopify.dev/docs/api/shopify-app-react-router)
* [npm package](https://www.npmjs.com/package/@shopify/shopify-app-react-router)
* [GitHub repo](https://github.com/Shopify/shopify-app-template-react-router#readme)

Node.js

The official client library for Node.js applications, with full TypeScript support. It has no framework dependencies, so it can be used by any Node.js app.

* [Docs](https://github.com/Shopify/shopify-app-js/tree/main/packages/apps/shopify-api#readme)
* [npm package](https://www.npmjs.com/package/@shopify/shopify-api)
* [GitHub repo](https://github.com/Shopify/shopify-app-js/tree/main/packages/apps/shopify-api)

Shopify API (Apps)

The full suite library for TypeScript/JavaScript Shopify apps to access the GraphQL and REST Admin APIs and the Storefront API.

* [npm package](https://www.npmjs.com/package/@shopify/shopify-api)
* [GitHub repo](https://github.com/Shopify/shopify-app-js/tree/main/packages/apps/shopify-api)

Ruby

The official client library for Ruby applications. It has no framework dependencies, so it can be used by any Ruby app. This API applies a rate limit based on the IP address making the request, which will be your server’s address for all requests made by the library. Learn more about [rate limits](https://shopify.dev/api/usage/limits#rate-limits).

* [Docs](https://shopify.github.io/shopify-api-ruby/)
* [Ruby gem](https://rubygems.org/gems/shopify_api)
* [GitHub repo](https://github.com/Shopify/shopify-api-ruby)

Android

The official client library for Android apps.

* [Docs](https://github.com/Shopify/mobile-buy-sdk-android#readme)
* [GitHub repo](https://github.com/Shopify/mobile-buy-sdk-android)

iOS

The official client library for iOS applications.

* [Docs](https://github.com/Shopify/mobile-buy-sdk-ios#readme)
* [GitHub repo](https://github.com/Shopify/mobile-buy-sdk-ios)

Other

Other libraries are available in addition to the ones listed here. Check the list of [developer tools for custom storefronts](https://shopify.dev/custom-storefronts/tools).

##### Shopify Hydrogen storefront creation

```js
npm init @shopify/hydrogen
// or
npx @shopify/create-hydrogen
// or
pnpm create @shopify/create-hydrogen
// or
yarn create @shopify/hydrogen
```

##### Storefront API client installation

```ts
npm install --save @shopify/storefront-api-client
// or
yarn add @shopify/storefront-api-client
```

##### Shopify app React Router package installation

```ts
npm install --save @shopify/shopify-app-react-router
// or
yarn add @shopify/shopify-app-react-router
```

##### Shopify API installation

```js
npm install --save @shopify/shopify-api
// or
yarn add @shopify/shopify-api
```

##### Shopify Ruby library installation

```ruby
bundle add shopify_api
```

***

## Authentication

The Storefront API supports both tokenless access and token-based authentication.

### Tokenless access

Tokenless access allows API queries without an access token providing access to essential features such as:

* Products and Collections
* Selling Plans
* Search
* Pages, Blogs, and Articles
* Cart (read/write)

Tokenless access has a query complexity limit of 1,000. Query complexity is calculated based on the cost of each field in the query. For more information, see the [Cost calculation](#rate-limits) section.

### Token-based authentication

For access to all Storefront API features, an access token is required. The following features require token-based authentication:

* Product Tags
* Metaobjects and Metafields
* Menu (Online Store navigation)
* Customers

The Storefront API has the following types of token-based access:

* **Public access**: Used to query the API from a browser or mobile app.
* **Private access**: Used to query the API from a server or other private context, like a Hydrogen backend.

Learn more about [access tokens for the Storefront API](https://shopify.dev/api/usage/authentication#access-tokens-for-the-storefront-api).

##### Tokenless (cURL)

```bash
curl -X POST \
  https://{shop}.myshopify.com/api/unstable/graphql.json \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "{
      products(first: 3) {
        edges {
          node {
            id
            title
          }
        }
      }
    }"
  }'
```

##### Token-based (cURL)

```bash
curl -X POST \
  https://{shop}.myshopify.com/api/unstable/graphql.json \
  -H 'Content-Type: application/json' \
  -H 'X-Shopify-Storefront-Access-Token: {storefront-access-token}' \
  -d '{
    "query": "{your_query}"
  }'
```

##### Hydrogen

```js
const storefront = createStorefrontClient({
publicStorefrontToken: env.PUBLIC_STOREFRONT_API_TOKEN,
storeDomain: `https://\${env.PUBLIC_STORE_DOMAIN}\`,
storefrontApiVersion: env.PUBLIC_STOREFRONT_API_VERSION || '2023-01',
});
```

##### Storefront API Client

```ts
import {createStorefrontApiClient} from '@shopify/storefront-api-client';

const client = createStorefrontApiClient({
  storeDomain: 'http://your-shop-name.myshopify.com',
  apiVersion: 'unstable',
  publicAccessToken: <your-storefront-public-access-token>,
});
```

##### React Router

```ts
import { authenticate } from "../shopify.server";

// Use private access token on requests that don't come from Shopify
const { storefront } = await unauthenticated.storefront(shop);
// OR
// Use private access token for app proxy requests
const { storefront } = await authenticate.public.appProxy(request);
```

##### Shopify API

```js
const adminApiClient = new shopify.clients.Rest({session});
const storefrontTokenResponse = await adminApiClient.post({
  path: 'storefront_access_tokens',
  type: DataType.JSON,
  data: {
    storefront_access_token: {
      title: 'This is my test access token',
    },
  },
});

const storefrontAccessToken =
  storefrontTokenResponse.body['storefront_access_token']['access_token'];
```

##### Ruby

```ruby
# Create a REST client from your offline session
client = ShopifyAPI::Clients::Rest::Admin.new(
  session: session
)

# Create a new access token
storefront_token_response = client.post(
  path: 'storefront_access_tokens',
  body: {
    storefront_access_token: {
      title: "This is my test access token",
    }
  }
)

storefront_access_token = storefront_token_response.body['storefront_access_token']['access_token']
```

***

## Endpoints and queries

The Storefront API is available only in GraphQL. There's no REST API for storefronts.

All Storefront API queries are made on a single GraphQL endpoint, which only accepts `POST` requests:

`https://{store_name}.myshopify.com/api/unstable/graphql.json`

### Versioning

The Storefront API is [versioned](https://shopify.dev/api/usage/versioning), with new releases four times a year. To keep your app stable, make sure that you specify a supported version in the URL.

### Graphi​QL explorer

Explore and learn Shopify's Storefront API using the [GraphiQL explorer](https://shopify.dev/custom-storefronts/tools/graphiql-storefront-api). To build queries and mutations with shop data, install [Shopify's GraphiQL app](https://shopify-graphiql-app.shopifycloud.com/).

### Usage limitations

* Shopify Plus [bot protection](https://help.shopify.com/en/manual/checkout-settings/bot-protection) is only available for the [Cart](https://shopify.dev/custom-storefronts/cart/manage) object. It isn't available for the [Checkout](https://shopify.dev/custom-storefronts/checkout) object.
* You can't use Storefront API to duplicate existing Shopify functionality—be sure to check the API terms of service before you start.

POST

## https://{store\_name}.myshopify.com/api/unstable/graphql.json

##### Tokenless request

```bash
# Get the ID and title of the three most recently added products
curl -X POST \
  https://{store_name}.myshopify.com/api/unstable/graphql.json \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "{
      products(first: 3) {
        edges {
          node {
            id
            title
          }
        }
      }
    }"
  }'
```

##### Token-based request

```bash
# Get the ID and title of the three most recently added products
curl -X POST \
  https://{store_name}.myshopify.com/api/unstable/graphql.json \
  -H 'Content-Type: application/json' \
  -H 'X-Shopify-Storefront-Access-Token: {storefront_access_token}' \
  -d '{
    "query": "{
      products(first: 3) {
        edges {
          node {
            id
            title
          }
        }
      }
    }"
  }'
```

##### Hydrogen

```javascript
import {json} from '@shopify/remix-oxygen';

export async function loader({context}) {
  const PRODUCTS_QUERY = `#graphql
    query products {
      products(first: 3) {
        edges {
          node {
            id
            title
          }
        }
      }
    }
  `;
  const {products} = await context.storefront.query(PRODUCTS_QUERY);
  return json({products});
}
```

##### Storefront API Client

```ts
const productQuery = `
  query ProductQuery($handle: String) {
    product(handle: $handle) {
      id
      title
      handle
    }
  }
`;

const {data, errors, extensions} = await client.request(productQuery, {
  variables: {
    handle: 'sample-product',
  },
});
```

##### React Router

```ts
const { storefront } = await unauthenticated.storefront(
  'your-development-store.myshopify.com'
);

const response = await storefront.graphql(
  `#graphql
  query products {
    products(first: 3) {
      edges {
        node {
          id
          title
        }
      }
    }
  }`,
);

const data = await response.json();
```

##### Shopify API

```js
// Load the access token as per instructions above
const storefrontAccessToken = '<my token>';
// Shop from which we're fetching data
const shop = 'my-shop.myshopify.com';

// StorefrontClient takes in the shop url and the Storefront Access Token for that shop.
const storefrontClient = new shopify.clients.Storefront({
  domain: shop,
  storefrontAccessToken,
});

// Use client.query and pass your query as \`data\`
const products = await storefrontClient.query({
  data: `{
    products (first: 3) {
      edges {
        node {
          id
          title
        }
      }
    }
  }`,
});
```

##### Ruby

```ruby
# Load the access token as per instructions above
store_front_access_token = '<my token>'
# Shop from which we're fetching data
shop = 'my-shop.myshopify.com'

# The Storefront client takes in the shop url and the Storefront Access Token for that shop.
storefront_client = ShopifyAPI::Clients::Graphql::Storefront.new(
  shop,
  storefront_access_token
)

# Call query and pass your query as `data`
my_query = <<~QUERY
  {
    products (first: 3) {
      edges {
        node {
          id
          title
        }
      }
    }
  }
QUERY
products = storefront_client.query(query: my_query)
```

***

## Directives

A directive provides a way for apps to describe additional options to the GraphQL executor. It lets GraphQL change the result of the query or mutation based on the additional information provided by the directive.

### Storefront Directives

@inContext (Country Code)

In the Storefront API, the `@inContext` directive takes an optional [country code argument](https://shopify.dev/api/storefront/unstable/enums/countrycode) and applies this to the query or mutation.

This example shows how to retrieve a list of available countries and their corresponding currencies for a shop that's located in France `@inContext(country: FR)`.

* [Examples for localized pricing](https://shopify.dev/api/examples/international-pricing)
* [GQL directives spec](https://graphql.org/learn/queries/#directives)

@inContext (Language)

In the Storefront API, beyond version 2022-04, the `@inContext` directive can contextualize any query to one of a shop's available languages with an optional [language code argument](https://shopify.dev/api/storefront/unstable/enums/LanguageCode).

This example shows how to return a product's `title`, `description`, and `options` translated into Spanish `@inContext(language: ES)`.

* [Examples for supporting multiple languages](https://shopify.dev/api/examples/multiple-languages)
* [GQL directives spec](https://graphql.org/learn/queries/#directives)

@inContext (Buyer Identity)

In the Storefront API, beyond version 2024-04, the `@inContext` directive can contextualize any query to a logged in buyer of a shop with an optional [buyer argument](https://shopify.dev/api/storefront/unstable/input-objects/BuyerInput).

This example shows how to return a product's price `amount` contextualized for a business customer buyer `@inContext(buyer: {customerAccessToken: 'token', companyLocationId: 'gid://shopify/CompanyLocation/1'})`.

* [Example for supporting a contextualized buyer identity](https://shopify.dev/custom-storefronts/headless/b2b#step-3-contextualize-storefront-api-requests)
* [GraphQL directives spec](https://graphql.org/learn/queries/#directives)

@inContext (Visitor Consent)

In the Storefront API, beyond version 2025-10, the `@inContext` directive can contextualize any query or mutation with visitor consent information using an optional `visitorConsent` argument.

This example shows how to create a cart with visitor consent preferences `@inContext(visitorConsent: {analytics: true, preferences: true, marketing: false, saleOfData: false})`.

The consent information is automatically encoded and included in the resulting [`checkoutUrl`](https://shopify.dev/docs/api/storefront/latest/objects/Cart#field-Cart.fields.checkoutUrl) to ensure privacy compliance throughout the checkout process. All consent fields are optional.

* [Examples for collecting and passing visitor consent with Checkout Kit](https://shopify.dev/docs/storefronts/mobile/checkout-kit/privacy-compliance)
* [GraphQL directives spec](https://graphql.org/learn/queries/#directives)

@defer

The `@defer` directive allows clients to prioritize part of a GraphQL query without having to make more requests to fetch the remaining information. It does this through streaming, where the first response contains the data that isn't deferred.

The directive accepts two optional arguments: `label` and `if`. The `label` is included in the fragment response if it's provided in the directive. When the `if` argument is `false`, the fragment isn't deferred.

This example shows how to return a product's `title` and `description` immediately, and then return the `descriptionHtml` and `options` after a short delay.

The `@defer` directive is available as a [developer preview](https://shopify.dev/docs/api/developer-previews#defer-directive-developer-preview) in `unstable`.

* [Examples for how to use `@defer`](https://shopify.dev/docs/custom-storefronts/building-with-the-storefront-api/defer)

## Operation

```graphql
query productDetails {
  productByHandle(handle: "baseball-t-shirt") {
    title
    description
    ... @defer(label: "Extra info") {
      descriptionHtml
      options {
        name
        values
      }
    }
  }
}
```

## Response

JSON

```json
--graphql
Content-Type: application/json
Content-Length: 158


{
  "data": {
    "productByHandle": {
      "title": "Baseball t-shirt",
      "description": "description":"3 strikes, you're... never out of style in this vintage-inspired tee."
    }
  },
  "hasNext": true
}


--graphql
Content-Type: application/json
Content-Length: 295


{
  "incremental": [{
    "path": ["productByHandle"],
    "label": "Extra info",
    "data": {
      "descriptionHtml": "<p>3 strikes, you're... never out of style in this vintage-inspired tee. </p>",
      "options": [
        {
          "name": "Size",
          "values": ["Small", "Medium", "Large"]
        },
        {
          "name": "Color",
          "values": ["White", "Red"]
        }
      ]
    }
  }],
  "hasNext": false
}


--graphql--
```

***

## Rate limits

The Storefront API is designed to support businesses of all sizes. The Storefront API will scale to support surges in buyer traffic or your largest flash sale. There are no rate limits applied on the number of requests that can be made to the API.

The Storefront API provides protection against malicious users, such as bots, from consuming a high level of capacity. If a request appears to be malicious, Shopify will respond with a `430 Shopify Security Rejection` [error code](https://shopify.dev/docs/api/usage/response-codes) to indicate potential security concerns. Ensure requests to the Storefront API include the correct [Buyer IP header](https://shopify.dev/docs/api/usage/authentication#optional-ip-header).

[Learn more about rate limits](https://shopify.dev/docs/api/usage/limits#rate-limits).

### Query complexity limit for tokenless access

Tokenless access has a query complexity limit of 1,000. This limit is calculated based on the cost of each field in the query in the same way as the GraphQL Admin API. For more information on how query costs are calculated, see the [Cost calculation](https://shopify.dev/docs/api/usage/limits#rate-limits#cost-calculation) section in the API rate limits documentation.

When using tokenless access, query complexity that exceeds 1,000 will result in an error.

{}

## Query complexity exceeded error response

```json
{
  "errors": [
    {
      "message": "Complexity exceeded",
      "extensions": {
        "code": "MAX_COMPLEXITY_EXCEEDED",
        "cost": 1250,
        "maxCost": 1000
      }
    }
  ]
}
```

{}

## Response

```json
{
  "errors": [
    {
      "message": "Internal error. Looks like something went wrong on our end.
        Request ID: 1b355a21-7117-44c5-8d8b-8948082f40a8 (include this in support requests).",
      "extensions": {
        "code": "INTERNAL_SERVER_ERROR"
      }
    }
  ]
}
```

***

## Status and error codes

All API queries return HTTP status codes that contain more information about the response.

### 200 OK

The Storefront API can return a `200 OK` response code in cases that would typically produce 4xx errors in REST.

### Error handling

The response for the errors object contains additional detail to help you debug your operation.

The response for mutations contains additional detail to help debug your query. To access this, you must request `userErrors`.

#### Properties

* errorsarray

A list of all errors returned

* errors\[n].​messagestring

Contains details about the error(s).

* errors\[n].​extensionsobject

Provides more information about the error(s) including properties and metadata.

* extensions.​codestring

Shows error codes common to Shopify. Additional error codes may also be shown.

* ACCESS\_​DENIED

The client doesn’t have correct [authentication](#authentication) credentials. Similar to 401 Unauthorized.

* SHOP\_​INACTIVE

The shop is not active. This can happen when stores repeatedly exceed API rate limits or due to fraud risk.

* INTERNAL\_​SERVER\_​ERROR

Shopify experienced an internal error while processing the request. This error is returned instead of 500 Internal Server Error in most circumstances.

***

{}

## Sample 200 error responses

##### Throttled

```json
{
  "errors": [
    {
      "message": "Throttled",
      "extensions": {
        "code": "THROTTLED"
      }
    }
  ]
}
```

##### Internal

```json
{
  "errors": [
    {
      "message": "Internal error. Looks like something went wrong on our end.
        Request ID: 1b355a21-7117-44c5-8d8b-8948082f40a8 (include this in support requests).",
      "extensions": {
        "code": "INTERNAL_SERVER_ERROR"
      }
    }
  ]
}
```

### 4xx and 5xx status codes

The 4xx and 5xx errors occur infrequently. They are often related to network communications, your account, or an issue with Shopify’s services.

Many errors that would typically return a 4xx or 5xx status code, return an HTTP 200 errors response instead. Refer to the [200 OK section](#200-ok) above for details.

***

#### 400 Bad Request

The server will not process the request.

***

#### 402 Payment Required

The shop is frozen. The shop owner will need to pay the outstanding balance to [unfreeze](https://help.shopify.com/en/manual/your-account/pause-close-store#unfreeze-your-shopify-store) the shop.

***

#### 403 Forbidden

The shop is forbidden. Returned if the store has been marked as fraudulent.

***

#### 404 Not Found

The resource isn’t available. This is often caused by querying for something that’s been deleted.

***

#### 423 Locked

The shop isn’t available. This can happen when stores repeatedly exceed API rate limits or due to fraud risk.

***

#### 5xx Errors

An internal error occurred in Shopify. Check out the [Shopify status page](https://www.shopifystatus.com) for more information.

***

Info

Didn’t find the status code you’re looking for? View the complete list of [API status response and error codes](https://shopify.dev/api/usage/response-codes).

{}

## Sample error codes

##### 400

```
HTTP/1.1 400 Bad Request
{
  "errors": {
    "query": "Required parameter missing or invalid"
  }
}
```

##### 402

```
HTTP/1.1 402 Payment Required
{
  "errors": "This shop's plan does not have access to this feature"
}
```

##### 403

```
HTTP/1.1 403 Forbidden
{
  "errors": "Unavailable Shop"
}
```

##### 404

```
HTTP/1.1 404 Not Found
{
  "errors": "Not Found"
}
```

##### 423

```
HTTP/1.1 423 Locked
{
  "errors": "This shop is unavailable"
}
```

##### 500

```
HTTP/1.1 500 Internal Server Error
{
  "errors": "An unexpected error occurred"
}
```

***

## Resources

[Get started\
\
](https://shopify.dev/docs/storefronts/headless/building-with-the-storefront-api)

[Learn more about how the Storefront API works and how to get started with it.](https://shopify.dev/docs/storefronts/headless/building-with-the-storefront-api)

[Storefront Learning Kit\
\
](https://github.com/Shopify/storefront-api-learning-kit)

[Explore a downloadable package of sample GraphQL queries for the Storefront API.](https://github.com/Shopify/storefront-api-learning-kit)

[Developer changelog\
\
](https://shopify.dev/changelog)

[Read about the changes currently introduced in the latest version of the Storefront API.](https://shopify.dev/changelog)

***
---
title: Shopify API access scopes
description: >-
  All apps need to request access to specific store data during the app
  authorization process. This is a list of available access scopes for the
  Shopify admin and Storefront APIs.
api_name: usage
source_url:
  html: 'https://shopify.dev/docs/api/usage/access-scopes'
  md: 'https://shopify.dev/docs/api/usage/access-scopes.md'
---

ExpandOn this page

* [How it works](https://shopify.dev/docs/api/usage/access-scopes.md#how-it-works)
* [Authenticated access scopes](https://shopify.dev/docs/api/usage/access-scopes.md#authenticated-access-scopes)
* [Unauthenticated access scopes](https://shopify.dev/docs/api/usage/access-scopes.md#unauthenticated-access-scopes)
* [Customer access scopes](https://shopify.dev/docs/api/usage/access-scopes.md#customer-access-scopes)
* [Checking granted access scopes](https://shopify.dev/docs/api/usage/access-scopes.md#checking-granted-access-scopes)
* [Limitations](https://shopify.dev/docs/api/usage/access-scopes.md#limitations)

# Shopify API access scopes

All apps need to request access to specific store data during the app authorization process. This guide provides a list of available access scopes for the GraphQL Admin, Storefront, Payment Apps APIs, and Customer Account APIs.

***

## How it works

Tip

For more information on how to configure your access scopes, refer to [app configuration](https://shopify.dev/docs/apps/build/cli-for-apps/app-configuration) and [manage access scopes](https://shopify.dev/docs/apps/build/authentication-authorization/app-installation/manage-access-scopes).

After you've [generated API credentials](https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets), your app needs to [be authorized to access store data](https://shopify.dev/docs/apps/build/authentication-authorization#authorization).

Authorization is the process of giving permissions to apps. Users can authorize Shopify apps to access data in a store. For example, an app might be authorized to access orders and product data in a store.

An app can request authenticated or unauthenticated access scopes.

| Scope type | Description | Example use cases |
| - | - | - |
| [Authenticated](#authenticated-access-scopes) | Controls access to resources in the [GraphQL Admin API](https://shopify.dev/docs/api/admin-graphql), [Web Pixel API](https://shopify.dev/docs/api/web-pixels-api), and [Payments Apps API](https://shopify.dev/docs/api/payments-apps). Authenticated access is intended for interacting with a store on behalf of a user. | * Creating products
* Managing discount codes |
| [Unauthenticated](#unauthenticated-access-scopes) | Controls an app's access to [Storefront API](https://shopify.dev/docs/api/storefront) objects. Unauthenticated access is intended for interacting with a store on behalf of a customer. | - Viewing products
- Initiating a checkout |
| [Customer](#customer-access-scopes) | Controls an app's access to [Customer Account API](https://shopify.dev/docs/api/customer) objects. Customer access is intended for interacting with data that belongs to a customer. | * Viewing orders
* Updating customer details |

***

## Authenticated access scopes

This section describes the authenticated access scopes that your app can request. In the table, access to some resources are marked with **permissions required**. In these cases, you must [request specific permission](#requesting-specific-permissions) to access data from the user in your Partner Dashboard.

Info

To authenticate an admin-created custom app, you or the app user needs to install the app from the Shopify admin to generate API credentials and the necessary API access tokens. Refer to [access scopes for admin-created custom apps](https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/generate-app-access-tokens-admin#permissions-required-to-assign-scopes-to-a-custom-app).

| Scope | Access |
| - | - |
| `read_all_orders` | All relevant [orders](https://shopify.dev/docs/api/admin-graphql/latest/objects/Order) rather than the default window of orders created within the last 60 dayspermissions requiredThis access scope is used in conjunction with existing order scopes, for example `read_orders` or `write_orders`.You need to [request permission for this access scope](#orders-permissions) from your Partner Dashboard before adding it to your app. |
| `write_app_proxy` | Allows your app to use [app proxies](https://shopify.dev/docs/apps/build/online-store/display-dynamic-data). |
| `read_assigned_fulfillment_orders`,`write_assigned_fulfillment_orders`,`read_merchant_managed_fulfillment_orders`,`write_merchant_managed_fulfillment_orders`,`read_third_party_fulfillment_orders`,`write_third_party_fulfillment_orders`,`read_marketplace_fulfillment_orders` | `FulfillmentOrder`As of API version 2024-10, `write_third_party_fulfillment_orders` will no longer allow [order management apps](https://shopify.dev/docs/apps/build/orders-fulfillment/order-management-apps) to create fulfillments for fulfillment orders that have been assigned to a different fulfillment service app. |
| `read_cart_transforms`,`write_cart_transforms` | `CartTransform` |
| `read_checkout_branding_settings`,`write_checkout_branding_settings` | `CheckoutBranding` |
| `read_content`,`write_content`,`read_online_store_pages` | `Article`, `Blog`, `Comment`, `Page` |
| `read_customer_events`,`write_pixels` | [Web Pixels API](https://shopify.dev/docs/api/web-pixels-api) |
| `read_customer_merge`,`write_customer_merge` | `CustomerMergePreview`, `CustomerMergeRequest` |
| `read_customer_payment_methods` | `CustomerPaymentMethod`permissions requiredYou need to [request permission for this access scope](#subscription-apis-permissions) from your Partner Dashboard before adding it to your app. |
| `read_customers`,`write_customers` | `Customer`, `Segment`, `Company`, `CompanyLocation` |
| `read_delivery_customizations`,`write_delivery_customizations` | `DeliveryCustomization` |
| `read_discounts`,`write_discounts` | [Discounts features](https://shopify.dev/docs/apps/build/discounts) |
| `read_draft_orders`,`write_draft_orders` | `DraftOrder` |
| `read_files`,`write_files` | `GenericFile` |
| `read_fulfillments`,`write_fulfillments` | `FulfillmentService` |
| `read_gift_cards`,`write_gift_cards` | `GiftCard` |
| `read_inventory`,`write_inventory` | `InventoryLevel`, `InventoryItem` |
| `read_legal_policies` | `ShopPolicy` |
| `read_locales`,`write_locales` | `ShopLocale` |
| `read_locations`,`write_locations` | `Location` |
| `read_markets`,`write_markets` | `Market` |
| `read_marketing_events`,`write_marketing_events` | `MarketingEvent`, `MarketingActivity` |
| `read_merchant_approval_signals` | `MerchantApprovalSignals` |
| `read_metaobject_definitions`,`write_metaobject_definitions` | `MetaobjectDefinition` |
| `read_metaobjects`,`write_metaobjects` | `Metaobject` |
| `read_online_store_navigation``write_online_store_navigation` | `UrlRedirect` |
| `read_order_edits`,`write_order_edits` | `CalculatedOrder`, `DeliveryCarrierService` |
| `read_orders`,`write_orders` | `AbandonedCheckout`, `Fulfillment`, `Order`, `OrderTransaction`, `DeliveryCarrierService` |
| `read_own_subscription_contracts`,`write_own_subscription_contracts` | GraphQL Admin API `SubscriptionContract`permissions requiredCustomer Account API `SubscriptionContract`permissions requiredYou need to [request permission for these access scopes](#subscription-apis-permissions) from your Partner Dashboard before adding them to your app. |
| `read_payment_customizations`,`write_payment_customizations` | `PaymentCustomization` |
| `read_payment_gateways`,`write_payment_gateways` | Payments Apps API `PaymentsAppConfiguration` |
| `read_payment_mandate`,`write_payment_mandate` | `PaymentMandate` |
| `write_payment_sessions` | Payments Apps API `PaymentSession`, `CaptureSession`, `RefundSession`, `VoidSession` |
| `read_payment_terms`,`write_payment_terms` | `PaymentSchedule`, `PaymentTerms` |
| `read_price_rules`,`write_price_rules` | `PriceRule` |
| `write_privacy_settings`,`read_privacy_settings` | `CookieBanner`, `PrivacySettings` |
| `read_products`,`write_products` | `Product`, `ProductVariant`, `Collection`, `ResourceFeedback` |
| `read_purchase_options`,`write_purchase_options` | `SellingPlan` |
| `read_returns`,`write_returns` | `Return` |
| `read_script_tags`,`write_script_tags` | `ScriptTag` |
| `read_shipping`,`write_shipping` | `DeliveryCarrierService` |
| `read_shopify_payments_disputes` | `ShopifyPaymentsDispute` |
| `read_shopify_payments_dispute_evidences` | `ShopifyPaymentsDisputeEvidence` |
| `read_shopify_payments_payouts` | `ShopifyPaymentsPayout`, `ShopifyPaymentsBalanceTransaction` |
| `read_store_credit_accounts` | `StoreCreditAccount` |
| `read_store_credit_account_transactions`,`write_store_credit_account_transactions` | `StoreCreditAccountDebitTransaction`, `StoreCreditAccountCreditTransaction` |
| `read_themes`,`write_themes` | `OnlineStoreTheme` |
| `read_translations` | `TranslatableResource` |
| `read_users` | `StaffMember`shopify plus |
| `read_validations`,`write_validations` | `Validation` |

### Requesting specific permissions

Follow the procedures below to request specific permissions to request access scopes in the Partner Dashboard.

#### Orders permissions

By default, you have access to the last 60 days' worth of orders for a store. To access all the orders, you need to request access to the `read_all_orders` scope from the user:

1. From the Partner Dashboard, go to [**Apps**](https://partners.shopify.com/current/apps).
2. Click the name of your app.
3. Click **API access**.
4. In the **Access requests** section, on the **Read all orders scope** card, click **Request access**.
5. On the **Orders** page that opens, describe your app and why you're applying for access.
6. Click **Request access**.

If Shopify approves your request, then you can add the `read_all_orders` scope to your app along with `read_orders` or `write_orders`.

#### Subscription APIs permissions

Subscription apps let users sell subscription products that generate multiple orders on a specific billing frequency.

With subscription products, the app user isn't required to get customer approval for each subsequent order after the initial subscription purchase. As a result, your app needs to request the required protected access scopes to use Subscription APIs from the app user:

1. From the Partner Dashboard, go to [**Apps**](https://partners.shopify.com/current/apps).
2. Click the name of your app.
3. Click **API access**.
4. In the **Access requests** section, on the **Access Subscriptions APIs** card, click **Request access**.
5. On the **Subscriptions** page that opens, describe why you're applying for access.
6. Click **Request access**.

If Shopify approves your request, then you can add the `read_customer_payment_methods` and `write_own_subscription_contracts` scopes to your app. If you're using the Customer Account API, you can add the `customer_read_own_subscription_contracts` or `customer_write_own_subscription_contracts` scopes.

#### Protected customer data permissions

By default, apps don't have access to any protected customer data. To access protected customer data, you must meet our [protected customer data requirements](https://shopify.dev/docs/apps/launch/protected-customer-data#requirements). You can add the relevant scopes to your app, but the API won't return data from non-development stores until your app is configured and approved for protected customer data use.

***

## Unauthenticated access scopes

Unauthenticated access scopes provide apps with read-only access to the [Storefront API](https://shopify.dev/docs/api/storefront). Unauthenticated access is intended for interacting with a store on behalf of a customer. For example, an app might need to do one or more of following tasks:

* Read products and collections
* Create customers and update customer accounts
* Query international prices for products and orders
* Interact with a cart during a customer's session
* Initiate a checkout

### Request scopes

To request unauthenticated access scopes for an app, select them when you [generate API credentials](https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets) or [change granted access scopes](https://shopify.dev/docs/apps/build/authentication-authorization/app-installation/manage-access-scopes).

To request access scopes or permissions for the Headless channel, refer to [managing the Headless channel](https://shopify.dev/docs/storefronts/headless/building-with-the-storefront-api/manage-headless-channels#request-storefront-permissions).

You can request the following unauthenticated access scopes:

| Scope | Access |
| - | - |
| `unauthenticated_read_checkouts`, `unauthenticated_write_checkouts` | [Cart](https://shopify.dev/docs/api/storefront/latest/objects/cart) object |
| `unauthenticated_read_customers`, `unauthenticated_write_customers` | [Customer](https://shopify.dev/docs/api/storefront/latest/objects/customer) object |
| `unauthenticated_read_customer_tags` | `tags` field on the [Customer](https://shopify.dev/docs/api/storefront/latest/objects/customer) object |
| `unauthenticated_read_content` | Storefront content, such as [Article](https://shopify.dev/docs/api/storefront/latest/objects/article), [Blog](https://shopify.dev/docs/api/storefront/latest/objects/blog), and [Comment](https://shopify.dev/docs/api/storefront/latest/objects/comment) objects |
| `unauthenticated_read_metaobjects` | View metaobjects, such as [Metaobject](https://shopify.dev/docs/api/storefront/latest/objects/metaobject) |
| `unauthenticated_read_product_inventory` | `quantityAvailable` field on the [ProductVariant](https://shopify.dev/docs/api/storefront/latest/objects/productvariant) object and `totalAvailable` field on the [Product](https://shopify.dev/docs/api/storefront/latest/objects/product) object |
| `unauthenticated_read_product_listings` | [Product](https://shopify.dev/docs/api/storefront/latest/objects/product) and [Collection](https://shopify.dev/docs/api/storefront/latest/objects/collection) objects |
| `unauthenticated_read_product_pickup_locations` | [Location](https://shopify.dev/docs/api/storefront/latest/objects/location) and [StoreAvailability](https://shopify.dev/docs/api/storefront/latest/objects/storeavailability) objects |
| `unauthenticated_read_product_tags` | `tags` field on the [Product](https://shopify.dev/docs/api/storefront/latest/objects/product) object |
| `unauthenticated_read_selling_plans` | Selling plan content on the [Product](https://shopify.dev/docs/api/storefront/latest/objects/product) object |

***

## Customer access scopes

Customer access scopes provide apps with read and write access to the [Customer Account API](https://shopify.dev/docs/api/customer). Customer access is intended for interacting with data that belongs to a customer. For example, an app might need to do one or more of following tasks:

* Read customers orders
* Update customer accounts
* Create and update customer addresses
* Read shop, customer or order metafields

### Request scopes

To request access scopes or permissions for the Headless or Hydrogen channel, refer to [managing permissions](https://shopify.dev/docs/storefronts/headless/building-with-the-customer-account-api/getting-started#step-2-configure-customer-account-api-access).

You can request the following customer access scopes:

| Scope | Access |
| - | - |
| `customer_read_customers`, `customer_write_customers` | [Customer](https://shopify.dev/docs/api/customer/latest/objects/Customer) object |
| `customer_read_orders`, `customer_write_orders` | [Order](https://shopify.dev/docs/api/customer/latest/objects/Order) object |
| `customer_read_draft_orders` | [Draft Order](https://shopify.dev/docs/api/customer/latest/objects/DraftOrder) object |
| `customer_read_markets` | [Market](https://shopify.dev/docs/api/customer/latest/objects/Market) object |
| `customer_read_store_credit_accounts` | [Store Credit Account](https://shopify.dev/docs/api/customer/latest/objects/StoreCreditAccount) object |
| `customer_read_own_subscription_contracts`, `customer_write_own_subscription_contracts` | [Subscription Contract](https://shopify.dev/docs/api/customer/latest/objects/SubscriptionContract) object for records that belong to your app |
| `customer_write_subscription_contracts` | [Subscription Contract](https://shopify.dev/docs/api/customer/latest/objects/SubscriptionContract) object for all records. Only available for Hydrogen and Headless storefronts |
| `customer_read_companies`, `customer_write_companies` | [Company](https://shopify.dev/docs/api/customer/latest/objects/Company) object |
| `customer_read_locations`, `customer_write_locations` | [Company Location](https://shopify.dev/docs/api/customer/latest/objects/CompanyLocation) object |

***

## Checking granted access scopes

You can check your app's granted access scopes using the [`appInstallation`](https://shopify.dev/docs/api/admin-graphql/latest/queries/appInstallation?example=Get+the+access+scopes+associated+with+the+app+installation) query in the GraphQL Admin API.

***

## Limitations

* Apps should request only the minimum amount of data that's necessary for an app to function when using a Shopify API. Shopify restricts access to scopes for apps that don't require legitimate use of the associated data.
* Only [public or custom apps](https://shopify.dev/docs/apps/launch/distribution) are granted access scopes. Legacy app types, such as private or unpublished, won't be granted new access scopes.

***

* [How it works](https://shopify.dev/docs/api/usage/access-scopes.md#how-it-works)
* [Authenticated access scopes](https://shopify.dev/docs/api/usage/access-scopes.md#authenticated-access-scopes)
* [Unauthenticated access scopes](https://shopify.dev/docs/api/usage/access-scopes.md#unauthenticated-access-scopes)
* [Customer access scopes](https://shopify.dev/docs/api/usage/access-scopes.md#customer-access-scopes)
* [Checking granted access scopes](https://shopify.dev/docs/api/usage/access-scopes.md#checking-granted-access-scopes)
* [Limitations](https://shopify.dev/docs/api/usage/access-scopes.md#limitations)

---
title: Global IDs in Shopify APIs
description: >-
  Learn how global IDs work in Shopify APIs and how to retrieve global IDs for
  different objects.
api_name: usage
source_url:
  html: 'https://shopify.dev/docs/api/usage/gids'
  md: 'https://shopify.dev/docs/api/usage/gids.md'
---

ExpandOn this page

* [How it works](https://shopify.dev/docs/api/usage/gids.md#how-it-works)
* [Global ID examples](https://shopify.dev/docs/api/usage/gids.md#global-id-examples)
* [Querying global IDs](https://shopify.dev/docs/api/usage/gids.md#querying-global-ids)
* [Using global IDs in mutations](https://shopify.dev/docs/api/usage/gids.md#using-global-ids-in-mutations)
* [Finding equivalent IDs between REST and Graph​QL](https://shopify.dev/docs/api/usage/gids.md#finding-equivalent-ids-between-rest-and-graphql)

# Global IDs in Shopify APIs

Shopify's GraphQL APIs use global IDs to refer to objects. A global ID is an application-wide uniform resource identifier (URI) that uniquely identifies an object. You can use a global ID to retrieve a specific Shopify object of any type.

***

## How it works

To enable GraphQL clients to neatly handle caching and data refetching, GraphQL servers expose object identifiers in a standardized way using the [Relay specification](https://relay.dev/graphql/objectidentification.htm).

Relay asks for a compliant server to expose a standard mechanism for fetching any object given an ID. These objects are referred as `nodes` and they implement the [`Node` interface](https://relay.dev/graphql/objectidentification.htm#sec-Node-Interface). Shopify's GraphQL APIs provide a [versionable](https://shopify.dev/docs/api/usage/versioning) implementation of this interface.

### Global ID structure

Shopify uses [GlobalID](https://github.com/rails/globalid) to encode global IDs. By default, when implementing a `Node` interface, a type's `id` field constructs a global ID with the following structure:

## Global ID structure

```text
gid://shopify/{object_name}/{id}
```

For example, a [`Product`](https://shopify.dev/docs/api/admin-graphql/latest/objects/Product) object with the ID `123` would resolve to the following global ID:

## Global ID of a Product object

```text
gid://shopify/Product/123
```

### Parameterized global IDs

Some objects are more complex and have global IDs that contain parameters. A global ID with parameters has the following structure:

## Parameterized global ID structure

```text
gid://shopify/{child_object_name}/{child_object_id}?{parent_object_name}_id={parent_object_id}
```

For example, the [`InventoryLevel`](https://shopify.dev/docs/api/admin-graphql/latest/objects/InventoryLevel) object is associated with the [`InventoryItem`](https://shopify.dev/docs/api/admin-graphql/latest/objects/InventoryItem) object. If the `InventoryLevel` object's ID is `123` and the `InventoryItem` object's ID is `456`, then the global ID would resolve to the following structure:

## Parameterized global ID for InventoryLevel object

```text
gid://shopify/InventoryLevel/123?inventory_item_id=456
```

***

## Global ID examples

The following table provides some common examples of global IDs that are associated with different GraphQL objects. For example purposes, each global ID is referenced as `123`.

| GraphQL object | Example global ID | Description |
| - | - | - |
| `Article` | `gid://shopify/Article/123` | A globally unique identifier of an article. |
| `Blog` | `gid://shopify/Blog/123` | A globally unique identifier of a blog. |
| `Collection` | `gid://shopify/Collection/123` | A globally unique identifier of a collection. |
| `Customer` | `gid://shopify/Customer/123` | A globally unique identifier of a customer. |
| `DeliveryCarrierService` | `gid://shopify/DeliveryCarrierService/123` | A globally unique identifier of a delivery carrier service. |
| `DeliveryLocationGroup` | `gid://shopify/DeliveryLocationGroup/123` | A globally unique identifier of a delivery location group. |
| `DeliveryProfile` | `gid://shopify/DeliveryProfile/123` | A globally unique identifier of a delivery profile, an object that enables shops to create shipping rates for each product variant and location. |
| `DeliveryZone` | `gid://shopify/DeliveryZone/123` | A globally unique identifier of a delivery zone. |
| `DraftOrder` | `gid://shopify/DraftOrder/123` | A globally unique identifier of a draft order. |
| `DraftOrderLineItem` | `gid://shopify/DraftOrderLineItem/123` | A globally unique identifier of a line item in a draft order. |
| `Duty` | `gid://shopify/Duty/123` | A globally unique identifier of duties on an order. |
| `EmailTemplate` | `gid://shopify/EmailTemplate/123` | A globally unique identifier of an email notification template that's associated with a Shopify store. |
| `Fulfillment` | `gid://shopify/Fulfillment/123` | A globally unique identifier of a fulfillment. |
| `FulfillmentEvent` | `gid://shopify/FulfillmentEvent/123` | A globally unique identifier of a fulfillment event. |
| `FulfillmentService` | `gid://shopify/FulfillmentService/123` | A globally unique identifier of a fulfillment service. |
| `InventoryItem` | `gid://shopify/InventoryItem/123` | A globally unique identifier of an inventory item, an object that represents a physical good. |
| `InventoryLevel` | `gid://shopify/InventoryLevel/123?inventory_item_id=456` | A globally unique identifier of an inventory level, an object that represents the quantities of an inventory item for a location. |
| `LineItem` | `gid://shopify/LineItem/123` | A globally unique identifier of a line item. |
| `Location` | `gid://shopify/Location/123` | A globally unique identifier of a location, an object that represents a geographical location where your stores, pop-up stores, headquarters, and warehouses exist. |
| `MarketingEvent` | `gid://shopify/MarketingEvent/123` | A globally unique identifer of a marketing event, an object that represents actions taken by your app, on behalf of the app user, to market products, collections, discounts, pages, blog posts, and other features. |
| `MediaImage` | `gid://shopify/MediaImage/123` | A globally unique identifier of a Shopify-hosted image. |
| `Metafield` | `gid://shopify/Metafield/123` | A globally unique identifier of a metafield, an object that provides a flexible way to attach additional information to a Shopify object. |
| `Order` | `gid://shopify/Order/123` | A globally unique identifier of an order. |
| `OrderTransaction` | `gid://shopify/OrderTransaction/123` | A globally unique identifier of an order transaction. |
| `Page` | `gid://shopify/Page/123` | A globally unique identifier of a page. |
| `Product` | `gid://shopify/Product/123` | A globally unique identifier of a product. |
| `ProductImage` | `gid://shopify/ProductImage/123` | A globally unique identifier of a product image. |
| `ProductVariant` | `gid://shopify/ProductVariant/123` | A globally unique identifier of a product variant. |
| `Refund` | `gid://shopify/Refund/123` | A globally unique identifier of a refund. |
| `Shop` | `gid://shopify/Shop/123` | A globally unique identifier of a Shopify store. |
| `StaffMember` | `gid://shopify/StaffMember/123` | A globally unique identifier of a staff member in a Shopify store. |
| `Theme` | `gid://shopify/Theme/123` | A globally unique identifier of a Shopify theme. |

***

## Querying global IDs

A `node` is an object that has a global ID and is of a type that's defined by the schema. Connections retrieve a list of nodes. For example, the `products` connection finds all the `Product` type nodes connected to the query root.

The following example shows how to use the [GraphQL Admin API](https://shopify.dev/docs/api/admin-graphql) to query the global IDs of the first 5 products in your store:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
{
  products(first:5) {
    edges {
      node {
        id
      }
    }
  }
}
```

## JSON response

```json
{
  "data": {
    "products": {
      "edges": [
        {
          "node": {
            "id": "gid://shopify/Product/1"
          }
        },
        {
          "node": {
            "id": "gid://shopify/Product/2"
          }
        },
        {
          "node": {
            "id": "gid://shopify/Product/3"
          }
        },
        {
          "node": {
            "id": "gid://shopify/Product/4"
          }
        },
        {
          "node": {
            "id": "gid://shopify/Product/5"
          }
        }
      ]
    }
  }
}
```

### Retrieving global IDs through the UI

Some global IDs can be quickly retrieved through the user interface (UI). For example, you can find a product's global ID from your Shopify admin by clicking **Products** and clicking a specific product. The URL of the page contains the product's global ID:

## Page URL containing the global ID of a product

```text
https://admin.shopify.com/store/{shop}/products/{id}
```

***

## Using global IDs in mutations

Many mutations in Shopify's GraphQL APIs require an `id` input field. The value of the `id` field needs to be constructed as a global ID.

The following example shows how to use an `id` input field to update a product's status:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
mutation {
  productUpdate(input: {id: "gid://shopify/Product/3", title: "Burton Custom Freestyle 151", status: "ARCHIVED"} ) {
    product {
      id
      status
    }
  }
}
```

## JSON response

```json
{
  "data": {
    "productUpdate": {
      "product": {
        "id": "gid://shopify/Product/3",
        "status": "ARCHIVED"
      }
    }
  }
}
```

***

## Finding equivalent IDs between REST and Graph​QL

Most REST Admin API resources include an `admin_graphql_api_id` property, which provides a global ID for the equivalent object in the GraphQL Admin API. For example, the following two properties on the [`Customer`](https://shopify.dev/docs/api/admin-rest/latest/resources/customer) resource are equivalent:

## Customer resource

```json
{
  "id": 123456789, // A simple ID for a Customer resource in the REST Admin API
  "admin_graphql_api_id": "gid://shopify/Customer/123456789" // A global ID for the equivalent Customer object in the GraphQL Admin API
}
```

Similarly, most GraphQL Admin API objects include a `legacyResourceId` field, which provides a simple ID for the equivalent resource in the REST Admin API. For example, the following two fields on the [`Product`](https://shopify.dev/docs/api/admin-graphql/latest/objects/Product#field-product-legacyresourceid) object are equivalent:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
{
  products(first:5) {
    edges {
      node {
        id # A global ID for the Product object in the GraphQL Admin API
        legacyResourceId  # A simple ID for the equivalent Product resource in the REST Admin API
      }
    }
  }
}
```

## JSON response

```json
{
	"data": {
		"products": {
			"edges": [
				{
					"node": {
						"id": "gid://shopify/Product/4353554645014",
						"legacyResourceId": "4353554645014"
					}
				},
				{
					"node": {
						"id": "gid://shopify/Product/4353554710550",
						"legacyResourceId": "4353554710550"
					}
				},
				{
					"node": {
						"id": "gid://shopify/Product/4358159007766",
						"legacyResourceId": "4358159007766"
					}
				},
				{
					"node": {
						"id": "gid://shopify/Product/5591484858390",
						"legacyResourceId": "5591484858390"
					}
				},
				{
					"node": {
						"id": "gid://shopify/Product/5591485448214",
						"legacyResourceId": "5591485448214"
					}
				}
			]
		}
	}
}
```

***

* [How it works](https://shopify.dev/docs/api/usage/gids.md#how-it-works)
* [Global ID examples](https://shopify.dev/docs/api/usage/gids.md#global-id-examples)
* [Querying global IDs](https://shopify.dev/docs/api/usage/gids.md#querying-global-ids)
* [Using global IDs in mutations](https://shopify.dev/docs/api/usage/gids.md#using-global-ids-in-mutations)
* [Finding equivalent IDs between REST and Graph​QL](https://shopify.dev/docs/api/usage/gids.md#finding-equivalent-ids-between-rest-and-graphql)

---
title: Perform bulk operations with the GraphQL Admin API
description: Learn how to retrieve large datasets from Shopify.
api_name: usage
source_url:
  html: 'https://shopify.dev/docs/api/usage/bulk-operations/queries'
  md: 'https://shopify.dev/docs/api/usage/bulk-operations/queries.md'
---

ExpandOn this page

* [Limitations](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#limitations)
* [Access token considerations](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#access-token-considerations)
* [Bulk query overview](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#bulk-query-overview)
* [Bulk query workflow](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#bulk-query-workflow)
* [Download result data](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#download-result-data)
* [The JSONL data format](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#the-jsonl-data-format)
* [Operation failures](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#operation-failures)
* [Canceling an operation](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#canceling-an-operation)
* [Rate limits](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#rate-limits)
* [Operation restrictions](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#operation-restrictions)
* [Next steps](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#next-steps)

# Perform bulk operations with the GraphQL Admin API

With the GraphQL Admin API, you can use bulk operations to asynchronously fetch data in bulk. The API is designed to reduce complexity when dealing with pagination of large volumes of data. You can bulk query any connection field that's defined by the GraphQL Admin API schema.

Instead of manually paginating results and managing a client-side throttle, you can instead run a bulk query operation. Shopify's infrastructure does the hard work of executing your query, and then provides you with a URL where you can download all of the data.

The GraphQL Admin API supports querying a single top-level field, and then selecting the fields that you want returned. You can also nest connections, such as variants on products.

Apps are limited to running a single bulk operation at a time per shop. When the operation is complete, the results are delivered in the form of a [JSONL file](http://jsonlines.org/) that Shopify makes available at a URL.

Note

Bulk operations are only available through the [GraphQL Admin API](https://shopify.dev/docs/api/admin-graphql). You can't perform bulk operations with the Storefront API.

***

## Limitations

* You can run only one bulk operation of each type ([`bulkOperationRunMutation`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkoperationrunmutation) or [`bulkOperationRunQuery`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkoperationrunquery)) at a time per shop.

- The bulk query operation has to complete within 10 days. After that it will be stopped and marked as `failed`.

When your query runs into this limit, consider reducing the query complexity and depth.

***

## Access token considerations

Because bulk query operations can take several days to complete, you should use [offline access tokens](https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/offline-access-tokens) when initiating bulk operations. [Online access tokens](https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens/online-access-tokens) expire after 24 hours, which means they'll expire before long-running bulk operations can complete. Using offline access tokens ensures that your app maintains access to retrieve the results when the operation finishes.

***

## Bulk query overview

The complete flow for running bulk queries is covered [later](#bulk-query-workflow), but below are some small code snippets that you can use to get started quickly.

### Step 1.​Submit a query

Run a [`bulkOperationRunQuery`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkOperationRunQuery) mutation and specify what information you want from Shopify.

The following mutation queries the `products` connection and returns each product's ID and title.

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
mutation {
  bulkOperationRunQuery(
   query: """
    {
      products {
        edges {
          node {
            id
            title
          }
        }
      }
    }
    """
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
```

## JSON response

```json
{
  "data": {
    "bulkOperationRunQuery": {
      "bulkOperation": {
        "id": "gid:\/\/shopify\/BulkOperation\/720918",
        "status": "CREATED"
      },
      "userErrors": []
    }
  },
...
}
```

### Step 2.​Wait for the operation to finish

To retrieve data, you need to wait for the operation to finish. You can determine when a bulk operation has finished by using a webhook or by polling the operation's status.

Tip

Subscribing to the webhook topic is recommended over polling as it limits the number of redundant API calls.

#### Option A. Subscribe to the `bulk_operations/finish` webhook topic

Note

Using webhooks with bulk operations is only available in Admin API version 2021-10 and higher.

You can use the [webhookSubscriptionCreate](https://shopify.dev/docs/api/admin-graphql/latest/mutations/webhooksubscriptioncreate) mutation to subscribe to the `bulk_operations/finish` webhook topic in order to receive a webhook when any operation finishes - in other words, it has completed, failed, or been cancelled.

For full setup instructions, refer to [Configuring webhooks](https://shopify.dev/docs/apps/build/webhooks/subscribe).

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL mutation

```graphql
mutation {
  webhookSubscriptionCreate(
    topic: BULK_OPERATIONS_FINISH
    webhookSubscription: {
      format: JSON,
      uri: "https://12345.ngrok.io/"}
  ) {
    userErrors {
      field
      message
    }
    webhookSubscription {
      id
    }
  }
}
```

## JSON response

```json
{
  "data": {
    "webhookSubscriptionCreate": {
      "userErrors": [],
      "webhookSubscription": {
        "id": "gid://shopify/WebhookSubscription/4567"
      }
    }
  },
  "extensions": {
    "cost": {
      "requestedQueryCost": 10,
      "actualQueryCost": 10,
      "throttleStatus": {
        "maximumAvailable": 1000,
        "currentlyAvailable": 990,
        "restoreRate": 50
      }
    }
  }
}
```

After you've subscribed to the webhook topic, Shopify sends a POST request to the specified URL any time a bulk operation on the store (both queries and [mutations](https://shopify.dev/docs/api/usage/bulk-operations/imports)) finishes.

**Example webhook response**

```json
{
  "admin_graphql_api_id": "gid://shopify/BulkOperation/720918",
  "completed_at": "2024-08-29T17:23:25Z",
  "created_at": "2024-08-29T17:16:35Z",
  "error_code": null,
  "status": "completed",
  "type": "query"
}
```

You now must retrieve the bulk operation's data URL by using the `node` field and passing the `admin_graphql_api_id` value from the webhook payload as its `id`:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
query {
  node(id: "gid://shopify/BulkOperation/720918") {
    ... on BulkOperation {
      url
      partialDataUrl
    }
  }
}
```

## JSON response

```json
{
  "data": {
    "node": {
      "url": "https:\/\/storage.googleapis.com\/shopify\/dyfkl3g72empyyoenvmtidlm9o4g?<params />",
      "partialDataUrl": null
    }
  },
  "extensions": {
    "cost": {
      "requestedQueryCost": 1,
      "actualQueryCost": 1,
      "throttleStatus": {
        "maximumAvailable": 1000,
        "currentlyAvailable": 999,
        "restoreRate": 50
      }
    }
  }
}
```

For more information on how webhooks work, refer to [Webhooks](https://shopify.dev/docs/apps/build/webhooks).

Note

Webhook delivery isn't always guaranteed, so you might still need to poll for the operation's status to check when it's finished.

#### Option B.​Poll your operation's status

While the operation is running, you can poll to see its progress using the `currentBulkOperation` field. The `objectCount` field increments to indicate the operation's progress, and the `status` field returns whether the operation is completed.

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
query {
  currentBulkOperation {
    id
    status
    errorCode
    createdAt
    completedAt
    objectCount
    fileSize
    url
    partialDataUrl
  }
}
```

## JSON response

```json
{
  "data": {
    "currentBulkOperation": {
      "id": "gid:\/\/shopify\/BulkOperation\/720918",
      "status": "COMPLETED",
      "errorCode": null,
      "createdAt": "2024-08-29T17:16:35Z",
      "completedAt": "2024-08-29T17:23:25Z",
      "objectCount": "57",
      "fileSize": "358",
      "url": "https:\/\/storage.googleapis.com\/shopify\/dyfkl3g72empyyoenvmtidlm9o4g?<params />",
      "partialDataUrl": null
    }
  },
  ...
}
```

### Step 3.​Retrieve your data

When an operation is completed, a JSONL output file is available for download at the URL specified in the `url` field. If the query produced no results, then the `url` field will return `null`.

See [Download result data](#download-result-data) for more details on the files we return and [JSONL file format](#the-jsonl-data-format) for how to parse them.

***

## Bulk query workflow

Below is the high-level workflow for creating a bulk query:

1. [Identify a potential bulk operation](#identify-a-potential-bulk-query).

   You can use a new or existing query, but it should potentially return a lot of data. Connection-based queries work best.

2. Test the query by using the [Shopify GraphiQL app](https://shopify-graphiql-app.shopifycloud.com).

3. [Write a new mutation document](#write-a-bulk-operation) for `bulkOperationRunQuery`.

4. Include the query as the value for the `query` argument in the mutation.

5. Run the mutation.

6. [Wait for the bulk operation to finish](#wait-for-the-bulk-operation-to-finish) by either:

   1. [Subscribing to a webhook topic](#option-a-use-the-bulk_operations-finish-webhook-topic) that sends a webhook payload when the operation is finished.
   2. [Polling the bulk operation](#option-b-poll-a-running-bulk-operation) until the `status` field shows that the operation is no longer running.

You can [check the operation's progress](#check-an-operations-progress) using the `objectCount` field in `currentBulkOperation`.

1. Download the JSONL file at the URL provided in the `url` field.

### Identify a potential bulk query

Identify a new or existing query that could return a lot of data and would benefit from being a bulk operation. Queries that use pagination to get all pages of results are the most common candidates.

The example query below retrieves some basic information from a store's first 50 products that were created on or after January 1, 2024.

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
{
  products(query: "created_at:>=2024-01-01 AND created_at:<2024-05-01", first: 50) {
    edges {
      cursor
      node {
        id
        createdAt
        updatedAt
        title
        handle
        descriptionHtml
        productType
        options {
          name
          position
          values
        }
        priceRange {
          minVariantPrice {
            amount
            currencyCode
          }
          maxVariantPrice {
            amount
            currencyCode
          }
        }
      }
    }
    pageInfo {
      hasNextPage
    }
  }
}
```

Tip

Use the [Shopify GraphiQL app](https://shopify-graphiql-app.shopifycloud.com) to run this query against your development store. The query used in a bulk operation requires the same permissions as it would when run as a normal query, so it's important to run the query first and ensure it succeeds without any access denied errors.

### Write a bulk operation

To turn the query above into a bulk query, use the [`bulkOperationRunQuery`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkOperationRunQuery) mutation. It's easiest to begin with a skeleton mutation without the `query` value:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL mutation

```graphql
mutation {
  bulkOperationRunQuery(
    query:"""
    """
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
```

* The triple quotes (""") define a multi-line string in GraphQL.
* The bulk operation's ID is returned so you can poll the operation.
* The `userErrors` field is returned to retrieve any error messages.

Paste the original sample query into the mutation, and then make a couple of minor optional changes:

* The `first` argument is optional and ignored if present, so it can be removed.
* The `cursor` and `pageInfo` fields are also optional and ignored if present, so they can be removed.

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL mutation

```graphql
mutation {
  bulkOperationRunQuery(
    query:"""
    {
      products(query: "created_at:>=2024-01-01 AND created_at:<2024-05-01") {
        edges {
          node {
            id
            createdAt
            updatedAt
            title
            handle
            descriptionHtml
            productType
            options {
              name
              position
              values
            }
            priceRange {
              minVariantPrice {
                amount
                currencyCode
              }
              maxVariantPrice {
                amount
                currencyCode
              }
            }
          }
        }
      }
    }
    """
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
```

If the mutation is successful, then the response looks similar to the example below:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## JSON response

```json
{
  "data": {
    "bulkOperationRunQuery": {
      "bulkOperation": {
        "id": "gid:\/\/shopify\/BulkOperation\/1",
        "status": "CREATED"
      },
      "userErrors": []
    }
  },
  ...
}
```

### Wait for the bulk operation to finish

To retrieve data, you need to wait for the operation to finish. You can determine when a bulk operation has finished by using a webhook or by polling the operation's status.

#### Option A. Use the `bulk_operations/finish` webhook topic

Use the [webhookSubscriptionCreate](https://shopify.dev/docs/api/admin-graphql/latest/mutations/webhooksubscriptioncreate) mutation to subscribe to the [`bulk_operations/finish`](https://shopify.dev/docs/api/admin-graphql/latest/enums/webhooksubscriptiontopic) webhook topic. For full setup instructions, refer to [Configuring webhooks](https://shopify.dev/docs/apps/build/webhooks/subscribe).

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL mutation

```graphql
mutation {
  webhookSubscriptionCreate(
    topic: BULK_OPERATIONS_FINISH
    webhookSubscription: {
      format: JSON,
      uri: "https://12345.ngrok.io/"}
  ) {
    userErrors {
      field
      message
    }
    webhookSubscription {
      id
    }
  }
}
```

After you've subscribed, you'll receive a webhook any time a bulk operation on the store (both queries and [mutations](https://shopify.dev/docs/api/usage/bulk-operations/imports)) finishes (for example, completes, fails, or is cancelled). Refer to the [GraphQL Admin API reference](https://shopify.dev/docs/api/webhooks?reference=graphql) for details on the webhook payload.

Once you receive the webhook, you must retrieve the bulk operation's data URL by querying the `node` field and passing in the ID given by `admin_graphql_api_id` in the webhook payload:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
query {
  node(id: "gid://shopify/BulkOperation/1") {
    ... on BulkOperation {
      url
      partialDataUrl
    }
  }
}
```

#### Option B.​Poll a running bulk operation

Another way to determine when the bulk operation has finished is to query the `currentBulkOperation` field:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
{
  currentBulkOperation {
    id
    status
    errorCode
    createdAt
    completedAt
    objectCount
    fileSize
    url
    partialDataUrl
  }
}
```

The field returns the latest bulk operation created (regardless of its status) for the authenticated app and shop. If you want to look up a specific operation by ID, then you can use the `node` field:

You can adjust your polling intervals based on the amount of data that you expect. For example, if you're currently making pagination queries manually and it takes one hour to fetch all product data, then that can serve as a rough estimate for the bulk operation time. In this situation, a polling interval of one minute would probably be better than every 10 seconds.

To learn about the other possible operation statuses, refer to the [`BulkOperationStatus` reference](https://shopify.dev/docs/api/admin-graphql/latest/enums/bulkoperationstatus).

### Check an operation's progress

Although polling is useful for checking whether an operation is complete, you can also use it to check the operation's progress by using the `objectCount` field. This field provides you with a running total of all the objects processed by your bulk operation. You can use the object count to validate your assumptions about how much data should be returned.

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
{
  currentBulkOperation {
    status
    objectCount
    url
  }
}
```

For example, if you're trying to query all products created in a single month and the object count exceeds your expected number, then it might be a sign that your query conditions are wrong. In that case, you might want to [cancel](#canceling-an-operation) your current operation and run a new one with a different query.

***

## Download result data

Only once an operation is finished running will there be result data available.

If an operation successfully completes, the `url` field will contain a URL where you can download the data. If an operation fails but some data was retrieved before the failure occurred, then a partially complete output file is available at the URL specified in the `partialDataUrl` field. In either case, the URLs return will be signed (authenticated) and will expire after one week.

Now that you've downloaded the data, it's time to parse it according to the JSONL format.

***

## The JSONL data format

Normal (non-bulk) GraphQL responses are JSON. The response structure mirrors the query structure, which results in a single JSON object with many nested objects. Most standard JSON parsers require the entire string or file to be read into memory, which can cause issues when the responses are large.

Since bulk operations are specifically designed to fetch large datasets, we've chosen the [JSON Lines](http://jsonlines.org/) (JSONL) format for the response data so that clients have more flexibility in how they consume the data. JSONL is similar to JSON, but each line is its own valid JSON object. To avoid issues with memory consumption, the file can be parsed one line at a time by using file streaming functionality, which most languages have.

Each line in the file is a node object returned in a connection. If a node has a nested connection, then each child node is extracted into its own object on the next line. For example, a bulk operation might use the following query to retrieve a list of products and their nested variants:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
{
  products {
    edges {
      node {
        id
        variants {
          edges {
            node {
              id
              title
            }
          }
        }
      }
    }
  }
}
```

In the JSONL results, each product object is followed by each of its variant objects on a new line. The order of each connection type is preserved and all nested connections appear after their parents in the file. Because connections are no longer nested in the response data structure, the bulk operation result automatically includes the `__parentId` field, which is a reference to an object's parent. This field doesn't exist in the API schema, so you can't explicitly query it.

```json
{"id":"gid://shopify/Product/1921569226808"}
{"id":"gid://shopify/ProductVariant/19435458986123","title":"52","__parentId":"gid://shopify/Product/1921569226808"}
{"id":"gid://shopify/ProductVariant/19435458986040","title":"70","__parentId":"gid://shopify/Product/1921569226808"}
{"id":"gid://shopify/Product/1921569259576"}
{"id":"gid://shopify/ProductVariant/19435459018808","title":"34","__parentId":"gid://shopify/Product/1921569259576"}
{"id":"gid://shopify/Product/1921569292344"}
{"id":"gid://shopify/ProductVariant/19435459051576","title":"Default Title","__parentId":"gid://shopify/Product/1921569292344"}
{"id":"gid://shopify/Product/1921569325112"}
{"id":"gid://shopify/ProductVariant/19435459084344","title":"36","__parentId":"gid://shopify/Product/1921569325112"}
{"id":"gid://shopify/Product/1921569357880"}
{"id":"gid://shopify/ProductVariant/19435459117112","title":"47","__parentId":"gid://shopify/Product/1921569357880"}
```

### Example

Most programming languages have the ability to read a file one line at a time to avoid reading the entire file into memory. This feature should be taken advantage of when dealing with the JSONL data files.

Here's a simple example in Ruby to demonstrate the proper way of loading and parsing a JSONL file:

```ruby
# Efficient: reads the file a single line at a time
File.open(file) do |f|
  f.each do |line|
    JSON.parse(line)
  end
end


# Inefficient: reads the entire file into memory


jsonl = File.read(file)


jsonl.each_line do |line|
  JSON.parse(line)
end
```

To demonstrate the difference using a 100MB JSONL file, the "good" version would consume only 2.5MB of memory while the "bad" version would consume 100MB (equal to the file size).

Other languages:

* NodeJS: [`readline`](https://nodejs.org/api/readline.html#readline_example_read_file_stream_line_by_line)
* Python: [built-in iterator](https://docs.python.org/3/tutorial/inputoutput.html#methods-of-file-objects)
* PHP: [`fgets`](https://www.php.net/manual/en/function.fgets.php)

***

## Operation failures

Bulk operations can fail for any of the reasons that a regular GraphQL query would fail, such as not having permission to query a field. For this reason, we encourage you to run the query normally first to make sure that it works. You'll get much better error feedback than when a query fails within a bulk operation.

When a bulk operation fails, [some data might be available to download](#download-result-data), the `status` field returns `FAILED`, and the `errorCode` field includes one of the following codes:

* `ACCESS_DENIED`: there are missing access scopes. Run the query normally (outside of a bulk operation) to get more details on which field is causing the issue.
* `INTERNAL_SERVER_ERROR`: something went wrong on our server and we've been notified of the error. These errors might be intermittent, so you can try [submitting the query again](#step-1-submit-a-query).
* `TIMEOUT`: one or more query timeouts occurred during execution. Try removing some fields from your query so that it can run successfully. These timeouts might be intermittent, so you can try [submitting the query again](#step-1-submit-a-query).

Tip

Querying resources using a [range search](https://shopify.dev/docs/api/usage/search-syntax#search-query-syntax) might timeout or return an error if the collection of resources is sufficiently large, and the search field is different from the specified (or default) sort key for the connection you are querying. If your query is slow or returns an error, then try specifying a sort key that matches the field used in the search. For example, `query: "created_at:>2024-05-01", sortKey: CREATED_AT`.

To learn about the other possible operation error codes, refer to the [`BulkOperationErrorCode`](https://shopify.dev/docs/api/admin-graphql/latest/enums/BulkOperationErrorCode) reference.

### Canceled operations

If bulk operations have stalled, then they might be canceled by Shopify. After bulk operations are canceled, a `status` of `CANCELED` is returned. You can retry canceled bulk operations by [submitting the query again](#step-1-submit-a-query).

Note

When using the `bulk_operations/finish` webhook, the `error_code` and `status` fields in the webhook payload will be lowercase. For example, `failed` instead of `FAILED`.

***

## Canceling an operation

To cancel an in-progress bulk operation, use the [`bulkOperationCancel`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkOperationCancel) mutation with the operation ID.

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL mutation

```graphql
mutation {
  bulkOperationCancel(id: "gid://shopify/BulkOperation/1") {
    bulkOperation {
      status
    }
    userErrors {
      field
      message
    }
  }
}
```

***

## Rate limits

You can run only one bulk operation of each type (`bulkOperationRunMutation` or `bulkOperationRunQuery`) at a time per shop. This limit is in place because operations are asynchronous and long-running. To run a subsequent bulk operation for a shop, you need to either cancel the running operation or wait for it to finish.

### How bulk operations fit within the Admin API rate limits

Bulk operations are initiated by you, the API consumer, by supplying a `query` string within the `bulkOperationRunQuery` mutation. Shopify then executes that `query` string asynchronously as a bulk operation.

This distinction between the `bulkOperationRunQuery` mutation and the bulk query string itself determines how rate limits apply as well: any GraphQL requests made by you count as normal API requests and are subject to [rate limits](https://shopify.dev/docs/api/usage/limits#rate-limits#graphql-admin-api-rate-limits), while the bulk operation query execution is not.

In the following example, you would be charged the cost of the mutation request (as with any other mutation), but not for the `query` for product titles that you want Shopify to run as a bulk operation:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
mutation {
  bulkOperationRunQuery(
   query: """
    {
      products {
        edges {
          node {
            title
          }
        }
      }
    }
    """
  ) {
    bulkOperation {
      id
    }
  }
}
```

Since you're only making low-cost requests for creating operations, polling their status, or canceling them, bulk operations are a very efficient way to query data compared to standard pagination queries.

***

## Operation restrictions

A bulk operation query needs to include a connection. If your query doesn't use a connection, then it should be executed as a normal synchronous GraphQL query.

Bulk operations have some additional restrictions:

* Maximum of five total connections in the query.
* Connections must implement the [`Node`](https://shopify.dev/docs/api/storefront/latest/interfaces/Node) interface
* The top-level `node` and `nodes` fields can't be used.
* Maximum of two levels deep for nested connections. For example, the following is invalid because there are three levels of nested connections:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
{
      products {
        edges {
          node {
            id
            variants { # nested level 1
              edges {
                node {
                  id
                  images { # nested level 2
                    edges {
                      node {
                        id
                        metafields { # nested level 3 (invalid)
                          edges {
                            node {
                              value
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
```

The `bulkOperationRunQuery` mutation will validate the supplied queries and provide errors by using the `userErrors` field.

It's hard to provide exhaustive examples of what's allowed and what isn't given the flexibility of GraphQL queries, so try some and see what works and what doesn't. If you find useful queries which aren't yet supported, then let us know on the [.dev Community](https://community.shopify.dev/) so we can collect common use cases.

***

## Next steps

* Consult our [reference documentation](https://shopify.dev/docs/api/admin-graphql/latest/objects/BulkOperation) to learn more about creating and managing bulk operations.
* Learn how [bulk import large volumes of data asychronously](https://shopify.dev/docs/api/usage/bulk-operations/imports).

***

* [Limitations](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#limitations)
* [Access token considerations](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#access-token-considerations)
* [Bulk query overview](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#bulk-query-overview)
* [Bulk query workflow](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#bulk-query-workflow)
* [Download result data](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#download-result-data)
* [The JSONL data format](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#the-jsonl-data-format)
* [Operation failures](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#operation-failures)
* [Canceling an operation](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#canceling-an-operation)
* [Rate limits](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#rate-limits)
* [Operation restrictions](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#operation-restrictions)
* [Next steps](https://shopify.dev/docs/api/usage/bulk-operations/queries.md#next-steps)

---
title: Bulk import data with the GraphQL Admin API
description: Learn how to bulk import large volumes of data asynchronously.
api_name: usage
source_url:
  html: 'https://shopify.dev/docs/api/usage/bulk-operations/imports'
  md: 'https://shopify.dev/docs/api/usage/bulk-operations/imports.md'
---

ExpandOn this page

* [Requirements](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#requirements)
* [Limitations](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#limitations)
* [How bulk importing data works](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#how-bulk-importing-data-works)
* [Create a JSONL file and include Graph​QL variables](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#create-a-jsonl-file-and-include-graphql-variables)
* [Upload the file to Shopify](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#upload-the-file-to-shopify)
* [Create a bulk mutation operation](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#create-a-bulk-mutation-operation)
* [Wait for the operation to finish](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#wait-for-the-operation-to-finish)
* [Retrieve the results](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#retrieve-the-results)
* [Cancel an operation](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#cancel-an-operation)
* [Next steps](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#next-steps)

# Bulk import data with the GraphQL Admin API

Importing large volumes of data using traditional and synchronous APIs is slow, complex to run, and difficult to manage. Instead of manually running a GraphQL mutation multiple times and managing a client-side throttle, you can run a bulk mutation operation.

Using the GraphQL Admin API, you can bulk import large volumes of data asynchronously. When the operation is complete, the results are delivered in a [JSON Lines (JSONL)](https://jsonlines.org/) file that Shopify makes available at a URL.

This guide introduces the [`bulkOperationRunMutation`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkoperationrunmutation) and shows you how to use it to bulk import data into Shopify.

***

## Requirements

* You're familiar with creating [products](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productcreate), [product variants](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productvariantcreate), and [collections](https://shopify.dev/docs/api/admin-graphql/latest/mutations/collectioncreate) in your development store.
* You're familiar with [performing bulk operations](https://shopify.dev/docs/api/usage/bulk-operations/queries) using the GraphQL Admin API.

***

## Limitations

* You can run only one bulk operation of each type ([`bulkOperationRunMutation`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkoperationrunmutation) or [`bulkOperationRunQuery`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkoperationrunquery)) at a time per shop.

- The bulk mutation operation has to complete within 24 hours. After that it will be stopped and marked as `failed`.

  When your import runs into this limit, consider reducing the input size.

* You can supply only one of the supported GraphQL API mutations to the `bulkOperationRunMutation` at a time:

  * [`collectionCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/collectioncreate)
  * [`collectionUpdate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/collectionupdate)
  * [`customerCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/customercreate)
  * [`customerUpdate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/customerupdate)
  * [`customerAddressCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/customerAddressCreate)
  * [`customerAddressDelete`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/customerAddressDelete)
  * [`customerAddressUpdate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/customerAddressUpdate)
  * [`customerPaymentMethodRemoteCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/customerpaymentmethodremotecreate)
  * [`giftCardCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/giftcardcreate)
  * [`giftCardUpdate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/giftcardupdate)
  * [`marketingActivityUpsertExternal`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/marketingActivityUpsertExternal)
  * [`marketingEngagementCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/marketingEngagementCreate)
  * [`metafieldsSet`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/metafieldsset)
  * [`metaobjectUpsert`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/metaobjectupsert)
  * [`priceListFixedPricesAdd`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/pricelistfixedpricesadd)
  * [`priceListFixedPricesDelete`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/pricelistfixedpricesdelete)
  * [`privateMetafieldUpsert`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/privatemetafieldupsert)
  * [`productCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productcreate)
  * [`productSet`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productSet)
  * [`productUpdate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productupdate)
  * [`productUpdateMedia`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productupdatemedia)
  * [`productVariantsBulkCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productVariantsBulkCreate)
  * [`productVariantsBulkDelete`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productVariantsBulkDelete)
  * [`productVariantsBulkReorder`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productVariantsBulkReorder)
  * [`productVariantsBulkUpdate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productVariantsBulkUpdate)
  * [`publishablePublish`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/publishablePublish)
  * [`publishableUnpublish`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/publishableUnpublish)
  * [`publicationUpdate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/publicationUpdate)
  * [`storeCreditAccountCredit`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/storecreditaccountcredit)
  * [`storeCreditAccountDebit`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/storecreditaccountdebit)
  * [`subscriptionBillingAttemptCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/subscriptionbillingattemptcreate)
  * [`subscriptionContractActivate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/subscriptioncontractactivate)
  * [`subscriptionContractAtomicCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/subscriptioncontractatomiccreate)
  * [`subscriptionContractCancel`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/subscriptioncontractcancel)
  * [`subscriptionContractExpire`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/subscriptioncontractexpire)
  * [`subscriptionContractFail`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/subscriptioncontractfail)
  * [`subscriptionContractPause`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/subscriptioncontractpause)
  * [`subscriptionContractProductChange`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/subscriptioncontractproductchange)
  * [`subscriptionContractSetNextBillingDate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/subscriptioncontractsetnextbillingdate)

- The mutation that's passed into `bulkOperationRunMutation` is limited to one connection field, which is defined by the GraphQL Admin API schema.

* The size of the JSONL file cannot exceed 20MB.

***

## How bulk importing data works

You initiate a bulk operation by supplying a mutation string in the [`bulkOperationRunMutation`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkOperationRunMutation). Shopify then executes that mutation string asynchronously as a bulk operation.

Most GraphQL Admin API requests that you make are subject to [rate limits](https://shopify.dev/docs/api/usage/limits#rate-limits), but the `bulkOperationRunMutation` request isn't. Because you're only making low-cost requests for creating operations, polling their status, or canceling them, bulk mutation operations are an efficient way to create data compared to standard GraphQL API requests.

The following diagram shows the steps involved in bulk importing data into Shopify:

![Workflow for bulk importing data](https://shopify.dev/assets/assets/images/api/tutorials/bulk-import-data-CVIWHlpb.png)

1. **Create a JSONL file and include GraphQL variables**: Include the variables for the mutation in a JSONL file format. Each line in the JSONL file represents one input unit. The mutation runs once on each line of the input file.

2. **Upload the file to Shopify**: Before you upload the file, you must reserve a link by running the [`stagedUploadsCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/stageduploadscreate) mutation. After the space has been reserved, you can upload the file by making a request using the information returned from the [`stagedUploadsCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/stageduploadscreate) response.

3. **Create a bulk mutation operation**: After the file has been uploaded, you can run [`bulkOperationRunMutation`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkoperationrunmutation) to create a bulk mutation operation. The [`bulkOperationRunMutation`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkoperationrunmutation) imports data in bulk by running the supplied GraphQL API mutation with the file of variables uploaded in the last step.

4. **Wait for the operation to finish**: To determine when the bulk mutation has finished, you can either:

   * **Subscribe to a webhook topic**: You can use the [`webhookSubscriptionCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/webhooksubscriptioncreate) mutation to subscribe to the `bulk_operations/finish` webhook topic in order to receive a webhook when any operation finishes - in other words, it has completed, failed, or been cancelled.

   - **Poll the status of the operation**: While the operation is running, you can poll to see its progress using the [`currentBulkOperation`](https://shopify.dev/docs/api/admin-graphql/latest/objects/queryroot) field. The `objectCount` field on the [`bulkOperation`](https://shopify.dev/docs/api/admin-graphql/latest/objects/bulkoperation) object increments to indicate the operation's progress, and the `status` field returns a boolean value that states whether the operation is completed.

5. **Retrieve the results**: When a bulk mutation operation is completed, a JSONL output file is available for download at the URL specified in the `url` field.

***

## Create a JSONL file and include Graph​QL variables

When adding GraphQL variables to a new JSONL file, you need to format the variables so that they are accepted by the corresponding bulk operation GraphQL API. The format of the input variables need to match the GraphQL Admin API schema.

For example, you might want to import a large quantity of products. Each attribute of a product must be mapped to existing fields defined in the GraphQL input object [`ProductInput`](https://shopify.dev/docs/api/admin-graphql/latest/input-objects/productinput). In the JSONL file, each line represents one product input. The GraphQL Admin API runs once on each line of the input file. One input should take up one line only, no matter how complex the input object structure is.

The following example shows a sample JSONL file that is used to create 10 products in bulk:

```json
{ "input": { "title": "Sweet new snowboard 1", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 2", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 3", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 4", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 5", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 6", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 7", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 8", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 9", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 10", "productType": "Snowboard", "vendor": "JadedPixel" } }
```

Note

The GraphQL Admin API doesn't serially process the contents of the JSONL file. Avoid relying on a particular sequence of lines and object order to achieve a desired result.

***

## Upload the file to Shopify

After you've created the JSONL file, and included the GraphQL variables, you can upload the file to Shopify. Before uploading the file, you need to first generate the upload URL and parameters.

### Generate the uploaded URL and parameters

You can use the [`stagedUploadsCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/stageduploadscreate) mutation to generate the values that you need to authenticate the upload. The mutation returns an array of [`stagedMediaUploadTarget`](https://shopify.dev/docs/api/admin-graphql/latest/objects/stagedmediauploadtarget) instances.

An instance of [`stagedMediaUploadTarget`](https://shopify.dev/docs/api/admin-graphql/latest/objects/stagedmediauploadtarget) has the following key properties:

* `parameters`: The parameters that you use to authenticate an upload request.
* `url`: The signed URL where you can upload the JSONL file that includes GraphQL variables.

The mutation accepts an input of type [`stagedUploadInput`](https://shopify.dev/docs/api/admin-graphql/latest/input-objects/stageduploadinput), which has the following fields:

| Field | Type | Description |
| - | - | - |
| `resource` | [enum](https://shopify.dev/docs/api/admin-graphql/latest/enums/stageduploadtargetgenerateuploadresource) | Specifies the resource type to upload. To use `bulkOperationRunMutation`, the resource type must be `BULK_MUTATION_VARIABLES`. |
| `filename` | [string](https://shopify.dev/docs/api/admin-graphql/latest/scalars/String) | The name of the file to upload. |
| `mimeType` | [string](https://shopify.dev/docs/api/admin-graphql/latest/scalars/String) | The [media type](https://en.wikipedia.org/wiki/Media_type) of the file to upload. To use `bulkOperationRunMutation`, the `mimeType` must be `"text/jsonl"`. |
| `httpMethod` | [enum](https://shopify.dev/docs/api/admin-graphql/latest/enums/stageduploadhttpmethodtype) | The HTTP method to be used by the staged upload. To use `bulkOperationRunMutation`, the `httpMethod` must be `POST`. |

#### Example

The following example uses the [`stagedUploadsCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/stageduploadscreate) mutation to generate the values required to upload a JSONL file and be consumed by the [`bulkOperationRunMutation`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkoperationrunmutation). You must first run the `stagedUploadsCreate` mutation with no variables, and then separately send a POST request to the staged upload URL with the JSONL data:

## POST https://{shop}.myshopify.com/admin/api/{api\_version}/graphql.json

## GraphQL mutation

```graphql
mutation {
  stagedUploadsCreate(input:[{
    resource: BULK_MUTATION_VARIABLES,
    filename: "bulk_op_vars",
    mimeType: "text/jsonl",
    httpMethod: POST
  }]){
    userErrors{
      field,
      message
    },
    stagedTargets{
      url,
      resourceUrl,
      parameters {
        name,
        value
      }
    }
  }
}
```

## JSON response

```json
{
  "data": {
    "stagedUploadsCreate": {
      "userErrors": [],
      "stagedTargets": [
        {
          "url": "https://shopify-staged-uploads.storage.googleapis.com",
          "resourceUrl": null,
          "parameters": [
            {
              "name": "key",
              "value": "tmp/21759409/bulk/2d278b12-d153-4667-a05c-a5d8181623de/bulk_op_vars"
            },
            {
              "name": "Content-Type",
              "value": "text/jsonl"
            },
            {
              "name": "success_action_status",
              "value": "201"
            },
            {
              "name": "acl",
              "value": "private"
            },
            {
              "name": "policy",
              "value": "zyJjb25kaXRpb25zIjpbeyJDb250ZW50LVR5cGUiOiJ0ZXh0XC9qc29ubCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0seyJhY2wiOiJwcml2YXRlIn0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMSwyMDk3MTUyMF0seyJidWNrZXQiOiJzaG9waWZ5LXN0YWdlZC11cGxvYWRzIn0seyJrZXkiOiJ0bXBcL2djc1wvMTQzMjMyMjEwNFwvYnVsa1wvM2QyNzhiMTItZDE1My00NjY3LWEwNWMtYTVkODE4MTYyM2RlXC9idWxrX29wX3ZhcnMifSx7IngtZ29vZy1kYXRlIjoiMjAyMjA4MzBUMDI1MTI3WiJ9LHsieC1nb29nLWNyZWRlbnRpYWwiOiJtZXJjaGFudC1hc3NldHNAc2hvcGlmeS10aWVycy5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbVwvMjAyMjA4MzBcL2F1dG9cL3N0b3JhZ2VcL2dvb2c0X3JlcXVlc3QifSx7IngtZ29vZy1hbGdvcml0aG0iOiJHT09HNC1SU0EtU0hBMjU2In1dLCJleHBpcmF0aW9uIjoiMjAyMi0wOC0zMVQwMjo1MToyN1oifQ=="
            },
            {
              "name": "x-goog-credential",
              "value": "merchant-assets@shopify-tiers.iam.gserviceaccount.com/20220830/auto/storage/goog4_request"
            },
            {
              "name": "x-goog-algorithm",
              "value": "GOOG4-RSA-SHA256"
            },
            {
              "name": "x-goog-date",
              "value": "20220830T025127Z"
            },
            {
              "name": "x-goog-signature",
              "value": "4c0f6920cd67cbdf1faae75c112a98d49f9751e4e0c9f525c850f15f40629afa13584ab9937ec9f5292065ca8fd357ba87e98d6ab0e383e15a6e444c7e9bae06fb95dc422ad673fe77aefcb68e9d1a6d55deb478e6976b61769e20863992fffd4036898f76c7a50e92f18aa4d9e3e04aa8d04086386dc0e488f2ccb0ebcc30c17da2ba5a4d6a9cd57553b41ef6698dbefc78a9b3fe1af167ea539b70e83e5fb015f061399e952270202b769ae8f4e7e50e97dbe6679c3281ec3fc886c3a67becc7b3cee1d0e6a2d0777d09f6d7b083499c58f9c566eeb5374afd67e26c7ab2a91cfe5c5deb83a507d7e3c3ea44bb9f401afd3f2e6b09742baff2b30bc3def78a"
            }
          ]
        }
      ]
    }
  },
  "extensions": {
    "cost": {
      "requestedQueryCost": 11,
      "actualQueryCost": 11
    }
  }
}
```

### Upload the JSONL file

After you generate the parameters and URL for an upload, you can upload the JSONL file using a POST request. You must use a multipart form, and include all parameters as form inputs in the request body.

To generate the parameters for the multipart form, start with the parameters returned from the `stagedUploadsCreate` mutation. Then, add the file attachment.

Note

The `file` parameter must be the last parameter in the list. If you add the `file` parameter somewhere else, then you'll receive an error.

**POST request**

```terminal
curl --location --request POST 'https://shopify-staged-uploads.storage.googleapis.com/' \
--form 'key="tmp/21759409/bulk/2d278b12-d153-4667-a05c-a5d8181623de/bulk_op_vars"' \
--form 'x-goog-credential="merchant-assets@shopify-tiers.iam.gserviceaccount.com/20220830/auto/storage/goog4_request"' \
--form 'x-goog-algorithm="GOOG4-RSA-SHA256"' \
--form 'x-goog-date="20220830T025127Z"' \
--form 'x-goog-signature="4c0f6920cd67cbdf1faae75c112a98d49f9751e4e0c9f525c850f15f40629afa13584ab9937ec9f5292065ca8fd357ba87e98d6ab0e383e15a6e444c7e9bae06fb95dc422ad673fe77aefcb68e9d1a6d55deb478e6976b61769e20863992fffd4036898f76c7a50e92f18aa4d9e3e04aa8d04086386dc0e488f2ccb0ebcc30c17da2ba5a4d6a9cd57553b41ef6698dbefc78a9b3fe1af167ea539b70e83e5fb015f061399e952270202b769ae8f4e7e50e97dbe6679c3281ec3fc886c3a67becc7b3cee1d0e6a2d0777d09f6d7b083499c58f9c566eeb5374afd67e26c7ab2a91cfe5c5deb83a507d7e3c3ea44bb9f401afd3f2e6b09742baff2b30bc3def78a"' \
--form 'policy="zyJjb25kaXRpb25zIjpbeyJDb250ZW50LVR5cGUiOiJ0ZXh0XC9qc29ubCJ9LHsic3VjY2Vzc19hY3Rpb25fc3RhdHVzIjoiMjAxIn0seyJhY2wiOiJwcml2YXRlIn0sWyJjb250ZW50LWxlbmd0aC1yYW5nZSIsMSwyMDk3MTUyMF0seyJidWNrZXQiOiJzaG9waWZ5LXN0YWdlZC11cGxvYWRzIn0seyJrZXkiOiJ0bXBcL2djc1wvMTQzMjMyMjEwNFwvYnVsa1wvM2QyNzhiMTItZDE1My00NjY3LWEwNWMtYTVkODE4MTYyM2RlXC9idWxrX29wX3ZhcnMifSx7IngtZ29vZy1kYXRlIjoiMjAyMjA4MzBUMDI1MTI3WiJ9LHsieC1nb29nLWNyZWRlbnRpYWwiOiJtZXJjaGFudC1hc3NldHNAc2hvcGlmeS10aWVycy5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbVwvMjAyMjA4MzBcL2F1dG9cL3N0b3JhZ2VcL2dvb2c0X3JlcXVlc3QifSx7IngtZ29vZy1hbGdvcml0aG0iOiJHT09HNC1SU0EtU0hBMjU2In1dLCJleHBpcmF0aW9uIjoiMjAyMi0wOC0zMVQwMjo1MToyN1oifQ=="' \
--form 'acl="private"' \
--form 'Content-Type="text/jsonl"' \
--form 'success_action_status="201"' \
--form 'file=@"/Users/username/Documents/bulk_mutation_tests/products_long.jsonl"'
```

**GraphQL variables in JSONL file**

```json
{ "input": { "title": "Sweet new snowboard 1", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 2", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 3", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 4", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 5", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 6", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 7", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 8", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 9", "productType": "Snowboard", "vendor": "JadedPixel" } }
{ "input": { "title": "Sweet new snowboard 10", "productType": "Snowboard", "vendor": "JadedPixel" } }
```

***

## Create a bulk mutation operation

After you upload the file, you can run [`bulkOperationRunMutation`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkoperationrunmutation) to import data in bulk. You must supply the corresponding mutation and the URL that you obtained in the [previous step](#generate-the-uploaded-url-and-parameters).

The [`bulkOperationRunMutation`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkoperationrunmutation) mutation takes the following arguments:

| Field | Type | Description |
| - | - | - |
| `mutation` | [string](https://shopify.dev/docs/api/admin-graphql/latest/scalars/String) | Specifies the GraphQL API mutation that you want to run in bulk. Valid values: [`productCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productcreate), [`collectionCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/collectioncreate), [`productUpdate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productupdate), [`productUpdateMedia`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productupdatemedia) |
| `stagedUploadPath` | [string](https://shopify.dev/docs/api/admin-graphql/latest/scalars/String) | The path to the file of inputs in JSONL format to be consumed by [`stagedUploadsCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/stageduploadscreate) |

### Example

In the following example, you want to run the following [`productCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/productcreate) mutation in bulk:

## POST https://{shop}.myshopify.com/admin/api/{api\_version}/graphql.json

## GraphQL mutation

```graphql
mutation call($input: ProductInput!) {
  productCreate(input: $input) {
    product {
      id
      title
      variants(first: 10) {
        edges {
          node {
            id
            title
            inventoryQuantity
          }
        }
      }
    }
    userErrors {
      message
      field
    }
  }
}
```

To run the `productCreate` mutation in bulk, pass the mutation as a string into [`bulkOperationRunMutation`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkOperationRunMutation):

## POST https://{shop}.myshopify.com/admin/api/{api\_version}/graphql.json

## GraphQL mutation

```graphql
mutation {
  bulkOperationRunMutation(
    mutation: "mutation call($input: ProductInput!) { productCreate(input: $input) { product {id title variants(first: 10) {edges {node {id title inventoryQuantity }}}} userErrors { message field } } }",
    stagedUploadPath: "tmp/21759409/bulk/89e620e1-0252-43b0-8f3b-3b7075ba4a23/bulk_op_vars") {
    bulkOperation {
      id
      url
      status
    }
    userErrors {
      message
      field
    }
  }
}
```

## JSON response

```json
{
  "data": {
    "bulkOperationRunMutation": {
      "bulkOperation": {
        "id": "gid://shopify/BulkOperation/206005076024",
        "url": null,
        "status": "CREATED"
      },
      "userErrors": []
    }
  },
  "extensions": {
    "cost": {
      "requestedQueryCost": 10,
      "actualQueryCost": 10
    }
  }
}
```

***

## Wait for the operation to finish

Tip

Subscribing to the webhook topic is recommended over polling as it limits the number of redundant API calls.

### Option A. Subscribe to the `bulk_operations/finish` webhook topic

Note

Using webhooks with bulk operations is only available in Admin API version 2021-10 and higher.

You can use the [`webhookSubscriptionCreate`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/webhooksubscriptioncreate) mutation to subscribe to the `bulk_operations/finish` webhook topic in order to receive a webhook when any operation finishes - in other words, it has completed, failed, or been cancelled.

For full setup instructions, refer to [Configuring webhooks](https://shopify.dev/docs/apps/build/webhooks/subscribe).

## POST https://{shop}.myshopify.com/admin/api/{api\_version}/graphql.json

## GraphQL mutation

```graphql
mutation {
  webhookSubscriptionCreate(
    topic: BULK_OPERATIONS_FINISH
    webhookSubscription: {
      format: JSON,
      uri: "https://12345.ngrok.io/"}
  ) {
    userErrors {
      field
      message
    }
    webhookSubscription {
      id
    }
  }
}
```

## JSON response

```json
{
  "data": {
    "webhookSubscriptionCreate": {
      "userErrors": [],
      "webhookSubscription": {
        "id": "gid://shopify/WebhookSubscription/4567"
      }
    }
  },
  "extensions": {
    "cost": {
      "requestedQueryCost": 10,
      "actualQueryCost": 10,
      "throttleStatus": {
        "maximumAvailable": 1000,
        "currentlyAvailable": 990,
        "restoreRate": 50
      }
    }
  }
}
```

After you've subscribed to the webhook topic, Shopify sends a POST request to the specified URL any time a bulk operation on the store (both mutations and [queries](https://shopify.dev/docs/api/usage/bulk-operations/queries)) finishes.

**Example webhook response**

```json
{
"admin_graphql_api_id": "gid://shopify/BulkOperation/206005076024",
"completed_at": "2024-01-28T19:11:09Z",
"created_at": "2024-01-28T19:10:59Z",
"error_code": null,
"status": "completed",
"type": "mutation",
}
```

You now must retrieve the bulk operation's data URL by using the `node` field and passing the `admin_graphql_api_id` value from the webhook payload as its `id`:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
query {
  node(id: "gid://shopify/BulkOperation/206005076024") {
    ... on BulkOperation {
      url
      partialDataUrl
    }
  }
}
```

## JSON response

```json
{
  "data": {
    "node": {
      "url": "https://storage.googleapis.com/shopify/dyfkl3g72empyyoenvmtidlm9o4g?<params />",
      "partialDataUrl": null
    }
  },
  "extensions": {
    "cost": {
      "requestedQueryCost": 1,
      "actualQueryCost": 1,
      "throttleStatus": {
        "maximumAvailable": 1000,
        "currentlyAvailable": 999,
        "restoreRate": 50
      }
    }
  }
}
```

For more information on how webhooks work, refer to [Webhooks](https://shopify.dev/docs/apps/build/webhooks).

Note

Webhook delivery isn't always guaranteed, so you might still need to poll for the operation's status to check when it's finished.

### Option B.​Poll the status of the operation

While the operation is running, you can poll to see its progress using the [`currentBulkOperation`](https://shopify.dev/docs/api/admin-graphql/latest/objects/queryroot) field. The `objectCount` field increments to indicate the operation's progress, and the `status` field returns whether the operation is completed.

You can adjust your polling intervals based on the amount of data that you import. To learn about other possible operation statuses, refer to the [`BulkOperationStatus`](https://shopify.dev/docs/api/admin-graphql/latest/enums/bulkoperationstatus) reference documentation.

To poll the status of the operation, use the following example request:

## POST https://{shop}.myshopify.com/admin/api/{api\_version}/graphql.json

## GraphQL query

```graphql
query {
 currentBulkOperation(type: MUTATION) {
    id
    status
    errorCode
    createdAt
    completedAt
    objectCount
    fileSize
    url
    partialDataUrl
 }
}
```

## JSON response

```json
{
  "data": {
    "currentBulkOperation": {
      "id": "gid://shopify/BulkOperation/206005076024",
      "status": "COMPLETED",
      "errorCode": null,
      "createdAt": "2024-01-28T19:10:59Z",
      "completedAt": "2024-01-28T19:11:09Z",
      "objectCount": "16",
      "fileSize": "6155",
      "url": "https://storage.googleapis.com/shopify-tiers-assets-prod-us-east1/iqtpj52yuoa7prkbpzp9gwn27kw3?GoogleAccessId=assets-us-prod%40shopify-tiers.iam.gserviceaccount.com&Expires=1612465869&Signature=KOhlcYhLve3NLr6rfVbAeY02crFAM3rMrDNfTSlgT%2FScI%2B8o%2B%2FdO99F3UseC837uWA6FzfrNhxdRNqhBN%2F8ekBTW7IyPRD6ho5phfE8MTaev4ltQrJygJTDbjXfX5KLJOuY8siH%2FDrc4gctZsMsNaf2%2FYp%2FaDzBzjfxJge8i8he69t0uZ39FBXrMxCeMVd6lU8%2FbgMuO80rTHjgI%2BlC8g2%2FWiHyq5rSTDLIxxGWRCddMfPcaivdWVdMubMa0wOt9W9R2mfjuTAgUBexUkJwhvrkdof%2Bg00gU1g4dIBWlUSO5D9tdrv9bmIy7FceopNufrpwnD1NXU8Narsx2yEQ6aA%3D%3D&response-content-disposition=attachment%3B+filename%3D%22bulk-206005076024.jsonl%22%3B+filename%2A%3DUTF-8%27%27bulk-206005076024.jsonl&response-content-type=application%2Fjsonl",
      "partialDataUrl": null
    }
  },
  "extensions": {
    "cost": {
      "requestedQueryCost": 1,
      "actualQueryCost": 1
    }
  }
}
```

***

## Retrieve the results

When a bulk mutation operation is finished, you can download a result data file.

If an operation successfully completes, then the `url` field contains a URL where you can download the data file. If an operation fails, but some data was retrieved before the failure occurred, then a partially complete data file is available at the URL specified in the `partialDataUrl` field.

In either case, the returned URLs are authenticated and expire after one week.

After you've downloaded the data, you can parse it according to the JSONL format. Since both input and response files are in JSONL, each line in the final asset file represents the response of running the mutation on the corresponding line in the input file.

### Operation success

The following example shows the response for a product that was successfully created:

## POST https://{shop}.myshopify.com/admin/api/{api\_version}/graphql.json

## JSON response

```json
{"data":{"productCreate":{"product":{"id":"gid:\/\/shopify\/Product\/5602345320504","title":"Monday morning snowboard 1","variants":{"edges":[{"node":{"id":"gid:\/\/shopify\/ProductVariant\/35645836853304","title":"First","inventoryQuantity":0}},{"node":{"id":"gid:\/\/shopify\/ProductVariant\/35645836886072","title":"Second","inventoryQuantity":0}}]}},"userErrors":[]}},"__lineNumber":0}
```

### Operation failures

Bulk operations can fail for any of the reasons that a regular GraphQL API mutation would fail, such as not having permission to access certain APIs. For this reason, the best approach is to run a single GraphQL mutation first to make sure that it works before running a mutation as part of a bulk operation.

If a bulk operation does fail, then its `status` field returns `FAILED` and the `errorCode` field returns a code such as one of the following:

* `ACCESS_DENIED`: There are missing access scopes. Run the mutation normally (outside of a bulk operation) to get more details on which field is causing the issue.
* `INTERNAL_SERVER_ERROR`: Something went wrong on Shopify's server and we've been notified of the error. These errors might be intermittent, so you can try making your request again.
* `TIMEOUT`: One or more mutation timeouts occurred during execution. Try removing some fields from your query so that it can run successfully. These timeouts might be intermittent, so you can try submitting the query again.

To learn about the other possible operation error codes, refer to the [`BulkOperationErrorCode`](https://shopify.dev/docs/api/admin-graphql/latest/enums/BulkOperationErrorCode) reference documentation.

Note

When using the `bulk_operations/finish` webhook, the `error_code` and `status` fields in the webhook payload will be lowercase. For example, `failed` instead of `FAILED`.

#### Validation error

If the input has the correct format, but one or more values failed the validation of the product creation service, then the response looks like the following:

```json
{"data"=>{"productCreate"=>{"userErrors"=>[{"message"=>"Some error message", "field"=>["some field"]}]}}}
```

#### Unrecognizable field error

If the input has an unrecognizable field, then the response looks like the following:

```json
{"errors"=>[{"message"=>"Variable input of type ProductInput! was provided invalid value for myfavoriteaddress (Field is not defined on ProductInput)", "locations"=>[{"line"=>1, "column"=>13}], "extensions"=>{"value"=>{"myfavoriteaddress"=>"test1"}, "problems"=>[{"path"=>["myfavoriteaddress"], "explanation"=>"Field is not defined on ProductInput"}]}}]}
```

Note

This check is executed by comparing the input against the [`productInput`](https://shopify.dev/docs/api/admin-graphql/latest/input-objects/productinput) object, which is specified as part of the mutation argument.

***

## Cancel an operation

To cancel an in-progress bulk operation, run the [`bulkOperationCancel`](https://shopify.dev/docs/api/admin-graphql/latest/mutations/bulkoperationcancel) mutation and supply the operation ID as an input variable:

## POST https://{shop}.myshopify.com/admin/api/{api\_version}/graphql.json

## GraphQL query

```graphql
mutation {
  bulkOperationCancel(id: "gid://shopify/BulkOperation/1") {
    bulkOperation {
      status
    }
    userErrors {
      field
      message
    }
  }
}
```

## JSON response

```json
{
  "data": {
    "bulkOperationCancel": {
      "id": "gid://shopify/BulkOperation/1",
      "bulkOperation": {
        "status": "COMPLETED"
      }
    }
  },
  "extensions": {
    "cost": {
      "requestedQueryCost": 1,
      "actualQueryCost": 1
    }
  }
}
```

***

## Next steps

* Consult our [reference documentation](https://shopify.dev/docs/api/admin-graphql/latest/objects/BulkOperation) to learn more about creating and managing bulk operations.
* Learn how to use bulk operations to [asynchronously fetch data in bulk](https://shopify.dev/docs/api/usage/bulk-operations/queries).

***

* [Requirements](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#requirements)
* [Limitations](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#limitations)
* [How bulk importing data works](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#how-bulk-importing-data-works)
* [Create a JSONL file and include Graph​QL variables](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#create-a-jsonl-file-and-include-graphql-variables)
* [Upload the file to Shopify](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#upload-the-file-to-shopify)
* [Create a bulk mutation operation](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#create-a-bulk-mutation-operation)
* [Wait for the operation to finish](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#wait-for-the-operation-to-finish)
* [Retrieve the results](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#retrieve-the-results)
* [Cancel an operation](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#cancel-an-operation)
* [Next steps](https://shopify.dev/docs/api/usage/bulk-operations/imports.md#next-steps)

---
title: Paginating results with GraphQL
description: >-
  With GraphQL, you can select which subset of results to retrieve from a
  connection by using cursor-based pagination.
api_name: usage
source_url:
  html: 'https://shopify.dev/docs/api/usage/pagination-graphql'
  md: 'https://shopify.dev/docs/api/usage/pagination-graphql.md'
---

ExpandOn this page

* [How it works](https://shopify.dev/docs/api/usage/pagination-graphql.md#how-it-works)
* [Forward pagination](https://shopify.dev/docs/api/usage/pagination-graphql.md#forward-pagination)
* [Backward pagination](https://shopify.dev/docs/api/usage/pagination-graphql.md#backward-pagination)
* [Connection edges](https://shopify.dev/docs/api/usage/pagination-graphql.md#connection-edges)
* [Search performance considerations](https://shopify.dev/docs/api/usage/pagination-graphql.md#search-performance-considerations)

# Paginating results with GraphQL

When you use a connection to retrieve a list of resources, you use arguments to specify the number of results to retrieve. You can select which set of results to retrieve from a connection by using cursor-based pagination.

Note

You can retrieve up to a maximum of 250 resources. If you need to paginate larger volumes of data, then you can [perform a bulk query operation](https://shopify.dev/docs/api/usage/bulk-operations/queries) using the GraphQL Admin API.

***

## How it works

Connections retrieve a list of nodes. A node is an object that has a [global ID](https://shopify.dev/docs/api/usage/gids) and is of a type that's defined by the schema, such as the `Order` type. For example, the `orders` connection finds all the `Order` nodes connected to the query root. The `nodes` field is similar to a for-loop because it retrieves the selected fields from each node in the connection.

To optimize performance and user experience, you can request only a certain number of nodes at a time. The batch of nodes that is returned is known as a page. The position of each node in the array is indicated by its cursor.

To retrieve the next page of nodes, you need to indicate the position of the node the page should start from. You can do so by providing a cursor. You can retrieve cursor information about the current page using [the `PageInfo` object](#the-pageinfo-object), and use that cursor value in a subsequent query by passing it in a [`after`](#forward-pagination) or [`before`](#backward-pagination) argument.

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
query {
  orders(first: 2) {
    nodes {
      id
      name
      createdAt
    }
  }
}
```

## JSON response

```json
{
	"data": {
		"orders": {
			"nodes": [
				{
					"id": "gid://shopify/Order/1",
					"name": "#1001",
					"createdAt": "2022-05-12T19:42:48Z"
				},
				{
					"id": "gid://shopify/Order/2",
					"name": "#1002",
					"createdAt": "2022-05-12T19:45:07Z"
				}
			]
		}
	}
}
```

Tip

You can also retrieve a list of nodes using [edges](#connection-edges).

### The `PageInfo` object

In the GraphQL Admin API, each connection returns a [`PageInfo`](https://shopify.dev/docs/api/admin-graphql/latest/objects/PageInfo) object that assists in cursor-based pagination. The `PageInfo` object is composed of the following fields:

| Field | Type | Description |
| - | - | - |
| `hasPreviousPage` | Boolean | Whether there are results in the connection before the current page. |
| `hasNextPage` | Boolean | Whether there are results in the connection after the current page. |
| `startCursor` | string | The cursor of the first node in the `nodes` list. |
| `endCursor` | string | The cursor of the last node in the `nodes` list. |

Note

The `PageInfo` object in the GraphQL Partner API is only composed of the `hasNextPage` and `hasPreviousPage` fields.

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
query {
  orders(first: 2) {
    nodes {
      id
      name
      createdAt
    }
    pageInfo {
      hasPreviousPage
      hasNextPage
      startCursor
      endCursor
    }
  }
}
```

## JSON response

```json
{
  "data": {
    "orders": {
      "nodes": [
        {
          "id": "gid://shopify/Order/1",
          "name": "#1001",
          "createdAt": "2022-05-12T19:42:48Z"
        },
        {
          "id": "gid://shopify/Order/2",
          "name": "#1002",
          "createdAt": "2022-05-12T19:45:07Z"
        }
      ]
      "pageInfo": {
        "hasPreviousPage": false,
        "hasNextPage": true,
        "startCursor": "eyJsYXN0X2lkIjoxNDIzOTgwNTI3NjM4LCJsYXN0X3ZhbHVlIjoiMjAyMC0wMS0yMCAxNDo0ODoxMS4wMDAwMDAifQ==",
        "endCursor": "eyJsYXN0X2lkIjoyMzIxMjM5MTQ2NTE4LCJsYXN0X3ZhbHVlIjoiMjAyMC0xMi0xNSAyMzowMDo0NS4wMDAwMDAifQ=="
      }
    }
  }
}
```

***

## Forward pagination

All connections in Shopify's APIs provide forward pagination. This is achieved with the following connection variables:

| Field | Type | Description |
| - | - | - |
| `first` | integer | The requested number of `nodes` for each page. |
| `after` | string | The cursor to retrieve `nodes` after in the connection. Typically, you should pass the `endCursor` of the previous page as `after`. |

### Examples

You can include the `PageInfo` fields in your queries to paginate your results. The following example includes the `hasNextPage` and `endCursor` fields, and uses query variables to pass the `endCursor` value as an argument:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
query ($numProducts: Int!, $cursor: String) {
  # The `$numProducts` variable is required and is used to specify the number of results to return. The `$cursor` variable isn't required. If the `$cursor` variable is omitted, then the `after` argument is ignored.
  products(first: $numProducts, after: $cursor) {
    nodes {
      title
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

## Variables

```json
{
  "numProducts": 3,
  "cursor": null
}
```

## JSON response

```json
{
  "data": {
    "products": {
      "nodes": [
        {
          "title": "Product 1 title"
        },
        {
          "title": "Product 2 title"
        },
        {
          "title": "Product 3 title"
        }
      ],
      "pageInfo": {
        // The response indicates that there's a next page and provides the cursor to use as an `after` input for the next page of nodes.
        "hasNextPage": true,
        "endCursor": "eyJsYXN0X2lkIjo3MDE3MjQ0MTY0MTUyLCJsYXN0X3ZhbHVlIjoiNzAxNzI0NDE2NDE1MiJ9"
      }
    }
  }
}
```

By using the same query with different variables, you can query for the next page:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
query ($numProducts: Int!, $cursor: String){
  products(first: $numProducts, after: $cursor) {
    nodes {
      title
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

## Variables

```json
{
  "numProducts": 3,
  "cursor": "eyJsYXN0X2lkIjo3MDE3MjQ0MTY0MTUyLCJsYXN0X3ZhbHVlIjoiNzAxNzI0NDE2NDE1MiJ9"
}
```

## JSON response

```json
{
  "data": {
    "products": {
      "nodes": [
        {
          "title": "Product 4 title"
        }
      ],
      "pageInfo": {
        // The response indicates that there's no next page. This is the last page of the connection.
        "hasNextPage": false,
        "endCursor": "eyJsYXN0X2lkIjo3MjE0Njc0MjgwNTA0LCJsYXN0X3ZhbHVlIjoiNzIxNDY3NDI4MDUwNCJ9"
      }
    }
  }
}
```

***

## Backward pagination

Some connections in Shopify's APIs also provide backward pagination. This is achieved with the following connection variables:

| Field | Type | Description |
| - | - | - |
| `last` | integer | The requested number of `nodes` for each page. |
| `before` | string | The cursor to retrieve `nodes` before in the connection. Typically, you should pass the `startCursor` of the previous page as `before`. |

### Examples

Similar to forward pagination, you can start at the end of the list of nodes, and then query in reverse page order to the beginning. The following example includes the `hasPreviousPage` and `startCursor` fields, and uses query variables to pass the `startCursor` value as an argument:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
query ($numProducts: Int!, $cursor: String){
  products(last: $numProducts, before: $cursor) {
    nodes {
      title
    }
    pageInfo {
      hasPreviousPage
      startCursor
    }
  }
}
```

## Variables

```json
{
  "numProducts": 3,
  "cursor": null
}
```

## JSON response

```json
{
  "data": {
    "products": {
      "nodes": [
        {
          "title": "Product 2 title"
        },
        {
          "title": "Product 3 title"
        },
        {
          "title": "Product 4 title"
        }
      ],
      "pageInfo": {
        "hasPreviousPage": true,
        "startCursor": "eyJsYXN0X2lkIjo3MDE3MjQ0MDk4NjE2LCJsYXN0X3ZhbHVlIjoiNzAxNzI0NDA5ODYxNiJ9"
      }
    }
  }
}
```

The `startCursor` field can also be used in the subsequent request as the input `before` to get the previous page:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
query ($numProducts: Int!, $cursor: String){
  products(last: $numProducts, before: $cursor) {
    nodes {
      title
    }
    pageInfo {
      hasPreviousPage
      startCursor
    }
  }
}
```

## Variables

```json
{
  "numProducts": 3,
  "cursor": "eyJsYXN0X2lkIjo3MDE3MjQ0MDk4NjE2LCJsYXN0X3ZhbHVlIjoiNzAxNzI0NDA5ODYxNiJ9"
}
```

## JSON response

```json
{
  "data": {
    "products": {
      "nodes": [
        {
          "title": "Product 1 title"
        }
      ],
      "pageInfo": {
        "hasPreviousPage": false,
        "startCursor": "eyJsYXN0X2lkIjo3MDE3MjQ0MDY1ODQ4LCJsYXN0X3ZhbHVlIjoiNzAxNzI0NDA2NTg0OCJ9"
      }
    }
  }
}
```

***

## Connection edges

In connections, an `Edge` type describes the connection between the node and its parent. In almost all cases, querying `nodes` and `pageInfo` is preferred to querying `edges`. However, if you want the `Edge` metadata, then you can query `edges` instead of `nodes`. Each `Edge` contains a minimum of that edge's cursor and the node.

### Example

The following query is equivalent to the [forward pagination query](https://shopify.dev/docs/api/usage/pagination-graphql#forward-pagination). However, it requests a cursor for every edge instead of only the `endCursor`:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

## GraphQL query

```graphql
query ($numProducts: Int!, $cursor: String){
  products(first: $numProducts, after: $cursor) {
    edges {
      cursor
      node {
        title
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
```

## Variables

```json
{
  "numProducts": 3,
  "cursor": null
}
```

## JSON response

```json
// The PageInfo `endCursor` and the last edge's `cursor` are the same. Also, the `edges[].node` list is the equivalent of the `nodes` list in the forward pagination query.
{
  "data": {
    "products": {
      "edges": [
        {
          "cursor": "eyJsYXN0X2lkIjo3MDE3MjQ0MDY1ODQ4LCJsYXN0X3ZhbHVlIjoiNzAxNzI0NDA2NTg0OCJ9",
          "node": {
            "title": "Product 1 title"
          }
        },
        {
          "cursor": "eyJsYXN0X2lkIjo3MDE3MjQ0MDk4NjE2LCJsYXN0X3ZhbHVlIjoiNzAxNzI0NDA5ODYxNiJ9",
          "node": {
            "title": "Product 2 title"
          }
        },
        {
          "cursor": "eyJsYXN0X2lkIjo3MDE3MjQ0MTY0MTUyLCJsYXN0X3ZhbHVlIjoiNzAxNzI0NDE2NDE1MiJ9",
          "node": {
            "title": "Product 3 title"
          }
        }
      ],
      "pageInfo": {
        "hasNextPage": true,
        "endCursor": "eyJsYXN0X2lkIjo3MDE3MjQ0MTY0MTUyLCJsYXN0X3ZhbHVlIjoiNzAxNzI0NDE2NDE1MiJ9"
      }
    }
  }
}
```

***

## Search performance considerations

Paginating resources using a [range search](https://shopify.dev/docs/api/usage/search-syntax#search-query-syntax) might timeout or return an error if the collection of resources is sufficiently large, and the search field is different from the specified (or default) sort key for the connection you are querying. If your query is slow or returns an error, then try specifying a sort key that matches the field used in the search. For example:

## POST https://{shop}.myshopify.com/api/{api\_version}/graphql.json

```graphql
{
  orders(first: 250, query: "created_at:>'2020-10-21'", sortKey: CREATED_AT) {
    edges {
      node {
        id
      }
    }
  }
}
```

***

* [How it works](https://shopify.dev/docs/api/usage/pagination-graphql.md#how-it-works)
* [Forward pagination](https://shopify.dev/docs/api/usage/pagination-graphql.md#forward-pagination)
* [Backward pagination](https://shopify.dev/docs/api/usage/pagination-graphql.md#backward-pagination)
* [Connection edges](https://shopify.dev/docs/api/usage/pagination-graphql.md#connection-edges)
* [Search performance considerations](https://shopify.dev/docs/api/usage/pagination-graphql.md#search-performance-considerations)
