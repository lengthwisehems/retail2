# ================================================================
# SKU HISTORY CHECK  -  variant_metrics + scraper files in blob storage
# sku_history_check.py
# ================================================================
# Paste a list of sku_brand values into SKU_LIST below, then run:
#
#       python sku_history_check.py
#
# RESULT 1  one row per pasted SKU, in the order you pasted them:
#   from the database (variant_metrics / lookup)
#       first_captured, last_captured, max_quantity_available,
#       snapshots_found, in_lookup, status
#   from the scraper CSV files in blob storage
#       first_source_file_name   first file the SKU appears in
#       last_source_file_name    last file the SKU appears in
#       source_files_found       how many files contain the SKU
#       max_quantity_in_files    highest Quantity Available in those files
#
# RESULT 2  only for SKUs NOT FOUND in variant_metrics: is their history
#           sitting under a different key (same sku_shopify or same
#           variant_title as the lookup row)?
#
# DAILY QUANTITY  one row per SKU per day it appears in the scraper files:
#       Date, First Quantity Available  (earliest file of that day)
#             Second Quantity Available (latest file of that day)
#
# Every tab also shows sku_shopify, style_id, sku_url, handle,
# product_title and variant_title, read from the latest scraper file the
# SKU appears in (if it is in no file: sku_shopify and variant_title from
# the lookup table).
#
# OUTPUT  one Excel workbook next to this script, one tab per result:
#       SKU_HISTORY_[BRAND]_YYYY_MM_DD.xlsx
#
# READ-ONLY. Nothing is changed in the database or in blob storage.
#
# NOTHING IS DOWNLOADED. For each of the brand's files the script asks
# Azure to search inside the file for the pasted SKUs, and Azure sends
# back only the matching rows (a few bytes per file). Expect roughly
# 1-3 minutes for a brand with ~700 files. Azure bills a small
# "query acceleration" fee per GB it scans (see the Azure Data Lake
# Storage pricing page).
#
# Needs: Python 3.8+, openpyxl for the Excel output, azure-storage-blob
# for the blob part and pymssql for the database part (the last two are
# already used by daily_denim_import.py).
#       pip install openpyxl
# ================================================================

# ================================================================
# SETTINGS - edit these
# ================================================================
BRAND = "GOODAMERICAN"          # as stored in the database "brand" column

SKU_LIST = """
GLST960-I1233-0
GLST960-I1233-00
GLST960-I1233-10
GLST960-I1233-12
GLST960-I1233-14
GLST960-I1233-15
GLST960-I1233-16
GLST960-I1233-18
GLST960-I1233-2
GLST960-I1233-20
GLST960-I1233-22
GLST960-I1233-24
GLST960-I1233-26
GLST960-I1233-28
GLST960-I1233-30
GLST960-I1233-32
GLST960-I1233-4
GLST960-I1233-6
GLST960-I1233-8
VSTA966F-CLPI-15
VSTA966F-CLPI-16
VSTA966F-CLPI-18
VSTA966F-CLPI-20
VSTA966F-CLPI-22
VSTA966F-CLPI-23W
VSTA966F-CLPI-24W
VSTA966F-CLPI-25W
VSTA966F-CLPI-26W
VSTA966F-CLPI-27W
VSTA966F-CLPI-28W
VSTA966F-CLPI-29W
VSTA966F-CLPI-30W
VSTA966F-CLPI-31W
VSTA966F-CLPI-32W

"""

# Container URL including a SAS token that has READ and LIST permission.
# (Don't upload this file to GitHub with the token in it.)
BLOB_SAS_URL = ""               # or set the BLOB_SAS_URL environment variable
BLOB_BASE_PATH = "outputs"      # same as daily_denim_import.py

# Database login. Leave SQL_PASSWORD blank to skip the database part and
# only show the blob-file columns.
SQL_SERVER   = "denim-sql.database.windows.net"
SQL_DATABASE = "denim_analytics"
SQL_USERNAME = "carrieboromisa"
SQL_PASSWORD = ""               # or set the SQL_PASSWORD environment variable

CHECK_OTHER_KEYS = True         # False = skip Result 2 (faster)
CHECK_BLOBS      = True         # False = database only
SEARCH_THREADS   = 8            # how many files Azure searches at the same time

# ================================================================
import os, re, io, csv, sys, time, datetime, threading
from concurrent.futures import ThreadPoolExecutor

# Environment variables win over the settings above, if they are set.
BLOB_SAS_URL = os.environ.get("BLOB_SAS_URL", BLOB_SAS_URL)
SQL_USERNAME = os.environ.get("SQL_USERNAME", SQL_USERNAME)
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", SQL_PASSWORD)

HERE = os.path.dirname(os.path.abspath(__file__))
csv.field_size_limit(2**31 - 1)


# ================================================================
# THE PASTED LIST
# ================================================================
def parse_sku_list(text):
    """One SKU per line. Blank lines, duplicates (ignoring upper/lower
    case), tabs and leading/trailing spaces are dropped. Spaces INSIDE
    a SKU are kept. Order of first appearance is preserved."""
    out, seen = [], set()
    for line in text.replace("\r", "").split("\n"):
        sku = line.replace("\t", " ").replace(" ", " ").strip()
        if not sku or sku.upper() in seen:
            continue
        seen.add(sku.upper())
        out.append(sku)
    return out


# ================================================================
# DATABASE PART  (same query as sku_history_check.sql)
# ================================================================
SQL_SCRIPT = r"""
SET NOCOUNT ON;
DECLARE @brand            VARCHAR(255)  = %s;
DECLARE @check_other_keys BIT           = %s;
DECLARE @sku_list         NVARCHAR(MAX) = %s;

DROP TABLE IF EXISTS #skus;
CREATE TABLE #skus (
    paste_order INT          NOT NULL,
    sku_brand   VARCHAR(255) COLLATE DATABASE_DEFAULT NOT NULL
);

INSERT INTO #skus (paste_order, sku_brand)
SELECT MIN(CAST(j.[key] AS INT)), x.sku
FROM OPENJSON(
         N'["'
       + REPLACE(STRING_ESCAPE(REPLACE(@sku_list, CHAR(13), N''), 'json'), N'\n', N'","')
       + N'"]'
     ) AS j
CROSS APPLY (
    SELECT CAST(LTRIM(RTRIM(REPLACE(REPLACE(j.value, CHAR(9), N' '), NCHAR(160), N' ')))
                AS VARCHAR(255)) AS sku
) AS x
WHERE x.sku <> ''
GROUP BY x.sku;

DROP TABLE IF EXISTS #result;

SELECT
    s.paste_order,
    s.sku_brand,
    MIN(vm.captured_datetime)   AS first_captured,
    MAX(vm.captured_datetime)   AS last_captured,
    MAX(vm.quantity_available)  AS max_quantity_available,
    COUNT(vm.captured_datetime) AS snapshots_found
INTO #result
FROM #skus AS s
LEFT JOIN variant_metrics AS vm
       ON vm.sku_brand = s.sku_brand
      AND (@brand IS NULL OR vm.brand = @brand)
GROUP BY s.paste_order, s.sku_brand
OPTION (RECOMPILE);

SELECT
    r.sku_brand,
    r.first_captured,
    r.last_captured,
    r.max_quantity_available,
    r.snapshots_found,
    CASE WHEN EXISTS (SELECT 1
                      FROM lookup AS l
                      WHERE l.sku_brand = r.sku_brand
                        AND (@brand IS NULL OR l.brand = @brand))
         THEN 'yes' ELSE 'NO' END                           AS in_lookup,
    CASE WHEN r.snapshots_found = 0
         THEN 'NOT FOUND in variant_metrics' ELSE 'found' END AS status,
    lk.sku_shopify                                         AS lookup_sku_shopify,
    lk.variant_title                                       AS lookup_variant_title
FROM #result AS r
OUTER APPLY (SELECT TOP 1 l.sku_shopify, l.variant_title
             FROM lookup AS l
             WHERE l.sku_brand = r.sku_brand
               AND (@brand IS NULL OR l.brand = @brand)) AS lk
ORDER BY r.paste_order
OPTION (RECOMPILE);

IF @check_other_keys = 1 AND EXISTS (SELECT 1 FROM #result WHERE snapshots_found = 0)
BEGIN
    DROP TABLE IF EXISTS #lk;

    SELECT DISTINCT
        r.paste_order,
        r.sku_brand,
        l.brand,
        l.sku_shopify,
        l.variant_title
    INTO #lk
    FROM #result AS r
    JOIN lookup  AS l
      ON l.sku_brand = r.sku_brand
     AND (@brand IS NULL OR l.brand = @brand)
    WHERE r.snapshots_found = 0
    OPTION (RECOMPILE);

    DROP TABLE IF EXISTS #alt;

    SELECT vm.brand, vm.sku_shopify, vm.variant_title, vm.sku_brand,
           vm.captured_datetime, vm.quantity_available
    INTO #alt
    FROM variant_metrics AS vm
    WHERE (@brand IS NULL OR vm.brand = @brand)
      AND (   EXISTS (SELECT 1 FROM #lk AS k
                      WHERE k.brand = vm.brand AND k.sku_shopify = vm.sku_shopify)
           OR EXISTS (SELECT 1 FROM #lk AS k
                      WHERE k.brand = vm.brand AND k.variant_title = vm.variant_title))
    OPTION (RECOMPILE);

    SELECT
        r.sku_brand,
        k.variant_title              AS lookup_variant_title,
        k.sku_shopify                AS lookup_sku_shopify,
        CASE WHEN k.sku_brand   IS NULL THEN 'not in lookup either'
             WHEN m.matched_on  IS NULL THEN 'no rows under sku_shopify or variant_title'
             ELSE m.matched_on END   AS matched_on,
        m.recorded_under_sku_brand,
        m.first_captured,
        m.last_captured,
        m.max_quantity_available,
        ISNULL(m.snapshots_found, 0) AS snapshots_found
    FROM #result AS r
    LEFT JOIN #lk AS k
           ON k.paste_order = r.paste_order
    OUTER APPLY (
        SELECT CASE WHEN MAX(x.by_sku) = 1 AND MAX(x.by_title) = 1
                         THEN 'same sku_shopify + variant_title'
                    WHEN MAX(x.by_sku) = 1 THEN 'same sku_shopify'
                    ELSE 'same variant_title' END AS matched_on,
               a.sku_brand               AS recorded_under_sku_brand,
               MIN(a.captured_datetime)  AS first_captured,
               MAX(a.captured_datetime)  AS last_captured,
               MAX(a.quantity_available) AS max_quantity_available,
               COUNT(*)                  AS snapshots_found
        FROM #alt AS a
        CROSS APPLY (
            SELECT CASE WHEN a.sku_shopify   = k.sku_shopify   THEN 1 ELSE 0 END AS by_sku,
                   CASE WHEN a.variant_title = k.variant_title THEN 1 ELSE 0 END AS by_title
        ) AS x
        WHERE a.brand = k.brand
          AND (x.by_sku = 1 OR x.by_title = 1)
        GROUP BY a.sku_brand
    ) AS m
    WHERE r.snapshots_found = 0
    ORDER BY r.paste_order, m.matched_on, m.recorded_under_sku_brand;
END
"""

R1_DB_COLS = ["sku_brand", "first_captured", "last_captured", "max_quantity_available",
              "snapshots_found", "in_lookup", "status"]
R2_COLS    = ["sku_brand", "lookup_variant_title", "lookup_sku_shopify", "matched_on",
              "recorded_under_sku_brand", "first_captured", "last_captured",
              "max_quantity_available", "snapshots_found"]

# Columns added to every tab, and their header names in the scraper CSV
# files (matched ignoring upper/lower case and spaces; first one found wins).
DETAIL_COLS    = ["sku_shopify", "style_id", "sku_url", "handle", "product_title", "variant_title"]
DETAIL_HEADERS = {
    "sku_shopify":   ["SKU - Shopify"],
    "style_id":      ["Style Id"],
    "sku_url":       ["SKU URL"],
    "handle":        ["Handle"],
    "product_title": ["Product Title", "Product"],
    "variant_title": ["Variant Title"],
}


def run_database(skus, brand, check_other_keys, connect=None):
    """Returns (result1_rows, result2_rows) as lists of dicts."""
    if connect is None:
        import pymssql
        connect = lambda: pymssql.connect(server=SQL_SERVER, user=SQL_USERNAME,
                                          password=SQL_PASSWORD, database=SQL_DATABASE,
                                          login_timeout=30, charset="UTF-8")
    conn = connect()
    try:
        cur = conn.cursor(as_dict=True)
        cur.execute(SQL_SCRIPT, (brand, 1 if check_other_keys else 0, "\n".join(skus)))
        sets = []
        while True:
            if cur.description:
                sets.append(cur.fetchall())
            if not cur.nextset():
                break
    finally:
        conn.close()
    r1 = sets[0] if sets else []
    r2 = sets[1] if len(sets) > 1 else []
    return r1, r2


# ================================================================
# BLOB PART
# ================================================================
FILENAME_RE = re.compile(r"^(.+?)_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})$")


def parse_filename(filename):
    """Same rule as daily_denim_import.py: BRAND_YYYY-MM-DD_HH-MM-SS.csv
    Returns (source_file_name, 'YYYY-MM-DD HH:MM:SS') or None."""
    base = re.sub(r"\.csv$", "", filename, flags=re.IGNORECASE)
    m = FILENAME_RE.match(base)
    if not m:
        return None
    try:
        dt = datetime.datetime.strptime(m.group(2) + " " + m.group(3).replace("-", ":"),
                                        "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return base, dt.strftime("%Y-%m-%d %H:%M:%S")


def clean_quantity(val):
    """Same cleaning as daily_denim_import.py. Blank / unreadable -> None."""
    if val is None:
        return None
    s = str(val).replace("$", "").replace(",", "").strip()
    if s == "":
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError, OverflowError):
        return None


SKU_COL, QTY_COL = "SKU - Brand", "Quantity Available"
MAX_QUERY_CHARS  = 200000       # Azure's limit for one query is 256 KB


def build_where(skus):
    """The search Azure runs inside each file. Matching ignores upper/lower
    case and spaces around the SKU, like the database does. A very long
    list is split into several queries to stay under Azure's size limit.
    Returns a list of 'FROM ... WHERE ...' strings."""
    keys = ["'" + k.upper().replace("'", "''") + "'" for k in skus]
    batches, cur, size = [], [], 0
    for k in keys:
        if cur and size + len(k) + 2 > MAX_QUERY_CHARS:
            batches.append(cur); cur, size = [], 0
        cur.append(k); size += len(k) + 2
    if cur:
        batches.append(cur)
    return [f'FROM BlobStorage WHERE TRIM(UPPER("{SKU_COL}")) IN ({", ".join(b)})' for b in batches]


def build_queries(skus):
    """Returns a list of (query_with_quantity, query_sku_only)."""
    return [(f'SELECT "{SKU_COL}", "{QTY_COL}" {w}', f'SELECT "{SKU_COL}" {w}') for w in build_where(skus)]


def norm_header(name):
    return re.sub(r"\s+", "", str(name or "")).lstrip("﻿").upper()


def parse_details(header, data):
    """Header row + full rows Azure sent back for one file ->
    {SKU_KEY: {detail column: value}}. First row per SKU wins."""
    pos = {norm_header(h): i for i, h in reversed(list(enumerate(header)))}
    sku_i = pos.get(norm_header(SKU_COL))
    if sku_i is None:
        return {}
    cols = {}
    for col, names in DETAIL_HEADERS.items():
        cols[col] = next((pos[norm_header(n)] for n in names if norm_header(n) in pos), None)
    out = {}
    for rec in csv.reader(io.StringIO(data.decode("utf-8-sig", "replace"), newline="")):
        if len(rec) <= sku_i or rec[sku_i].strip() == "":
            continue
        key = rec[sku_i].strip().upper()
        if key in out:
            continue
        out[key] = {c: (rec[i].strip() or None) if i is not None and i < len(rec) else None
                    for c, i in cols.items()}
    return out


def parse_hits(data):
    """Rows Azure sent back for one file -> {SKU_KEY: quantity}. If the same
    SKU is in the file twice, the first row wins."""
    hits = {}
    for rec in csv.reader(io.StringIO(data.decode("utf-8-sig", "replace"), newline="")):
        if not rec or rec[0].strip() == "":
            continue
        hits.setdefault(rec[0].strip().upper(), clean_quantity(rec[1]) if len(rec) > 1 else None)
    return hits


class BlobSearch:
    """Read-only access to one container through a SAS URL. Files are
    searched on Azure's side ("query blob contents"); they are never
    downloaded."""

    def __init__(self, sas_url):
        if "?" not in sas_url or not sas_url.lower().startswith("https://"):
            raise SystemExit("BLOB_SAS_URL is not set. Paste the container URL that ends "
                             "with the SAS token (it must include '?sp=rl...').")
        try:
            from azure.storage.blob import ContainerClient, DelimitedTextDialect
            from azure.core.exceptions import HttpResponseError
        except ImportError:
            raise SystemExit("The blob part needs the azure-storage-blob package:\n"
                             "    pip install azure-storage-blob")
        self.sas_url, self._ContainerClient, self.HttpResponseError = sas_url.strip(), ContainerClient, HttpResponseError
        self.fmt_in  = DelimitedTextDialect(delimiter=",", quotechar='"', lineterminator="\n",
                                            escapechar="", has_header=True)
        self.fmt_out = DelimitedTextDialect(delimiter=",", quotechar='"', lineterminator="\n",
                                            escapechar="", has_header=False)
        self._local = threading.local()

    def _container(self):                           # one client per thread
        if not hasattr(self._local, "client"):
            self._local.client = self._ContainerClient.from_container_url(self.sas_url)
        return self._local.client

    def _refused(self, e):
        code = getattr(e, "error_code", None) or getattr(e, "status_code", "")
        code = getattr(code, "value", code)
        hint = ("the SAS token needs both Read and List permission"
                if str(code) == "AuthorizationPermissionMismatch"
                else "the SAS token is wrong, expired or not active yet")
        return SystemExit(f"Blob storage refused the request ({code}): {hint}.")

    def brand_files(self, brand):
        """Every CSV in the brand's folder whose name follows the
        BRAND_YYYY-MM-DD_HH-MM-SS.csv pattern - the same files
        daily_denim_import.py would import. The folder is matched to the
        brand ignoring upper/lower case (outputs/GoodAmerican/...).
        Returns ([(blob_name, source_file_name, captured_datetime)], ignored)."""
        c = self._container()
        try:
            base = BLOB_BASE_PATH.strip("/") + "/"
            folders = [b.name for b in c.walk_blobs(name_starts_with=base, delimiter="/") if b.name.endswith("/")]
            mine = [f for f in folders if f.rstrip("/").split("/")[-1].upper() == brand.upper()]
            if not mine:
                names = ", ".join(sorted(f.rstrip("/").split("/")[-1] for f in folders))
                raise SystemExit(f"No folder for brand '{brand}' under {BLOB_BASE_PATH}/. Folders there: {names}")
            out, ignored = [], []
            for folder in mine:
                for b in c.list_blobs(name_starts_with=folder):
                    if not b.name.lower().endswith(".csv"):
                        continue
                    parsed = parse_filename(b.name.split("/")[-1])
                    if parsed is None:
                        ignored.append(b.name)
                    else:
                        out.append((b.name, parsed[0], parsed[1]))
        except self.HttpResponseError as e:
            if getattr(e, "status_code", None) in (401, 403):
                raise self._refused(e)
            raise
        out.sort(key=lambda x: (x[2], x[0]))
        return out, ignored

    def _query(self, blob_name, query):
        errors = []
        data = self._container().get_blob_client(blob_name).query_blob(
            query, blob_format=self.fmt_in, output_format=self.fmt_out, on_error=errors.append).readall()
        return data, errors

    def search_file(self, blob_name, queries, attempts=5):
        """Returns (hits, note). hits = {SKU_KEY: quantity} for the pasted SKUs
        found in this file; note = None, or why the file could not be
        searched properly (it is then reported, never silently skipped)."""
        hits, notes = {}, []
        for with_qty, sku_only in queries:
            query, last = with_qty, None
            for n in range(attempts):
                try:
                    data, errors = self._query(blob_name, query)
                    hits.update({k: v for k, v in parse_hits(data).items() if k not in hits})
                    if errors:
                        notes.append(f"Azure reported {len(errors)} read error(s): "
                                     f"{getattr(errors[0], 'description', errors[0])}"[:150])
                    last = None
                    break
                except self.HttpResponseError as e:
                    status, msg = getattr(e, "status_code", None), str(e).split("\n")[0]
                    if status in (401, 403):
                        raise self._refused(e)
                    if "does not exist in the header row" in msg:
                        if query is with_qty:       # no quantity column: still find the SKU, quantity stays blank
                            query, last = sku_only, msg
                            continue
                        return {}, f"no '{SKU_COL}' column"
                    if status is not None and 400 <= status < 500 and status not in (408, 429):
                        return {}, msg[:150]
                    last = msg
                except Exception as e:              # dropped connection etc. -> try again
                    last = str(e)
                time.sleep(2 * (n + 1))
            if last is not None:
                return {}, f"gave up after {attempts} attempts: {last}"[:150]
        return hits, ("; ".join(notes) or None)

    def _header(self, blob_name, chunk=65536, max_chunks=16):
        """Column names of one file. Reads only the start of the file, up to
        the end of its first line."""
        blob, buf = self._container().get_blob_client(blob_name), b""
        for _ in range(max_chunks):
            part = blob.download_blob(offset=len(buf), length=chunk).readall()
            buf += part
            if b"\n" in buf or len(part) < chunk:
                break
        line = buf.split(b"\n", 1)[0].decode("utf-8-sig", "replace")
        return next(csv.reader([line]), [])

    def details(self, blob_name, skus, attempts=3):
        """The detail columns (DETAIL_COLS) of the pasted SKUs in one file.
        Azure sends back only the matching rows. Returns (details, note)."""
        last = None
        for n in range(attempts):
            try:
                header = self._header(blob_name)
                out = {}
                for where in build_where(skus):
                    data, _ = self._query(blob_name, f"SELECT * {where}")
                    for k, v in parse_details(header, data).items():
                        out.setdefault(k, v)
                return out, None
            except self.HttpResponseError as e:
                if getattr(e, "status_code", None) in (401, 403):
                    raise self._refused(e)
                last = str(e).split("\n")[0]
            except Exception as e:
                last = str(e)
            time.sleep(2 * (n + 1))
        return {}, f"detail columns not read, gave up after {attempts} attempts: {last}"[:150]

    def search(self, brand, skus, log=print):
        """Returns ({SKU_KEY: blob columns}, status). status["files"] is the
        list of (blob_name, source_file_name, captured_datetime) searched and
        status["found"] is {SKU_KEY: [(captured_datetime, source_file_name,
        blob_name, quantity)]} - one entry per file the SKU is in."""
        files, ignored = self.brand_files(brand)
        folders = sorted({f[0].rsplit("/", 1)[0] for f in files})
        container = self.sas_url.split("?")[0].rstrip("/").split("/")[-1]
        log(f"  {len(files):,} {brand} files to search, in container '{container}', folder: " + ", ".join(folders))
        if ignored:
            log(f"  ({len(ignored)} file(s) ignored - name is not BRAND_YYYY-MM-DD_HH-MM-SS.csv)")
        queries = build_queries(skus)
        found = {k.upper(): [] for k in skus}       # SKU_KEY -> [(captured_dt, source_file_name, blob_name, qty)]
        problems, t0 = [], time.time()
        with ThreadPoolExecutor(max_workers=max(1, SEARCH_THREADS)) as pool:
            results = pool.map(lambda f: self.search_file(f[0], queries), files)
            for n, (f, (hits, note)) in enumerate(zip(files, results), 1):
                if note:
                    problems.append((f[1], note))
                for key, qty in hits.items():
                    if key in found:
                        found[key].append((f[2], f[1], f[0], qty))
                if n % 100 == 0 or n == len(files):
                    el = time.time() - t0
                    log(f"    {n:>5,}/{len(files):,} files searched   {el/60:4.1f} min elapsed, "
                        f"~{el/n*(len(files)-n)/60:4.1f} min left")
        out, latest = {}, {}                        # latest: blob_name -> SKUs last seen in it
        for key, rows in found.items():
            rows.sort(key=lambda r: (r[0], r[2]))
            qtys = [r[3] for r in rows if r[3] is not None]
            out[key] = {
                "first_source_file_name": rows[0][1] if rows else None,
                "last_source_file_name":  rows[-1][1] if rows else None,
                "source_files_found":     len(rows),
                "max_quantity_in_files":  max(qtys) if qtys else None,
            }
            out[key].update({c: None for c in DETAIL_COLS})
            if rows:
                latest.setdefault(rows[-1][2], []).append(key)

        if latest:
            log(f"  reading {', '.join(DETAIL_COLS)} from the latest file of each SKU "
                f"({len(latest)} file(s)) ...")
            with ThreadPoolExecutor(max_workers=max(1, SEARCH_THREADS)) as pool:
                results = pool.map(lambda b: self.details(b, latest[b]), list(latest))
                for blob_name, (det, note) in zip(list(latest), results):
                    if note:
                        problems.append((re.sub(r"\.csv$", "", blob_name.split("/")[-1], flags=re.IGNORECASE), note))
                    for key in latest[blob_name]:
                        out[key].update(det.get(key, {}))
        return out, {"files": files, "folders": folders, "problems": problems, "found": found}


R1_BLOB_COLS = ["first_source_file_name", "last_source_file_name", "source_files_found",
                "max_quantity_in_files"]


# ================================================================
# OUTPUT
# ================================================================
def fmt(v):
    if v is None:
        return ""
    if isinstance(v, datetime.datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v)


def log(msg=""):
    """Print a message with a time stamp in front. Leading blank lines stay
    blank."""
    blank = len(msg) - len(msg.lstrip("\n"))
    print("\n" * blank + datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ") + msg[blank:], flush=True)


def print_table(title, rows, cols):
    log(f"\n{title}")
    if not rows:
        print("  (no rows)")
        return
    cells  = [[fmt(r.get(c)) for c in cols] for r in rows]
    widths = [max(len(c), *(len(x[i]) for x in cells)) for i, c in enumerate(cols)]
    line   = lambda vals: "  " + "  ".join(v.ljust(w) for v, w in zip(vals, widths)).rstrip()
    print(line(cols))
    print(line(["-" * w for w in widths]))
    for x in cells:
        print(line(x))


DAILY_COLS = ["Date", "First Quantity Available", "Second Quantity Available"]


def build_daily(skus, files, found, base):
    """One row per SKU per day. First Quantity Available comes from the
    day's earliest file, Second Quantity Available from the day's latest
    file (blank if the day has only one file, or the SKU / its quantity is
    not in that file). Days on which the SKU is in neither file are left
    out; a SKU in no file at all gets one row with a blank Date.
    files = [(blob_name, source_file_name, captured_datetime)] sorted by time,
    found = {SKU_KEY: [(captured_datetime, source_file_name, blob_name, qty)]},
    base  = {SKU_KEY: row of columns to copy onto every row of that SKU}."""
    days = {}                                       # 'YYYY-MM-DD' -> [source_file_name, ...] earliest first
    for _, name, dt in files:
        days.setdefault(dt[:10], []).append(name)
    out = []
    for sku in skus:
        key = sku.upper()
        qty = {r[1]: r[3] for r in found.get(key, [])}
        rows = []
        for day in sorted(days):
            first = days[day][0]
            second = days[day][-1] if len(days[day]) > 1 else None
            if first not in qty and second not in qty:
                continue
            rows.append({"Date": datetime.datetime.strptime(day, "%Y-%m-%d").date(),
                         "First Quantity Available":  qty.get(first),
                         "Second Quantity Available": qty.get(second)})
        for r in rows or [{}]:
            out.append({**base.get(key, {"sku_brand": sku}), **r})
    return out


def excel_value(v):
    if isinstance(v, (datetime.date, int, float)) or v is None:
        return v
    if hasattr(v, "is_finite"):                     # Decimal from the database
        return int(v) if v == v.to_integral_value() else float(v)
    return str(v)


def write_xlsx(path, sheets):
    """sheets = [(tab_name, rows, cols)]. If the file is open in Excel the
    workbook is saved under a new name instead. Returns the path or None."""
    from openpyxl import Workbook
    from openpyxl.styles import Font
    from openpyxl.utils import get_column_letter
    from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

    wb = Workbook()
    wb.remove(wb.active)
    for name, rows, cols in sheets:
        ws = wb.create_sheet(name[:31])
        ws.append(cols)
        for cell in ws[1]:
            cell.font = Font(bold=True)
        widths = [len(c) for c in cols]
        for r in rows:
            vals = [excel_value(r.get(c)) for c in cols]
            vals = [ILLEGAL_CHARACTERS_RE.sub("", v) if isinstance(v, str) else v for v in vals]
            ws.append(vals)
            for i, (cell, v) in enumerate(zip(ws[ws.max_row], vals)):
                if isinstance(v, datetime.datetime):
                    cell.number_format = "yyyy-mm-dd hh:mm:ss"
                elif isinstance(v, datetime.date):
                    cell.number_format = "mm/dd/yyyy"
                elif isinstance(v, str) and v.startswith("="):
                    cell.data_type = "s"            # text, not a formula
                widths[i] = max(widths[i], 19 if isinstance(v, datetime.datetime) else len(fmt(v)))
        for i, w in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = min(w + 2, 60)
        ws.freeze_panes = "B2"
        ws.auto_filter.ref = ws.dimensions
    for attempt in (path, path[:-5] + "_" + datetime.datetime.now().strftime("%H%M%S") + ".xlsx"):
        try:
            wb.save(attempt)
            return attempt
        except PermissionError:                     # file is open in Excel
            continue
    return None


# ================================================================
# MAIN
# ================================================================
def main():
    try:
        sys.stdout.reconfigure(errors="replace")
    except Exception:
        pass
    skus = parse_sku_list(SKU_LIST)
    log("=" * 64)
    log(f"  SKU HISTORY CHECK   brand: {BRAND}   SKUs pasted: {len(skus)}")
    log("=" * 64)
    if not skus:
        log("SKU_LIST is empty - nothing to do.")
        return
    try:
        import openpyxl  # noqa: F401  (checked now, before the slow part)
    except ImportError:
        raise SystemExit("The Excel output needs the openpyxl package:\n    pip install openpyxl")

    # ---- database ------------------------------------------------
    db_rows, r2, db_done = {}, [], False
    if not SQL_PASSWORD:
        log("\nDatabase: SKIPPED (SQL_PASSWORD is blank) - showing blob-file columns only.")
    else:
        log("\nDatabase: querying variant_metrics / lookup ...")
        r1, r2 = run_database(skus, BRAND, CHECK_OTHER_KEYS)
        db_rows = {r["sku_brand"].upper(): r for r in r1}
        db_done = True

    # ---- blob files ----------------------------------------------
    blob_rows, blob_done = {}, False
    if CHECK_BLOBS:
        log("\nBlob storage: asking Azure to search the files (nothing is downloaded) ...")
        blob_rows, status = BlobSearch(BLOB_SAS_URL).search(BRAND, skus, log=log)
        blob_done = True
        if status["problems"]:
            log(f"\n  WARNING: {len(status['problems'])} file(s) could NOT be searched properly, so the "
                  f"results below may be missing them:")
            for name, note in status["problems"][:25]:
                log(f"    {name}: {note}")

    # ---- result 1 --------------------------------------------------
    result1, details = [], {}                       # details: SKU_KEY -> sku_brand + DETAIL_COLS
    for sku in skus:
        row = {"sku_brand": sku}
        d = db_rows.get(sku.upper())
        if db_done:
            if d is None:                           # should not happen; flag, don't guess
                row["status"] = "NO ROW RETURNED BY DATABASE"
            else:
                row.update({c: d.get(c) for c in R1_DB_COLS})
        if blob_done:
            row.update(blob_rows[sku.upper()])
        if d:                                       # SKU not in any file: fall back to the lookup table
            row["sku_shopify"]   = row.get("sku_shopify")   or d.get("lookup_sku_shopify")
            row["variant_title"] = row.get("variant_title") or d.get("lookup_variant_title")
        details[sku.upper()] = {c: row.get(c) for c in ["sku_brand"] + DETAIL_COLS}
        result1.append(row)

    r2_rows = [{**details.get(str(r["sku_brand"]).upper(), {}), **r} for r in r2]
    daily = []
    if blob_done:
        base = {k: {**v, **({c: db_rows.get(k, {}).get(c) for c in ("in_lookup", "status")} if db_done else {})}
                for k, v in details.items()}
        daily = build_daily(skus, status["files"], status["found"], base)

    if db_done:
        print_table("RESULT 1 - from the database (variant_metrics / lookup)", result1, R1_DB_COLS)
    if blob_done:
        print_table("RESULT 1 - from the scraper files in blob storage" if db_done
                    else "RESULT 1 - from the scraper files in blob storage (database part skipped)",
                    result1, ["sku_brand"] + R1_BLOB_COLS)
    if db_done and r2:
        print_table("RESULT 2 - NOT FOUND SKUs: rows in variant_metrics under another key", r2, R2_COLS)

    # ---- excel workbook --------------------------------------------
    first = ["sku_brand"] + DETAIL_COLS
    sheets = []
    if db_done:
        sheets.append(("Result 1 - Database", result1, first + R1_DB_COLS[1:]))
    if blob_done:
        sheets.append(("Result 1 - Blob Files", result1, first + R1_BLOB_COLS))
    if db_done and CHECK_OTHER_KEYS:
        sheets.append(("Result 2 - Other Keys", r2_rows, first + R2_COLS[1:]))
    if blob_done:
        sheets.append(("Daily Quantity", daily,
                       first + DAILY_COLS + (["in_lookup", "status"] if db_done else [])))
        if status["problems"]:
            sheets.append(("Files Not Searched", [{"source_file_name": n, "problem": p}
                                                  for n, p in status["problems"]],
                           ["source_file_name", "problem"]))
    if not sheets:
        log("\nNothing to save - both the database and blob parts were skipped.")
        return

    brand = re.sub(r'[\\/:*?"<>|\s]+', "_", BRAND.strip()).upper()
    name = f"SKU_HISTORY_{brand}_{datetime.date.today():%Y_%m_%d}.xlsx"
    path = write_xlsx(os.path.join(HERE, name), sheets)
    log(f"\nDaily Quantity tab: {len(daily):,} row(s)" if blob_done
          else "\nDaily Quantity tab: SKIPPED (needs the blob part, CHECK_BLOBS = False)")
    log(f"Saved ({len(sheets)} tabs): {path or 'could not save - is the file open in Excel?'}")


if __name__ == "__main__":
    main()