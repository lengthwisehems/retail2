-- Last_Import_By_Brand_fast.sql
-- ═══════════════════════════════════════════════════════════════
-- Same result as Last_Import_By_Brand.sql (most recent capture + import per
-- brand, with a staleness flag) but returns in SECONDS instead of ~20 minutes.
--
-- WHY THE ORIGINAL WAS SLOW
--   SELECT brand, MAX(captured_datetime), MAX(imported_at)
--   FROM variant_metrics GROUP BY brand;
-- variant_metrics holds ~24M rows across all brands. captured_datetime is
-- indexed (ix_vm_brand_dt = brand, captured_datetime), so MAX(captured_datetime)
-- alone would be instant. But imported_at is in NO index, so MAX(imported_at)
-- forces a full clustered scan of all ~24M wide rows -> ~20 minutes.
--
-- HOW THIS IS FAST
--   1. Walk the DISTINCT brands by seeking the index brand-to-brand (a
--      recursive "index skip scan" - ~30 tiny seeks, not a 24M-row scan).
--   2. For each brand, seek its single latest row via ix_vm_brand_dt
--      (ORDER BY captured_datetime DESC -> backward index seek -> TOP 1) and
--      read imported_at from that row.
-- Total work = ~30 brands x a couple of index seeks each = seconds.
--
-- NOTE ON imported_at: this reports the imported_at OF the newest-captured row
-- rather than a table-wide MAX(imported_at). For "did this brand's last scrape
-- import," those are the same thing - each import writes rows whose
-- captured_datetime and imported_at both advance together. If you ever need a
-- true independent MAX(imported_at), the clean fix is a supporting index:
--   CREATE NONCLUSTERED INDEX ix_vm_brand_imported
--       ON dbo.variant_metrics (brand, imported_at DESC);
-- after which the ORIGINAL query would also run in seconds.
-- ═══════════════════════════════════════════════════════════════

WITH brands AS (
    SELECT MIN(brand) AS brand FROM dbo.variant_metrics
    UNION ALL
    SELECT (SELECT MIN(v.brand) FROM dbo.variant_metrics v WHERE v.brand > brands.brand)
    FROM brands
    WHERE brands.brand IS NOT NULL
)
SELECT
    b.brand,
    x.last_captured_datetime,
    x.last_imported_at,
    DATEDIFF(HOUR, x.last_imported_at, GETUTCDATE())            AS hours_since_last_import,
    CASE
        WHEN DATEDIFF(HOUR, x.last_imported_at, GETUTCDATE()) > 36 THEN 'STALE - missed a cycle'
        WHEN DATEDIFF(HOUR, x.last_imported_at, GETUTCDATE()) > 15 THEN 'CHECK - later than expected'
        ELSE 'OK'
    END                                                         AS status
FROM brands b
CROSS APPLY (
    SELECT TOP 1
        v.captured_datetime AS last_captured_datetime,
        v.imported_at       AS last_imported_at
    FROM dbo.variant_metrics v
    WHERE v.brand = b.brand
    ORDER BY v.captured_datetime DESC          -- backward seek on ix_vm_brand_dt
) x
WHERE b.brand IS NOT NULL
ORDER BY x.last_imported_at ASC               -- oldest / most-concerning first
OPTION (MAXRECURSION 1000);
