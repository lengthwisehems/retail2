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
--   1. Get the brand list from dbo.lookup, which is tiny compared to
--      variant_metrics (every brand that has variant_metrics rows also has
--      lookup rows). DISTINCT over that small table is instant. (We do NOT do
--      SELECT DISTINCT brand FROM variant_metrics - that scans all ~24M rows.)
--   2. For each brand, seek its single latest row via ix_vm_brand_dt
--      (ORDER BY captured_datetime DESC -> backward index seek -> TOP 1) and
--      read imported_at from that row.
-- Total work = ~30 brands x one index seek each = seconds.
--
-- NOTES
--  * imported_at reported here is the imported_at OF the newest-captured row,
--    not a table-wide MAX(imported_at). For "did this brand's last scrape
--    import," they're the same - each import advances both timestamps together.
--    For a true independent MAX(imported_at), add a supporting index:
--        CREATE NONCLUSTERED INDEX ix_vm_brand_imported
--            ON dbo.variant_metrics (brand, imported_at DESC);
--    after which even the ORIGINAL query runs in seconds.
--  * CROSS APPLY drops any brand with zero variant_metrics rows. Swap to
--    OUTER APPLY if you want such brands listed with NULLs.
-- ═══════════════════════════════════════════════════════════════

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
FROM (SELECT DISTINCT brand FROM dbo.lookup) b
CROSS APPLY (
    SELECT TOP 1
        v.captured_datetime AS last_captured_datetime,
        v.imported_at       AS last_imported_at
    FROM dbo.variant_metrics AS v WITH (INDEX(ix_vm_brand_dt))
    WHERE v.brand = b.brand
    ORDER BY v.captured_datetime DESC          -- backward seek on ix_vm_brand_dt
) x
ORDER BY x.last_imported_at ASC;              -- oldest / most-concerning first
