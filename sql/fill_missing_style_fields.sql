/*
================================================================================
 fill_missing_style_fields.sql

 Backfills blank style_id, style_name, style_name_grouping, sku_shopify, and
 barcode values across dbo.lookup, dbo.style_info, dbo.style_metrics,
 dbo.variant_metrics in denim_analytics, using an authoritative list of
 per-lookup_id fill values (e.g. a "Fill in ... missing values.xlsx" sheet
 with columns: lookup_id, ..., "If style_id is blank, fill with",
 "If style_name is blank, fill with", "If style_name_grouping is blank, fill
 with", "If sku_shopify is blank, fill with", "If barcode is blank, fill
 with").

 SCHEMA NOTES (denim_analytics):
   - dbo.lookup carries all five target fields directly, keyed by lookup_id.
     It is the master crosswalk and the only table addressed by lookup_id.
   - dbo.style_info / dbo.style_metrics carry style_id/style_name/
     style_name_grouping but have NO sku_brand or lookup_id column, so their
     target rows are resolved by joining the (now backfilled) dbo.lookup rows
     on brand+handle (falling back to brand+style_id, then brand+product_name).
   - dbo.variant_metrics carries sku_shopify/sku_brand/barcode but has NO
     style_id/style_name/style_name_grouping columns, and is resolved by
     joining dbo.lookup on brand+sku_brand.

 RULES:
   - Only fills columns that are currently NULL. Never overwrites a populated
     value (COALESCE(current, supplied)).
   - dbo.lookup is filled directly from the supplied values.
     dbo.style_info / dbo.style_metrics / dbo.variant_metrics are filled from
     the resulting dbo.lookup rows (never from the raw spreadsheet directly),
     so a single set of source values propagates consistently everywhere.
   - Every field actually changed is logged to dbo.manual_overrides for audit.
   - Runs inside a transaction and ROLLS BACK by default (@dry_run = 1).
     Review the printed summary counts, then re-run with @dry_run = 0 to
     commit.

 USAGE (SSMS / sqlcmd):
   1. Populate #fill_values in STEP 0 below (one row per lookup_id from your
      spreadsheet) -- e.g. via INSERT VALUES or BULK INSERT from a CSV export.
   2. Run the whole script once with @dry_run = 1 and review the row counts.
   3. Change @dry_run to 0 and re-run to commit.

 USAGE (fill_missing_style_fields.py):
   The Python wrapper reads the spreadsheet/CSV itself, populates
   #fill_values via parameterized inserts, and executes everything from the
   "-- === END STEP 0 ===" marker onward, so the VALUES list below is ignored
   in that mode.
================================================================================
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @dry_run BIT = 1;                          -- set to 0 to commit changes
DECLARE @run_by  VARCHAR(100) = SUSER_SNAME();
DECLARE @note    VARCHAR(255) = 'fill_missing_style_fields.sql backfill from supplied fill-value list';

-- ---------------------------------------------------------------------------
-- STEP 0: authoritative fill values, one row per dbo.lookup.lookup_id
-- ---------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#fill_values') IS NOT NULL DROP TABLE #fill_values;
CREATE TABLE #fill_values (
    lookup_id               INT PRIMARY KEY,
    new_style_id             VARCHAR(100) COLLATE DATABASE_DEFAULT NULL,
    new_style_name           VARCHAR(255) COLLATE DATABASE_DEFAULT NULL,
    new_style_name_grouping  VARCHAR(100) COLLATE DATABASE_DEFAULT NULL,
    new_sku_shopify          VARCHAR(100) COLLATE DATABASE_DEFAULT NULL,
    new_barcode              VARCHAR(100) COLLATE DATABASE_DEFAULT NULL
);

INSERT INTO #fill_values (lookup_id, new_style_id, new_style_name, new_style_name_grouping, new_sku_shopify, new_barcode) VALUES
    (698, '6957855047763', 'GOOD LEGS SKINNY JEANS', 'Good Legs Skinny', '42566611238995', '197217064540');
    -- one row per record from the fill-value spreadsheet

-- === END STEP 0 ===

IF NOT EXISTS (SELECT 1 FROM #fill_values)
BEGIN
    RAISERROR('No rows supplied in #fill_values.', 16, 1);
    RETURN;
END

BEGIN TRY
    BEGIN TRANSACTION;

    -- =========================================================================
    -- PART 1: dbo.lookup  (fill directly from the supplied values)
    -- =========================================================================
    IF OBJECT_ID('tempdb..#lookup_changes') IS NOT NULL DROP TABLE #lookup_changes;
    CREATE TABLE #lookup_changes (
        lookup_id            INT,
        old_style_id             VARCHAR(100),  new_style_id             VARCHAR(100),
        old_style_name           VARCHAR(255),  new_style_name           VARCHAR(255),
        old_style_name_grouping  VARCHAR(100),  new_style_name_grouping  VARCHAR(100),
        old_sku_shopify          VARCHAR(100),  new_sku_shopify          VARCHAR(100),
        old_barcode              VARCHAR(100),  new_barcode              VARCHAR(100)
    );

    -- dbo.lookup has a unique index on (brand, sku_shopify) [uq_lookup_sku].
    -- A supplied sku_shopify can collide with a value that ALREADY belongs to
    -- a different lookup_id (a pre-existing duplicate/stale row for the same
    -- variant under a different sku_brand). Detect those up front so the
    -- UPDATE can skip just that field instead of erroring out, and report
    -- them for manual reconciliation.
    IF OBJECT_ID('tempdb..#lookup_sku_conflicts') IS NOT NULL DROP TABLE #lookup_sku_conflicts;
    SELECT
        l.lookup_id,
        l.brand,
        l.sku_brand,
        f.new_sku_shopify           AS attempted_sku_shopify,
        other.lookup_id             AS conflicting_lookup_id,
        other.sku_brand             AS conflicting_sku_brand
    INTO #lookup_sku_conflicts
    FROM dbo.lookup AS l
    INNER JOIN #fill_values AS f ON f.lookup_id = l.lookup_id
    INNER JOIN dbo.lookup AS other
        ON other.brand = l.brand
       AND other.sku_shopify = f.new_sku_shopify
       AND other.lookup_id <> l.lookup_id
    WHERE l.sku_shopify IS NULL AND f.new_sku_shopify IS NOT NULL;

    UPDATE l
    SET
        style_id            = COALESCE(l.style_id, f.new_style_id),
        style_name           = COALESCE(l.style_name, f.new_style_name),
        style_name_grouping  = COALESCE(l.style_name_grouping, f.new_style_name_grouping),
        sku_shopify          = COALESCE(l.sku_shopify,
                                   CASE WHEN c.lookup_id IS NULL THEN f.new_sku_shopify END),
        barcode              = COALESCE(l.barcode, f.new_barcode)
    OUTPUT
        deleted.lookup_id,
        deleted.style_id,            inserted.style_id,
        deleted.style_name,          inserted.style_name,
        deleted.style_name_grouping, inserted.style_name_grouping,
        deleted.sku_shopify,         inserted.sku_shopify,
        deleted.barcode,             inserted.barcode
    INTO #lookup_changes
    FROM dbo.lookup AS l
    INNER JOIN #fill_values AS f ON f.lookup_id = l.lookup_id
    LEFT JOIN #lookup_sku_conflicts AS c ON c.lookup_id = l.lookup_id
    WHERE l.style_id IS NULL OR l.style_name IS NULL OR l.style_name_grouping IS NULL
       OR l.sku_shopify IS NULL OR l.barcode IS NULL;

    INSERT INTO dbo.manual_overrides (table_name, record_id, field_name, old_value, new_value, overridden_at, overridden_by, notes)
    SELECT 'lookup', lookup_id, 'style_id', old_style_id, new_style_id, GETDATE(), @run_by, @note FROM #lookup_changes WHERE old_style_id IS NULL AND new_style_id IS NOT NULL
    UNION ALL
    SELECT 'lookup', lookup_id, 'style_name', old_style_name, new_style_name, GETDATE(), @run_by, @note FROM #lookup_changes WHERE old_style_name IS NULL AND new_style_name IS NOT NULL
    UNION ALL
    SELECT 'lookup', lookup_id, 'style_name_grouping', old_style_name_grouping, new_style_name_grouping, GETDATE(), @run_by, @note FROM #lookup_changes WHERE old_style_name_grouping IS NULL AND new_style_name_grouping IS NOT NULL
    UNION ALL
    SELECT 'lookup', lookup_id, 'sku_shopify', old_sku_shopify, new_sku_shopify, GETDATE(), @run_by, @note FROM #lookup_changes WHERE old_sku_shopify IS NULL AND new_sku_shopify IS NOT NULL
    UNION ALL
    SELECT 'lookup', lookup_id, 'barcode', old_barcode, new_barcode, GETDATE(), @run_by, @note FROM #lookup_changes WHERE old_barcode IS NULL AND new_barcode IS NOT NULL;

    -- =========================================================================
    -- Bridge: resolve brand+handle/style_id/product_name/sku_brand for the
    -- affected lookup rows, from the (now backfilled) dbo.lookup.
    -- =========================================================================
    IF OBJECT_ID('tempdb..#target_styles') IS NOT NULL DROP TABLE #target_styles;
    SELECT DISTINCT l.brand, l.handle, l.style_id, l.product_name
    INTO #target_styles
    FROM dbo.lookup AS l
    INNER JOIN #fill_values AS f ON f.lookup_id = l.lookup_id
    WHERE l.handle IS NOT NULL OR l.style_id IS NOT NULL OR l.product_name IS NOT NULL;

    IF OBJECT_ID('tempdb..#target_sku_brands') IS NOT NULL DROP TABLE #target_sku_brands;
    SELECT DISTINCT l.brand, l.sku_brand
    INTO #target_sku_brands
    FROM dbo.lookup AS l
    INNER JOIN #fill_values AS f ON f.lookup_id = l.lookup_id
    WHERE l.sku_brand IS NOT NULL;

    -- =========================================================================
    -- PART 2: dbo.style_info
    -- =========================================================================
    IF OBJECT_ID('tempdb..#style_info_changes') IS NOT NULL DROP TABLE #style_info_changes;
    CREATE TABLE #style_info_changes (
        style_info_id            INT,
        old_style_id              VARCHAR(100), new_style_id              VARCHAR(100),
        old_style_name            VARCHAR(255), new_style_name            VARCHAR(255),
        old_style_name_grouping   VARCHAR(100), new_style_name_grouping   VARCHAR(100)
    );

    UPDATE si
    SET
        style_id            = COALESCE(si.style_id,
                                   (SELECT TOP 1 l.style_id FROM dbo.lookup l
                                    WHERE l.brand = si.brand AND (l.handle = si.handle OR l.style_id = si.style_id OR l.product_name = si.product_name)
                                      AND l.style_id IS NOT NULL ORDER BY l.lookup_id DESC)),
        style_name           = COALESCE(si.style_name,
                                   (SELECT TOP 1 l.style_name FROM dbo.lookup l
                                    WHERE l.brand = si.brand AND (l.handle = si.handle OR l.style_id = si.style_id OR l.product_name = si.product_name)
                                      AND l.style_name IS NOT NULL ORDER BY l.lookup_id DESC)),
        style_name_grouping  = COALESCE(si.style_name_grouping,
                                   (SELECT TOP 1 l.style_name_grouping FROM dbo.lookup l
                                    WHERE l.brand = si.brand AND (l.handle = si.handle OR l.style_id = si.style_id OR l.product_name = si.product_name)
                                      AND l.style_name_grouping IS NOT NULL ORDER BY l.lookup_id DESC))
    OUTPUT
        deleted.style_info_id,
        deleted.style_id,            inserted.style_id,
        deleted.style_name,          inserted.style_name,
        deleted.style_name_grouping, inserted.style_name_grouping
    INTO #style_info_changes
    FROM dbo.style_info AS si
    WHERE (si.style_id IS NULL OR si.style_name IS NULL OR si.style_name_grouping IS NULL)
      AND EXISTS (
          SELECT 1 FROM #target_styles ts
          WHERE ts.brand = si.brand
            AND ((ts.handle IS NOT NULL AND ts.handle = si.handle)
              OR (ts.style_id IS NOT NULL AND ts.style_id = si.style_id)
              OR (ts.product_name IS NOT NULL AND ts.product_name = si.product_name))
      );

    INSERT INTO dbo.manual_overrides (table_name, record_id, field_name, old_value, new_value, overridden_at, overridden_by, notes)
    SELECT 'style_info', style_info_id, 'style_id', old_style_id, new_style_id, GETDATE(), @run_by, @note FROM #style_info_changes WHERE old_style_id IS NULL AND new_style_id IS NOT NULL
    UNION ALL
    SELECT 'style_info', style_info_id, 'style_name', old_style_name, new_style_name, GETDATE(), @run_by, @note FROM #style_info_changes WHERE old_style_name IS NULL AND new_style_name IS NOT NULL
    UNION ALL
    SELECT 'style_info', style_info_id, 'style_name_grouping', old_style_name_grouping, new_style_name_grouping, GETDATE(), @run_by, @note FROM #style_info_changes WHERE old_style_name_grouping IS NULL AND new_style_name_grouping IS NOT NULL;

    -- =========================================================================
    -- PART 3: dbo.style_metrics  (same key pattern as style_info)
    -- =========================================================================
    IF OBJECT_ID('tempdb..#style_metrics_changes') IS NOT NULL DROP TABLE #style_metrics_changes;
    CREATE TABLE #style_metrics_changes (
        style_metric_id           INT,
        old_style_id              VARCHAR(100), new_style_id              VARCHAR(100),
        old_style_name            VARCHAR(255), new_style_name            VARCHAR(255),
        old_style_name_grouping   VARCHAR(100), new_style_name_grouping   VARCHAR(100)
    );

    UPDATE sm
    SET
        style_id            = COALESCE(sm.style_id,
                                   (SELECT TOP 1 l.style_id FROM dbo.lookup l
                                    WHERE l.brand = sm.brand AND (l.handle = sm.handle OR l.style_id = sm.style_id OR l.product_name = sm.product_name)
                                      AND l.style_id IS NOT NULL ORDER BY l.lookup_id DESC)),
        style_name           = COALESCE(sm.style_name,
                                   (SELECT TOP 1 l.style_name FROM dbo.lookup l
                                    WHERE l.brand = sm.brand AND (l.handle = sm.handle OR l.style_id = sm.style_id OR l.product_name = sm.product_name)
                                      AND l.style_name IS NOT NULL ORDER BY l.lookup_id DESC)),
        style_name_grouping  = COALESCE(sm.style_name_grouping,
                                   (SELECT TOP 1 l.style_name_grouping FROM dbo.lookup l
                                    WHERE l.brand = sm.brand AND (l.handle = sm.handle OR l.style_id = sm.style_id OR l.product_name = sm.product_name)
                                      AND l.style_name_grouping IS NOT NULL ORDER BY l.lookup_id DESC))
    OUTPUT
        deleted.style_metric_id,
        deleted.style_id,            inserted.style_id,
        deleted.style_name,          inserted.style_name,
        deleted.style_name_grouping, inserted.style_name_grouping
    INTO #style_metrics_changes
    FROM dbo.style_metrics AS sm
    WHERE (sm.style_id IS NULL OR sm.style_name IS NULL OR sm.style_name_grouping IS NULL)
      AND EXISTS (
          SELECT 1 FROM #target_styles ts
          WHERE ts.brand = sm.brand
            AND ((ts.handle IS NOT NULL AND ts.handle = sm.handle)
              OR (ts.style_id IS NOT NULL AND ts.style_id = sm.style_id)
              OR (ts.product_name IS NOT NULL AND ts.product_name = sm.product_name))
      );

    INSERT INTO dbo.manual_overrides (table_name, record_id, field_name, old_value, new_value, overridden_at, overridden_by, notes)
    SELECT 'style_metrics', style_metric_id, 'style_id', old_style_id, new_style_id, GETDATE(), @run_by, @note FROM #style_metrics_changes WHERE old_style_id IS NULL AND new_style_id IS NOT NULL
    UNION ALL
    SELECT 'style_metrics', style_metric_id, 'style_name', old_style_name, new_style_name, GETDATE(), @run_by, @note FROM #style_metrics_changes WHERE old_style_name IS NULL AND new_style_name IS NOT NULL
    UNION ALL
    SELECT 'style_metrics', style_metric_id, 'style_name_grouping', old_style_name_grouping, new_style_name_grouping, GETDATE(), @run_by, @note FROM #style_metrics_changes WHERE old_style_name_grouping IS NULL AND new_style_name_grouping IS NOT NULL;

    -- =========================================================================
    -- PART 4: dbo.variant_metrics  (brand+sku_brand bridges to dbo.lookup)
    -- =========================================================================
    IF OBJECT_ID('tempdb..#variant_metrics_changes') IS NOT NULL DROP TABLE #variant_metrics_changes;
    CREATE TABLE #variant_metrics_changes (
        variant_metric_id  INT,
        old_sku_shopify    VARCHAR(100), new_sku_shopify VARCHAR(100),
        old_barcode        VARCHAR(100), new_barcode     VARCHAR(100)
    );

    UPDATE vm
    SET
        sku_shopify = COALESCE(vm.sku_shopify,
                          (SELECT TOP 1 l.sku_shopify FROM dbo.lookup l
                           WHERE l.brand = vm.brand AND l.sku_brand = vm.sku_brand AND l.sku_shopify IS NOT NULL
                           ORDER BY l.lookup_id DESC)),
        barcode     = COALESCE(vm.barcode,
                          (SELECT TOP 1 l.barcode FROM dbo.lookup l
                           WHERE l.brand = vm.brand AND l.sku_brand = vm.sku_brand AND l.barcode IS NOT NULL
                           ORDER BY l.lookup_id DESC))
    OUTPUT
        deleted.variant_metric_id,
        deleted.sku_shopify, inserted.sku_shopify,
        deleted.barcode,     inserted.barcode
    INTO #variant_metrics_changes
    FROM dbo.variant_metrics AS vm
    INNER JOIN #target_sku_brands AS t ON t.brand = vm.brand AND t.sku_brand = vm.sku_brand
    WHERE vm.sku_shopify IS NULL OR vm.barcode IS NULL;

    INSERT INTO dbo.manual_overrides (table_name, record_id, field_name, old_value, new_value, overridden_at, overridden_by, notes)
    SELECT 'variant_metrics', variant_metric_id, 'sku_shopify', old_sku_shopify, new_sku_shopify, GETDATE(), @run_by, @note FROM #variant_metrics_changes WHERE old_sku_shopify IS NULL AND new_sku_shopify IS NOT NULL
    UNION ALL
    SELECT 'variant_metrics', variant_metric_id, 'barcode', old_barcode, new_barcode, GETDATE(), @run_by, @note FROM #variant_metrics_changes WHERE old_barcode IS NULL AND new_barcode IS NOT NULL;

    -- =========================================================================
    -- SUMMARY
    -- =========================================================================
    SELECT
        (SELECT COUNT(*) FROM #fill_values)               AS fill_values_supplied,
        (SELECT COUNT(*) FROM #lookup_changes)             AS lookup_rows_touched,
        (SELECT COUNT(*) FROM #style_info_changes)         AS style_info_rows_touched,
        (SELECT COUNT(*) FROM #style_metrics_changes)      AS style_metrics_rows_touched,
        (SELECT COUNT(*) FROM #variant_metrics_changes)    AS variant_metrics_rows_touched,
        (SELECT COUNT(*) FROM #lookup_sku_conflicts)       AS lookup_sku_shopify_conflicts_skipped,
        @dry_run                                           AS dry_run;

    -- Rows where sku_shopify was intentionally left blank because the
    -- supplied value already belongs to a different lookup_id for this
    -- brand. Investigate conflicting_lookup_id -- these usually indicate a
    -- stale/duplicate dbo.lookup row for the same real-world variant that
    -- should be merged or deleted by hand before re-running this script.
    IF EXISTS (SELECT 1 FROM #lookup_sku_conflicts)
        SELECT * FROM #lookup_sku_conflicts;

    IF @dry_run = 1
    BEGIN
        PRINT 'DRY RUN: rolling back. Review the counts above, set @dry_run = 0 to commit.';
        ROLLBACK TRANSACTION;
    END
    ELSE
    BEGIN
        PRINT 'Committing changes.';
        COMMIT TRANSACTION;
    END
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH
