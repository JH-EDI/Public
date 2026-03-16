-- open-mirror: full-refresh
-- sap-archive-metadata-start
-- rows: 110464
-- elapsed_seconds: 7.607407
-- last_run_utc: 2026-02-26T17:21:22.223543Z
-- sap-archive-metadata-end

-- sap-archive-catalog-metadata-start
-- schema: SAPBI
-- table: /1SEM/UCS__M8007
-- row_count: 110464
-- generated_at: 2026-02-26T10:06:38.743433
-- sap-archive-catalog-metadata-end

-- Open Mirror format (full refresh):
--   __watermark: monotonically increasing value to represent snapshot version
--   __op: operation type ("u" for upsert in full refresh)
--   __deleted: 1 indicates a soft-deleted row (not used for full refresh)

SELECT
    CURRENT_TIMESTAMP AS "__watermark",
    'u' AS "__op",
    0 AS "__deleted",
    CAST("MANDT" AS NVARCHAR(3)) AS "MANDT",
    CAST("VERSION" AS INTEGER) AS "VERSION",
    CAST("/1FB/CURKEY_GC" AS INTEGER) AS "/1FB/CURKEY_GC",
    CAST("FISCYEAR" AS NVARCHAR(4)) AS "FISCYEAR",
    CAST("FISCPERIOD" AS NVARCHAR(3)) AS "FISCPERIOD",
    CAST("/1FB/CS_DIMEN" AS INTEGER) AS "/1FB/CS_DIMEN",
    CAST("/1FB/CS_UNIT" AS INTEGER) AS "/1FB/CS_UNIT",
    CAST("TASK" AS INTEGER) AS "TASK",
    CAST("NRLCH" AS NVARCHAR(2)) AS "NRLCH",
    CAST("DCHIND" AS NVARCHAR(1)) AS "DCHIND",
    CAST("STATUS" AS NVARCHAR(3)) AS "STATUS",
    CAST("ERROR" AS NVARCHAR(4)) AS "ERROR",
    CAST("WARNG" AS NVARCHAR(4)) AS "WARNG",
    CAST("CHUSER" AS NVARCHAR(12)) AS "CHUSER",
    CAST("CHDATE" AS NVARCHAR(8)) AS "CHDATE",
    CAST("CHTIME" AS NVARCHAR(6)) AS "CHTIME",
    CAST("STATWFID" AS NVARCHAR(22)) AS "STATWFID"
FROM "SAPBI"."/1SEM/UCS__M8007"