-- sap-archive-catalog-metadata-start
-- schema: SAPBI
-- tables: RSBREQUESTMESS (snapshot), RSBREQUESTDELTA (delta)
-- generated_at: 2026-03-13T00:00:00
-- sap-archive-catalog-metadata-end

-- Snapshot extractor for RSBREQUESTMESS using the ADSO delta table.
--
-- This outputs an Open Mirror–compatible incremental feed:
--   * __watermark: increasing watermark (delta request id)
--   * __op: operation type ("u" for upsert)
--   * __deleted: 1 when the row is missing from the snapshot (tombstone)
--
-- Watermark column: RSBREQUESTDELTA.REQUIDOUT (increasing per delta request)
-- Primary key in snapshot: (REQUID, POSIT)

SELECT
  CASE WHEN m."REQUID" IS NULL THEN 2 ELSE 4 END AS "__rowMarker__",
  d."REQUIDIN" AS "REQUID",
  m."POSIT",
  m."MSGSTATUS",
  m."RQRUN",
  m."MSGTY",
  m."MSGID",
  m."MSGNO",
  m."MSGV1",
  m."MSGV2",
  m."MSGV3",
  m."MSGV4",
  m."TSTMP",
  m."DETLEVEL"
FROM "SAPBI"."RSBREQUESTDELTA" AS d
LEFT JOIN "SAPBI"."RSBREQUESTMESS" AS m
  ON m."REQUID" = d."REQUIDIN"
WHERE d."REQUIDOUT" > {WATERMARK}
ORDER BY d."REQUIDOUT" ASC, d."REQUIDIN" ASC
