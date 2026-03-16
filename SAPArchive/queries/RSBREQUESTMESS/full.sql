-- Full snapshot extractor for RSBREQUESTMESS (no watermark filtering).
--
-- This generates a complete snapshot that can be used as the first Open Mirror
-- file. Subsequent runs will use delta.sql (watermark-based) to emit only
-- changes.

SELECT
  0 AS "__watermark",
  'u' AS "__op",
  0 AS "__deleted",
  m."REQUID",
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
FROM "SAPBI"."RSBREQUESTMESS" AS m
ORDER BY m."REQUID", m."POSIT"
