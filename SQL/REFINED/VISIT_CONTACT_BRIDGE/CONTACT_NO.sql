WITH src
     AS (SELECT DISTINCT(VISIT_KEY) AS VIS_KEY,
                CONTACT_NO,
                VISIT_KEY,
                _CREATED_TIMESTAMP
         FROM   DATAHUB.DFO_STRUCTURED.VISIT_CONTACT c
         WHERE  c._TENANT_ID IS NOT NULL
                AND c._CREATED_TIMESTAMP IS NOT NULL
                AND c.VISIT_KEY IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY VIS_KEY ORDER BY _CREATED_TIMESTAMP DESC)=1
                ),
     tgt
     AS (SELECT VISIT_KEY,
                CONTACT_NO
         FROM   DATAHUB.DFO_REFINED.VISIT_CONTACT_BRIDGE
         )
SELECT VIS_KEY as src_visitKey,
       t.VISIT_KEY AS trg_visitKey,
       s.CONTACT_NO as src_contactNo,
       t.CONTACT_NO AS trg_contactNo,
       CASE
         -- WHEN src_contactNo IS NULL AND trg_contactNo IS NULL THEN true
         WHEN src_contactNo = trg_contactNo THEN true
         ELSE false
       END AS flag
FROM   src s
       INNER JOIN tgt t
               ON s.VISIT_KEY = t.VISIT_KEY;