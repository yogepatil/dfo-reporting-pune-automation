WITH src
     AS (SELECT VISIT_KEY,
                VISIT_UTM_CONTENT_TEXT,
                _TENANT_ID,
                CREATED_AT_WITH_MILLISECONDS
         FROM   DATAHUB.DFO_STRUCTURED.VISIT_EVENT c
         WHERE  c._TENANT_ID IS NOT NULL
                AND c._CREATED_TIMESTAMP IS NOT NULL
                AND c.EVENT_ID IS NOT NULL
                AND c.EVENT_OBJECT IS NOT NULL
                AND c.VISIT_ID IS NOT NULL
                AND c.VISITOR_ID IS NOT NULL
                AND c.CREATED_AT_WITH_MILLISECONDS IS NOT NULL
                AND c.VISIT_EVENT_TYPE IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY VISIT_KEY ORDER BY CREATED_AT_WITH_MILLISECONDS DESC)=1
                ),
     tgt
     AS (SELECT VISIT_KEY,
                VISIT_UTM_CONTENT_TEXT,
                _TENANT_ID
         FROM   DATAHUB.DFO_REFINED.VISIT_FACT
         )
SELECT s.VISIT_UTM_CONTENT_TEXT AS src_visitUtmContText,
       t.VISIT_UTM_CONTENT_TEXT AS trg_visitUtmContText,
       CASE
         WHEN src_visitUtmContText = '' AND trg_visitUtmContText IS NULL THEN true
         WHEN src_visitUtmContText IS NULL AND trg_visitUtmContText IS NULL THEN true
         WHEN src_visitUtmContText = trg_visitUtmContText THEN true
         ELSE false
       END AS flag
FROM   src s
       INNER JOIN tgt t
               ON s.VISIT_KEY = t.VISIT_KEY
               AND s._TENANT_ID = t._TENANT_ID;