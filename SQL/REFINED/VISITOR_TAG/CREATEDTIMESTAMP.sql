WITH src
     AS (SELECT EVENT_ID
                ,VISITOR_ID
                ,VISITOR_KEY
                ,CREATED_AT_WITH_MILLISECONDS
                ,VISITOR_TAGS
                ,_TENANT_ID
         FROM   DATAHUB.DFO_STRUCTURED.VISIT_EVENT c
         WHERE  c._TENANT_ID IS NOT NULL
                AND c._CREATED_TIMESTAMP IS NOT NULL
                AND c.EVENT_ID IS NOT NULL
                AND c.EVENT_OBJECT IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY c.VISITOR_KEY ORDER BY c.EVENT_TYPE DESC, c.CREATED_AT_WITH_MILLISECONDS DESC)=1
                ),
     tgt
     AS (SELECT VISITOR_KEY,
                CREATED_TIMESTAMP
         FROM   DATAHUB.DFO_REFINED.VISITOR_TAG
         )
SELECT t.VISITOR_KEY AS trg_visitorkey,
       s.created_at_with_milliseconds AS src_createatwithmillisec,
       t.created_timestamp AS trg_createdTime,
       CASE
         WHEN src_createatwithmillisec = trg_createdTime THEN true
         ELSE false
       END AS flag
FROM   src s
       INNER JOIN tgt t
               ON s.VISITOR_KEY = t.VISITOR_KEY;