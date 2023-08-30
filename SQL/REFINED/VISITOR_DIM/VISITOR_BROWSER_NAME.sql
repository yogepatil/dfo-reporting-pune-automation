WITH src
     AS (SELECT DISTINCT(VISITOR_ID) AS VIS_ID,
                VISITOR_BROWSER_NAME,
                VISITOR_KEY
         FROM   DATAHUB.DFO_STRUCTURED.VISIT_EVENT c
         WHERE  c._TENANT_ID IS NOT NULL
                AND c._CREATED_TIMESTAMP IS NOT NULL
                AND c.EVENT_ID IS NOT NULL
                AND c.EVENT_OBJECT = 'Visitor'
                AND c.EVENT_TYPE IN ('VisitorCreated','VisitorUpdated')
                AND c.EVENT_OBJECT IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY c.VISITOR_KEY ORDER BY c.EVENT_TYPE DESC, c.CREATED_AT_WITH_MILLISECONDS DESC)=1),
     tgt
     AS (SELECT VISITOR_ID,
                VISITOR_BROWSER_NAME,
                VISITOR_KEY
         FROM   DATAHUB.DFO_REFINED.VISITOR_DIM
        --WHERE  _TENANT_ID = '11eb664d-c2b5-ee70-8733-0242ac110002'
         )
SELECT s.VIS_ID AS src_visitorid,
       t.VISITOR_ID AS trg_visitorid,
       s.VISITOR_BROWSER_NAME AS src_brows_name,
       t.VISITOR_BROWSER_NAME AS trg_brows_name,
       CASE
         WHEN src_brows_name = trg_brows_name THEN true
         ELSE false
       END AS flag
FROM   src s
       INNER JOIN tgt t
               ON s.VISITOR_KEY = t.VISITOR_KEY;