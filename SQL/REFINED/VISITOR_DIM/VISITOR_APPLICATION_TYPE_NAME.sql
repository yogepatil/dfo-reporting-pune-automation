WITH src
     AS (SELECT DISTINCT(VISITOR_ID) AS VIS_ID,
                VISITOR_APPLICATION_TYPE,
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
                VISITOR_KEY,
                VISITOR_APPLICATION_TYPE_NAME
         FROM   DATAHUB.DFO_REFINED.VISITOR_DIM
         --WHERE  _TENANT_ID = '11eb505f-7844-7680-923b-0242ac110003'
         )
SELECT s.VIS_ID AS src_visitorid,
       t.VISITOR_ID AS trg_visitorid,
       s.VISITOR_APPLICATION_TYPE AS src_visAppType,
       t.VISITOR_APPLICATION_TYPE_NAME AS trg_visAppType,
       CASE
         WHEN src_visAppType = trg_visAppType THEN true
         ELSE false
       END AS flag
FROM   src s
       INNER JOIN tgt t
               ON s.VISITOR_KEY = t.VISITOR_KEY;