WITH src
     AS (SELECT DISTINCT(VISITOR_ID) AS VIS_ID,
                VISITOR_COUNTRY_NAME,
                VISITOR_KEY
         FROM   DATAHUB.DFO_STRUCTURED.VISIT_EVENT c
         WHERE  c._TENANT_ID IS NOT NULL
                AND c._CREATED_TIMESTAMP IS NOT NULL
                AND c.EVENT_ID IS NOT NULL
                AND c.EVENT_OBJECT = 'Visitor'
                AND (c.EVENT_TYPE = 'VisitorCreated' OR c.EVENT_TYPE = 'VisitorUpdated')
                AND c.EVENT_OBJECT IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY c.VISITOR_KEY ORDER BY c.EVENT_TYPE DESC, c.CREATED_AT_WITH_MILLISECONDS DESC)=1),
     tgt
     AS (SELECT VISITOR_ID,
                VISITOR_COUNTRY_NAME,
                VISITOR_KEY,
                CREATED_TIMESTAMP
         FROM   DATAHUB.DFO_REFINED.VISITOR_DIM
         )
SELECT s.VIS_ID AS src_visitorid,
       t.VISITOR_ID AS trg_visitorid,
       s.VISITOR_COUNTRY_NAME AS src_vis_country,
       t.VISITOR_COUNTRY_NAME AS trg_vis_country,
       CASE
         WHEN src_vis_country = trg_vis_country THEN true
         ELSE false
       END AS flag
FROM   src s
       INNER JOIN tgt t
               ON s.VISITOR_KEY = t.VISITOR_KEY;