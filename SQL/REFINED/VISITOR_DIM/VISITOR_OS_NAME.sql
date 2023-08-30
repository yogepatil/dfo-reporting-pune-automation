WITH src
     AS (SELECT DISTINCT(VISITOR_ID) AS VIS_ID,
                VISITOR_OS_NAME,
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
                VISITOR_OS_NAME,
                VISITOR_KEY,
                CREATED_TIMESTAMP
         FROM   DATAHUB.DFO_REFINED.VISITOR_DIM
         )
SELECT s.VIS_ID AS src_visitorid,
       t.VISITOR_ID AS trg_visitorid,
       s.VISITOR_OS_NAME AS src_vis_os_name,
       t.VISITOR_OS_NAME AS trg_vis_os_name,
       CASE
         WHEN src_vis_os_name = trg_vis_os_name THEN true
         ELSE false
       END AS flag
FROM   src s
       INNER JOIN tgt t
               ON s.VISITOR_KEY = t.VISITOR_KEY;