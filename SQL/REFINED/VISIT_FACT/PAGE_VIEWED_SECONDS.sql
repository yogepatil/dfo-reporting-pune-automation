WITH src
     AS (SELECT VISIT_KEY,
                VISIT_EVENT_TYPE,
				(parse_json(visit_custom_variables):timeSpentOnPage::number) AS timeSpentOnPage
         FROM   DATAHUB.DFO_STRUCTURED.VISIT_EVENT c
         WHERE  c._TENANT_ID IS NOT NULL
                AND c._CREATED_TIMESTAMP IS NOT NULL
                AND c.EVENT_ID IS NOT NULL
                AND c.VISIT_EVENT_TYPE IN ('TimeSpentOnPage')
                AND c.EVENT_OBJECT IS NOT NULL
                AND c.VISIT_ID IS NOT NULL
                AND c.VISITOR_ID IS NOT NULL
                AND c.CREATED_AT_WITH_MILLISECONDS IS NOT NULL
                AND c.VISIT_EVENT_TYPE IS NOT NULL
                GROUP BY VISIT_EVENT_TYPE, VISIT_KEY, timeSpentOnPage
                ),
     tgt
     AS (SELECT VISIT_KEY,
                PAGE_VIEWED_SECONDS
         FROM   DATAHUB.DFO_REFINED.VISIT_FACT
         )
SELECT s.VISIT_KEY as SRC_VISKEY,
       timeSpentOnPage AS SRC_TIMESPENTONPAGE,
       t.PAGE_VIEWED_SECONDS AS TRG_PAGEVIEWEDSEC,
       t.VISIT_KEY AS TRG_VISIT_KEY,
       CASE
         WHEN SRC_TIMESPENTONPAGE IS NULL AND TRG_PAGEVIEWEDSEC IS NULL THEN true
         WHEN SRC_TIMESPENTONPAGE = TRG_PAGEVIEWEDSEC THEN true
         ELSE false
       END AS flag
FROM   SRC s
       INNER JOIN tgt t
               ON s.VISIT_KEY = t.VISIT_KEY;