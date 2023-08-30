WITH src
     AS (SELECT VISIT_KEY,
                VISIT_EVENT_TYPE,
                COUNT(VISIT_EVENT_TYPE) AS VE_COUNT
         FROM   DATAHUB.DFO_STRUCTURED.VISIT_EVENT c
         WHERE  c._TENANT_ID IS NOT NULL
                AND c._CREATED_TIMESTAMP IS NOT NULL
                AND c.EVENT_ID IS NOT NULL
                AND c.VISIT_EVENT_TYPE IN ('KnowledgeBaseArticleClicked')
                AND c.EVENT_OBJECT IS NOT NULL
                AND c.VISIT_ID IS NOT NULL
                AND c.VISITOR_ID IS NOT NULL
                AND c.CREATED_AT_WITH_MILLISECONDS IS NOT NULL
                AND c.VISIT_EVENT_TYPE IS NOT NULL
                GROUP BY VISIT_EVENT_TYPE, VISIT_KEY
                ),
     tgt
     AS (SELECT VISIT_KEY,
                KNOWLEDGEBASE_ARTICLE_CLICKED_COUNT
         FROM   DATAHUB.DFO_REFINED.VISIT_FACT
         )
SELECT s.VISIT_KEY as SRC_VISKEY,
       VE_COUNT AS SRC_VECOUNT,
       t.KNOWLEDGEBASE_ARTICLE_CLICKED_COUNT AS TRG_KNWB_ARTICLE_COUNT,
       t.VISIT_KEY AS TRG_VISIT_KEY,
       CASE
         WHEN SRC_VECOUNT IS NULL AND TRG_KNWB_ARTICLE_COUNT IS NULL THEN true
         WHEN SRC_VECOUNT = TRG_KNWB_ARTICLE_COUNT THEN true
         ELSE false
       END AS flag
FROM   SRC s
       INNER JOIN tgt t
               ON s.VISIT_KEY = t.VISIT_KEY;