WITH src
     AS (SELECT DISTINCT(VISIT_EVENT_ID) AS VIS_ID,
                EVENT_OBJECT,
                VISIT_ACTIVITY_KEY,
                VISIT_KEY,
                VISIT_ID,
                VISIT_VISITOR_KEY,
                VISITOR_ID,
                VISIT_CREATED_AT_WITH_MILLISECONDS,
                parse_json(VISIT_CUSTOM_VARIABLES):articleId::text as articleId
         FROM   DATAHUB.DFO_STRUCTURED.VISIT_EVENT c
         WHERE  c._TENANT_ID IS NOT NULL
                AND c._CREATED_TIMESTAMP IS NOT NULL
                AND c.EVENT_ID IS NOT NULL
                AND c.EVENT_OBJECT = 'VisitorEvent'
                AND c.VISIT_EVENT_TYPE IN ('KnowledgeBaseArticleClicked')
                AND c.VISIT_ACTIVITY_KEY IS NOT NULL
                AND c.EVENT_OBJECT IS NOT NULL
                AND c.VISIT_ID IS NOT NULL
                AND c.VISITOR_ID IS NOT NULL
                AND c.VISIT_CREATED_AT_WITH_MILLISECONDS IS NOT NULL
                AND c.VISIT_EVENT_TYPE IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY VIS_ID ORDER BY VISIT_CREATED_AT_WITH_MILLISECONDS DESC)=1
                ),
     tgt
     AS (SELECT VISITOR_ACTIVITY_ID,
                VISIT_ACTIVITY_KEY,
                VISIT_KEY,
                VISITOR_ID,
                KNOWLEDGE_BASE_ARTICLE_ID
         FROM   DATAHUB.DFO_REFINED.VISIT_ACTIVITY_FACT
         )
SELECT VIS_ID as src_visitEventid,
       t.VISITOR_ACTIVITY_ID AS trg_visitorActid,
       t.VISIT_KEY AS trg_visitKey,
       t.VISITOR_ID AS trg_visitorid,
       try_to_decimal(substr(s.articleId,1,38),38,0) AS src_articlid,
       t.KNOWLEDGE_BASE_ARTICLE_ID AS trg_knowBaseArticleid,
       CASE
         WHEN src_articlid IS NULL AND trg_knowBaseArticleid IS NULL THEN true
         WHEN src_articlid = trg_knowBaseArticleid THEN true
         ELSE false
       END AS flag
FROM   src s
       INNER JOIN tgt t
               ON s.VISIT_ACTIVITY_KEY = t.VISIT_ACTIVITY_KEY;