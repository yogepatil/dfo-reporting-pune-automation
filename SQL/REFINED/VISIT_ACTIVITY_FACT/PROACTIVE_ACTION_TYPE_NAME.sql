WITH src
     AS (SELECT DISTINCT(VISIT_EVENT_ID) AS VIS_ID,
                EVENT_OBJECT,
                VISIT_ACTIVITY_KEY,
                VISIT_KEY,
                VISIT_ID,
                VISIT_VISITOR_KEY,
                VISITOR_ID,
                VISIT_CREATED_AT_WITH_MILLISECONDS,
                parse_json(VISIT_CUSTOM_VARIABLES):actionType::text as actionType
         FROM   DATAHUB.DFO_STRUCTURED.VISIT_EVENT c
         WHERE  c._TENANT_ID IS NOT NULL
                AND c._CREATED_TIMESTAMP IS NOT NULL
                AND c.EVENT_ID IS NOT NULL
                AND c.EVENT_OBJECT = 'VisitorEvent'
                AND c.VISIT_EVENT_TYPE IN ('ProactiveActionDisplayed','ProactiveActionClicked', 'ProactiveActionSuccess', 'ProactiveActionFailed')
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
                PROACTIVE_ACTION_TYPE_NAME
         FROM   DATAHUB.DFO_REFINED.VISIT_ACTIVITY_FACT
         )
SELECT VIS_ID as src_visitEventid,
       t.VISITOR_ACTIVITY_ID AS trg_visitorActid,
       t.VISIT_KEY AS trg_visitKey,
       t.VISITOR_ID AS trg_visitorid,
       substr(s.actionType,1,35) AS src_actionType,
       t.PROACTIVE_ACTION_TYPE_NAME AS trg_proactiveActionType,
       CASE
         WHEN src_actionType IS NULL AND trg_proactiveActionType IS NULL THEN true
         WHEN src_actionType = trg_proactiveActionType THEN true
         ELSE false
       END AS flag
FROM   src s
       INNER JOIN tgt t
               ON s.VISIT_ACTIVITY_KEY = t.VISIT_ACTIVITY_KEY;