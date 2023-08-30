WITH src
     AS (SELECT VISIT_ID AS VIS_ID,
                VISIT_KEY,
                parse_json(visit_custom_variables):conversionType::text as conversion_type_name
                ,parse_json(visit_custom_variables):conversionValue::number as conversion_amount
                ,parse_json(visit_custom_variables):conversionTimeWithMilliseconds::timestamp_ltz as conversion_timestamp
                ,LOWER(_TENANT_ID) AS _TENANT_ID,
                DATEDIFF(MILLISECONDS, VISIT_CREATED_AT_WITH_MILLISECONDS,CONVERSION_TIMESTAMP)/ 1000 AS CONVERSION_SECONDS
         FROM   DATAHUB.DFO_STRUCTURED.VISIT_EVENT c
         WHERE  c._TENANT_ID IS NOT NULL
                AND c._CREATED_TIMESTAMP IS NOT NULL
                AND c.EVENT_ID IS NOT NULL
                AND c.EVENT_OBJECT = 'VisitorEvent'
                AND c.VISIT_EVENT_TYPE IN ('Conversion')
                AND c.EVENT_OBJECT IS NOT NULL
                AND c.VISIT_ID IS NOT NULL
                QUALIFY RANK() OVER (PARTITION BY VISIT_ID, CONVERSION_TYPE_NAME, conversion_timestamp ORDER BY VISIT_CREATED_AT_WITH_MILLISECONDS ASC)=1),
     tgt
     AS (SELECT VISIT_ID,
                CONVERSION_TYPE_NAME,
                CONVERSION_TIMESTAMP,
                VISIT_KEY
         FROM   DATAHUB.DFO_REFINED.VISIT_CONVERSION_FACT
         )
SELECT VIS_ID as src_visitid,
       t.VISIT_ID AS trg_visitid,
       s.conversion_timestamp AS src_conversionTimeStamp,
       t.CONVERSION_TIMESTAMP AS trg_conversionTimeStamp,
       CASE
         WHEN src_conversionTimeStamp = trg_conversionTimeStamp THEN true
         ELSE false
       END AS flag
FROM   src s
       INNER JOIN tgt t
               ON s.VISIT_KEY = t.VISIT_KEY
               AND s.conversion_type_name = t.CONVERSION_TYPE_NAME;