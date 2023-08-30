WITH src
     AS (SELECT VISIT_ID AS VIS_ID,
                VISIT_KEY,
                Parse_json(visit_custom_variables): conversionType :: text AS conversion_type_name,
                Parse_json(visit_custom_variables): conversionValue :: number AS conversion_amount,
                PARSE_JSON(VISIT_CUSTOM_VARIABLES): CONVERSIONTIMEWITHMILLISECONDS :: TIMESTAMP_LTZ AS CONVERSION_TIMESTAMP,
                LOWER(_TENANT_ID) AS _TENANT_ID,
                DATEDIFF(MILLISECONDS, VISIT_CREATED_AT_WITH_MILLISECONDS,CONVERSION_TIMESTAMP)/ 1000 AS CONVERSION_SECONDS
         FROM   DATAHUB.DFO_STRUCTURED.VISIT_EVENT c
         WHERE  c._TENANT_ID IS NOT NULL
                AND c._CREATED_TIMESTAMP IS NOT NULL
                AND c.EVENT_ID IS NOT NULL
                AND c.EVENT_OBJECT = 'VisitorEvent'
                AND c.VISIT_EVENT_TYPE IN ('Conversion')
                AND c.EVENT_OBJECT IS NOT NULL
                AND c.VISIT_ID IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY VISIT_ID ORDER BY VISIT_CREATED_AT_WITH_MILLISECONDS ASC)=1),
     tgt
     AS (SELECT VISIT_ID,
                CONVERSION_AMOUNT,
                VISIT_KEY
         FROM   DATAHUB.DFO_REFINED.VISIT_FACT
         )
SELECT VIS_ID as src_visitid,
       t.VISIT_ID AS trg_visitid,
       s.conversion_amount AS src_convAmount,
       t.CONVERSION_AMOUNT AS trg_convAmount,
       CASE
         WHEN src_convAmount = trg_convAmount THEN true
         ELSE false
       END AS flag
FROM   src s
       INNER JOIN tgt t
               ON s.VISIT_KEY = t.VISIT_KEY;