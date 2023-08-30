WITH src AS (SELECT VISIT_CONVERSION_KEY,
                    VISIT_KEY,
                    VISIT_ID,
                    CONVERSION_TYPE_NAME,
                    CONVERSION_TIMESTAMP,
                    CONVERSION_AMOUNT,
                    CONVERSION_SECONDS
             FROM DATAHUB.DFO_REFINED.VISIT_CONVERSION_FACT
         MINUS
             SELECT VISIT_CONVERSION_KEY,
                    VISIT_KEY,
                    VISIT_ID,
                    CONVERSION_TYPE_NAME,
                    CONVERSION_TIMESTAMP,
                    CONVERSION_AMOUNT,
                    CONVERSION_SECONDS
         FROM DATAHUB.SUITE_REFINED.VISIT_CONVERSION_FACT_VIEW_V001)
SELECT src.*, FALSE flag FROM src;