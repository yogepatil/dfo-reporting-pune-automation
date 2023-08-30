WITH src AS (SELECT VISITOR_KEY,
                    VISITOR_ID,
                    VISITOR_DEVICE_TYPE,
                    VISITOR_APPLICATION_TYPE_NAME,
                    VISITOR_BROWSER_NAME,
                    VISITOR_BROWSER_VERSION,
                    VISITOR_BROWSER_LANGUAGE
             FROM DATAHUB.DFO_REFINED.VISITOR_DIM
         MINUS
             SELECT VISITOR_KEY,
                    VISITOR_ID,
                    VISITOR_DEVICE_TYPE,
                    VISITOR_APPLICATION_TYPE_NAME,
                    VISITOR_BROWSER_NAME,
                    VISITOR_BROWSER_VERSION,
                    VISITOR_BROWSER_LANGUAGE
         FROM DATAHUB.DFO_REFINED.VISITOR_DIM_VIEW_V001)
SELECT src.*, FALSE flag FROM src;