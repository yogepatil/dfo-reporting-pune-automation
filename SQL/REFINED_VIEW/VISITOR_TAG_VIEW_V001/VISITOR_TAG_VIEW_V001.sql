WITH src AS (SELECT VISITOR_KEY,
                    TAG_NO,
                    TAG_STATUS,
                    CREATED_TIMESTAMP,
                    ADDED_TIMESTAMP, REMOVED_TIMESTAMP
             FROM DATAHUB.DFO_REFINED.VISITOR_TAG
         MINUS
             SELECT VISITOR_KEY,
                    TAG_NO,
                    TAG_STATUS,
                    CREATED_TIMESTAMP,
                    ADDED_TIMESTAMP,
                    REMOVED_TIMESTAMP
         FROM DATAHUB.DFO_REFINED.VISITOR_TAG_VIEW_V001)
SELECT src.*, FALSE flag FROM src;