WITH src AS (SELECT VISIT_KEY,
                    CONTACT_KEY,
                    VISIT_ID,
                    CONTACT_ID,
                    CONTACT_NO,
                    INTERACTION_KEY,
                    INTERACTION_ID,
                    CUSTOMER_CONTACT_KEY,
                    CUSTOMER_CONTACT_ID,
                    _TENANT_ID,
                    _CREATED_TIMESTAMP,
                    _MODIFIED_TIMESTAMP
             FROM DATAHUB.DFO_REFINED.VISIT_CONTACT_BRIDGE
         MINUS
             SELECT VISIT_KEY,
                    CONTACT_KEY,
                    VISIT_ID,
                    CONTACT_ID,
                    CONTACT_NO,
                    INTERACTION_KEY,
                    INTERACTION_ID,
                    CUSTOMER_CONTACT_KEY,
                    CUSTOMER_CONTACT_ID,
                    _TENANT_ID,
                    _CREATED_TIMESTAMP,
                    _MODIFIED_TIMESTAMP
         FROM DATAHUB.SUITE_REFINED.VISIT_CONTACT_BRIDGE_VIEW_V001)
SELECT src.*, FALSE flag FROM src;