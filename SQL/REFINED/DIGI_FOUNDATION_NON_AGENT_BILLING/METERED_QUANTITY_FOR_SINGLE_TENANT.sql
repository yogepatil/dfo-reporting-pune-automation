WITH src
         AS (SELECT DISTINCT(QUERY_DATE) AS Q_DATE,
                _TENANT_ID,
                SUM(PRODUCT_AMOUNT) AS PROD_AMOUNT
         FROM   DATAHUB.METERING_STRUCTURED.BILLING_EVENT c
         WHERE  c._TENANT_ID IS NOT NULL
                AND c._CREATED_TIMESTAMP IS NOT NULL
                AND c.EVENT_ID IS NOT NULL
                AND c.EVENT_TYPE IN ('Meter')
                AND c.PROD_TYPE_ID IN (636)
                AND c._TENANT_ID IN (99999)
                GROUP BY c.QUERY_DATE, c._TENANT_ID
                ORDER BY PROD_AMOUNT DESC
            ),
     tgt
     AS (SELECT USAGE_DATE_ID,
                PRODUCT_KEY,
                BUS_NO,
                METERED_QUANTITY
         FROM   DATAHUB.METERING_REFINED.METERED_DAILY_USAGE_FACT
         WHERE PRODUCT_KEY IN (287)
               AND BUS_NO IN (99999)
               ORDER BY METERED_QUANTITY DESC
         )
SELECT s._TENANT_ID AS src_tenantid,
       t.BUS_NO AS trg_busno,
       s.Q_DATE,
       t.USAGE_DATE_ID,
       s.PROD_AMOUNT AS src_prodamount,
       t.METERED_QUANTITY AS trg_meteredquantity,
       CASE
         WHEN src_prodamount = trg_meteredquantity THEN true
         ELSE false
       END AS flag
FROM   src s
       INNER JOIN tgt t
               ON s._TENANT_ID = t.BUS_NO AND
                  s.Q_DATE = t.USAGE_DATE_ID;