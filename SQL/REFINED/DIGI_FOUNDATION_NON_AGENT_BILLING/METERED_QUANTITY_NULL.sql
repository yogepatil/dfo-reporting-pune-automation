WITH src
     AS (SELECT * FROM DATAHUB.METERING_REFINED.METERED_DAILY_USAGE_FACT )
SELECT _TENANT_ID, USAGE_DATE_ID, COUNT(*), false "FLAG"
FROM src
WHERE PRODUCT_KEY IN (287)
      AND BUS_NO IN (99999)
      AND METERED_QUANTITY IS NULL
GROUP BY USAGE_DATE_ID, _TENANT_ID HAVING COUNT(*) > 1;