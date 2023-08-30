WITH src
     AS (SELECT * FROM DATAHUB.METERING_REFINED.METERED_DAILY_USAGE_FACT )
SELECT _TENANT_ID, USAGE_DATE_ID, COUNT(*), false "FLAG"
FROM src
WHERE PRODUCT_KEY IN (286)
      AND BUS_NO IN (4160, 14337, -1472651971)
GROUP BY USAGE_DATE_ID, _TENANT_ID HAVING COUNT(*) > 1;