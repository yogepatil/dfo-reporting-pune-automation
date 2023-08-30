WITH src
     AS (SELECT * FROM DATAHUB.DFO_REFINED.VISIT_CONVERSION_FACT)
SELECT VISIT_ID, COUNT(*), false "FLAG"
FROM src
GROUP BY VISIT_ID HAVING COUNT(*) < 1;