WITH src
     AS (SELECT * FROM DATAHUB.DFO_REFINED.VISIT_FACT)
SELECT VISIT_KEY, COUNT(*), false "FLAG"
FROM src
GROUP BY VISIT_KEY HAVING COUNT(*) > 1;