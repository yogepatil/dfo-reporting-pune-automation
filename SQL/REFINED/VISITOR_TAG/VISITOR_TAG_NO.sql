WITH sc AS
     (SELECT visitor_key ,
             tag_no ,
             c_tenant_id ,
             t_visitor_key ,
             t_tag_no ,
             t_tag_status ,
             c_visitor_key ,
             c_tag_no ,
             flag ,
             m_visitor_key
      FROM
        (SELECT src.visitor_key ,
                src.tag_no ,
                NVL(src.tenant_id, src.t_tenant_id) AS c_tenant_id ,
                src.t_visitor_key ,
                src.t_tag_no ,
                src.t_tag_status ,
                NVL(src.visitor_key, src.t_visitor_key) AS c_visitor_key ,
                NVL(src.tag_no, src.t_tag_no) AS c_tag_no ,
                CASE
                    WHEN src.t_visitor_key IS NULL
                         AND src.tag_no IS NOT NULL THEN true
                    WHEN src.t_visitor_key=src.visitor_key
                         AND src.tag_no=src.t_tag_no
                         AND src.t_tag_status='ACTIVE' THEN 'NO CHANGE'
                    WHEN (src.t_visitor_key=src.visitor_key
                          OR m_visitor_key IS NOT NULL)
                         AND src.tag_no IS NULL
                         AND t_tag_no IS NOT NULL
                         AND t_tag_status='ACTIVE' THEN 'UPDATE TAG TO INACTIVE'
                    WHEN src.t_visitor_key=src.visitor_key
                         AND src.tag_no IS NOT NULL
                         AND t_tag_status='INACTIVE' THEN 'UPDATE TAG TO ACTIVE'
                END AS flag,
                src.m_visitor_key
         FROM
           (SELECT visitor_key ,
                   tag_no ,
                   tenant_id ,
                   t_visitor_key ,
                   t_tag_no ,
                   t_tag_status ,
                   t_tenant_id ,
                   m_visitor_key
            FROM
              (SELECT ve.visitor_key AS visitor_key ,
                      f.value AS tag_no ,
                      _tenant_id AS tenant_id
               FROM
                 (SELECT visitor_id,
                         visitor_key,
                         event_id,
                         visitor_tags,
                         _tenant_id
                  FROM datahub.dforeporting_structured.visit_event --{track_changes_inserts_only}
                  QUALIFY ROW_NUMBER() OVER (PARTITION BY visitor_key
                  ORDER BY created_at_with_milliseconds DESC)=1) ve,
                    TABLE (flatten(INPUT=>visitor_tags))f
               WHERE _tenant_id IS NOT NULL
                 AND ve.visitor_key IS NOT NULL
                 AND ve.visitor_id IS NOT NULL
               UNION ALL SELECT ve.visitor_key ,
                                NULL AS tag_no ,
                                _tenant_id AS tenant_id
               FROM dforeporting_structured.visit_event ve
               WHERE array_size(visitor_tags)=0
                 AND visitor_key IN
                   (SELECT visitor_key
                    FROM datahub.dforeporting_structured.visit_event
                   )
                 AND _tenant_id IS NOT NULL
                 AND visitor_key IS NOT NULL
                 AND visitor_id IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY ve.visitor_key
                                                                       ORDER BY created_at_with_milliseconds DESC)=1 )EVENT_1
            FULL OUTER JOIN
              (SELECT visitor_key AS t_visitor_key ,
                      tag_no AS t_tag_no ,
                      tag_status AS t_tag_status ,
                      _tenant_id AS t_tenant_id
               FROM datahub.dforeporting_refined.visitor_tag
               WHERE _tenant_id IS NOT NULL
               GROUP BY t_visitor_key,
                        t_tag_no,
                        t_tag_status,
                        t_tenant_id) TAGS ON EVENT_1.visitor_key=TAGS.t_visitor_key
            AND EVENT_1.tag_no=TAGS.t_tag_no
            AND EVENT_1.tenant_id=TAGS.t_tenant_id
            LEFT OUTER JOIN
              (SELECT DISTINCT visitor_key AS m_visitor_key
               FROM datahub.dforeporting_structured.visit_event
              ) EVENT_2 ON EVENT_2.m_visitor_key=TAGS.t_visitor_key) src
         GROUP BY visitor_key,
                  tag_no,
                  c_tenant_id,
                  t_visitor_key,
                  t_tag_no,
                  t_tag_status,
                  c_visitor_key,
                  c_tag_no,
                  flag,
                  m_visitor_key)
      WHERE flag IS NOT NULL
        AND flag <> true) SELECT c_visitor_key AS visitor_key ,
                                        c_tag_no AS tag_no ,
                                        flag ,
                                        c_tenant_id AS _tenant_id ,
                                        m_visitor_key AS master_key
   FROM sc;