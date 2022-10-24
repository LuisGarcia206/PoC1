--1:
--  with between:
SELECT d.department,e.job,
       COUNT(DISTINCT CASE WHEN CAST(SUBSTR(q.datetime,6,2) AS INT64) BETWEEN 1 AND 3 THEN q.id END) AS Q1,
       COUNT(DISTINCT CASE WHEN CAST(SUBSTR(q.datetime,6,2) AS INT64) BETWEEN 4 AND 6 THEN q.id END) AS Q2,
       COUNT(DISTINCT CASE WHEN CAST(SUBSTR(q.datetime,6,2) AS INT64) BETWEEN 7 AND 9 THEN q.id END) AS Q3,
       COUNT(DISTINCT CASE WHEN CAST(SUBSTR(q.datetime,6,2) AS INT64) BETWEEN 10 AND 12 THEN q.id END) AS Q4
  FROM db1_poc1.hired_employees q
  LEFT JOIN db1_poc1.departments d
    ON d.id = q.department_id
  LEFT JOIN db1_poc1.jobs e
    ON e.id = q.job_id
 WHERE SUBsTR(q.datetime,1,4) = '2021'
   AND q.department_id IS NOT NULL
   AND q.job_id IS NOT NULL
 GROUP BY d.department,
          e.job
 ORDER BY 1,2;
--  with ceil:
SELECT d.department,e.job,
       COUNT(DISTINCT CASE WHEN CEIL(CAST(SUBSTR(q.datetime,6,2) AS INT64)/3) = 1 THEN q.id END) AS Q1,
       COUNT(DISTINCT CASE WHEN CEIL(CAST(SUBSTR(q.datetime,6,2) AS INT64)/3) = 2 THEN q.id END) AS Q2,
       COUNT(DISTINCT CASE WHEN CEIL(CAST(SUBSTR(q.datetime,6,2) AS INT64)/3) = 3 THEN q.id END) AS Q3,
       COUNT(DISTINCT CASE WHEN CEIL(CAST(SUBSTR(q.datetime,6,2) AS INT64)/3) = 4 THEN q.id END) AS Q4
  FROM db1_poc1.hired_employees q
  LEFT JOIN db1_poc1.departments d
    ON d.id = q.department_id
  LEFT JOIN db1_poc1.jobs e
    ON e.id = q.job_id
 WHERE SUBSTR(q.datetime,1,4) = '2021'
   AND q.department_id IS NOT NULL
   AND q.job_id IS NOT NULL
 GROUP BY d.department,
          e.job
 ORDER BY 1,2;

--2

SELECT w.id,w.department,w.hired
  FROM (SELECT d.id,
               d.department,
               COUNT(DISTINCT q.id) hired,
               AVG(COUNT(DISTINCT q.id)) OVER() avg_hired
          FROM db1_poc1.hired_employees q
          LEFT JOIN db1_poc1.departments d
            ON d.id = q.department_id
         WHERE SUBSTR(q.datetime,1,4) = '2021'
           AND q.department_id IS NOT NULL
         GROUP BY d.id,
                  d.department) w
 WHERE w.hired > w.avg_hired
 ORDER BY 3 DESC;