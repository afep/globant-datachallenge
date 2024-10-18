SELECT 
    d.department,
    j.job,
    COUNT(CASE WHEN DATE_PART('quarter', TO_TIMESTAMP(he.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 1 THEN 1 END) AS Q1,
    COUNT(CASE WHEN DATE_PART('quarter', TO_TIMESTAMP(he.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 2 THEN 1 END) AS Q2,
    COUNT(CASE WHEN DATE_PART('quarter', TO_TIMESTAMP(he.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 3 THEN 1 END) AS Q3,
    COUNT(CASE WHEN DATE_PART('quarter', TO_TIMESTAMP(he.datetime, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')) = 4 THEN 1 END) AS Q4
FROM 
    data_challenge.employees he
JOIN 
    data_challenge.departments d ON he.department_id = d.id
JOIN 
    data_challenge.jobs j ON he.job_id = j.id
WHERE 
    DATE_PART('year', he.datetime::timestamp) = 2021
GROUP BY 
    d.department, j.job
ORDER BY 
    d.department, j.job;

/*********************************************************************************************************/
SELECT 
    d.department,
    j.job,
    COUNT(*) FILTER (WHERE DATE_PART('quarter', he.datetime::timestamp) = 1) AS Q1,
    COUNT(*) FILTER (WHERE DATE_PART('quarter', he.datetime::timestamp) = 2) AS Q2,
    COUNT(*) FILTER (WHERE DATE_PART('quarter', he.datetime::timestamp) = 3) AS Q3,
    COUNT(*) FILTER (WHERE DATE_PART('quarter', he.datetime::timestamp) = 4) AS Q4
FROM 
    data_challenge.employees he
JOIN 
    data_challenge.departments d ON he.department_id = d.id
JOIN 
    data_challenge.jobs j ON he.job_id = j.id
WHERE 
    DATE_PART('year', he.datetime::timestamp) = 2021
GROUP BY 
    d.department, j.job
ORDER BY 
    d.department, j.job;


/*********************************************************************************************************/
WITH hires_per_department AS (
    SELECT 
        he.department_id,
        d.department,
        COUNT(he.id) AS hired
    FROM 
        data_challenge.employees he
    JOIN 
        data_challenge.departments d ON he.department_id = d.id
    WHERE 
        DATE_PART('year', he.datetime::timestamp) = 2021
    GROUP BY 
        he.department_id, d.department
),
mean_hires AS (
    SELECT AVG(hired) AS mean_hired FROM hires_per_department    
)
SELECT 
    hpd.department_id AS id,
    hpd.department,
    hpd.hired
FROM 
    hires_per_department hpd,
    mean_hires mh
WHERE 
    hpd.hired > mh.mean_hired
ORDER BY 
    hpd.hired DESC;