-- Requirement 4: Department Performance Analytics
-- Tổng hợp theo Tháng (YYYY-MM)

CREATE OR REPLACE TABLE `fpt-fresher-495407.gold_dataset.department_analytics` AS
SELECT
    t.dept_id,
    d.dept_name,
    FORMAT_DATE('%Y-%m', t.service_date) AS report_month,
    t._source_system,
    
    COUNT(DISTINCT t.patient_id) AS patient_volume,
    COUNT(DISTINCT t.encounter_id) AS total_encounters,
    SUM(t.paid_amount) AS total_revenue

FROM `fpt-fresher-495407.silver_dataset.transactions` AS t

-- Department info
LEFT JOIN `fpt-fresher-495407.silver_dataset.departments` AS d
    ON t.dept_id = d.dept_id 
   AND t._source_system = d._source_system

GROUP BY
    t.dept_id,
    d.dept_name,
    report_month,
    t._source_system;
