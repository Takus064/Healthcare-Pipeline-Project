-- Requirement 1: Total Charge Amount per provider by department
-- Kết hợp thông tin Bác sĩ (SCD Type 2 Point-in-time) và Khoa (SCD Type 1)

CREATE OR REPLACE TABLE `fpt-fresher-495407.gold_dataset.provider_department_charge` AS
SELECT
    t.provider_id,
    p.first_name AS provider_first_name,
    p.last_name AS provider_last_name,
    p.specialization,
    t.dept_id,
    d.dept_name,
    t._source_system,
    
    -- Tính tổng số tiền khám/dịch vụ (charge amount)
    SUM(t.amount) AS total_charge_amount

FROM `fpt-fresher-495407.silver_dataset.transactions` AS t

-- SCD Type 1 Join cho bảng departments
LEFT JOIN `fpt-fresher-495407.silver_dataset.departments` AS d
    ON t.dept_id = d.dept_id 
   AND t._source_system = d._source_system

-- SCD Type 2 Join cho bảng providers (lấy bản ghi hiện tại)
LEFT JOIN `fpt-fresher-495407.silver_dataset.providers` AS p
    ON t.provider_id = p.provider_id 
   AND t._source_system = p._source_system
   AND p.is_current = TRUE

GROUP BY
    t.provider_id,
    p.first_name,
    p.last_name,
    p.specialization,
    t.dept_id,
    d.dept_name,
    t._source_system;
