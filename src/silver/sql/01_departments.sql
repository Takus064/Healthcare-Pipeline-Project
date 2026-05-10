-- 1. Khởi tạo bảng Silver nếu chưa có
CREATE TABLE IF NOT EXISTS `fpt-fresher-495407.silver_dataset.departments` (
    dept_id STRING,
    src_dept_id STRING,
    dept_name STRING,
    _source_system STRING,
    _silver_updated_at TIMESTAMP,
    _is_quarantined BOOLEAN
);
TRUNCATE TABLE `fpt-fresher-495407.silver_dataset.departments`;
INSERT INTO `fpt-fresher-495407.silver_dataset.departments`
SELECT CASE
        WHEN _source_system = 'hospital_a' THEN CONCAT('HA-', deptid)
        ELSE CONCAT('HB-', deptid)
    END AS dept_id,
    deptid AS src_dept_id,
    name AS dept_name,
    _source_system,
    CURRENT_TIMESTAMP() AS _silver_updated_at,
    CASE
        WHEN deptid IS NULL
        OR name IS NULL THEN TRUE
        ELSE FALSE
    END AS _is_quarantined
FROM (
        SELECT DISTINCT deptid,
            name,
            'hospital_a' AS _source_system
        FROM `fpt-fresher-495407.bronze_dataset.ha_departments`
        UNION ALL
        SELECT DISTINCT deptid,
            name,
            'hospital_b' AS _source_system
        FROM `fpt-fresher-495407.bronze_dataset.hb_departments`
    ) --merge dữ liệu từ bronze vào silver
    /* MERGE INTO `fpt-fresher-495407.silver_dataset.departments` AS T
     USING (
     WITH hospital_a_rn AS (
     SELECT
     CAST(deptid AS STRING) AS dept_id,
     CAST(name AS STRING) AS dept_name,
     _source_system,
     ROW_NUMBER() OVER (
     PARTITION BY deptid, _source_system
     ORDER BY _bronze_loaded_at DESC
     ) AS rn
     FROM `fpt-fresher-495407.bronze_dataset.hospital_a_departments`
     ),
     hospital_b_rn AS (
     SELECT
     CAST(deptid AS STRING) AS dept_id,
     CAST(name AS STRING) AS dept_name,
     _source_system,
     ROW_NUMBER() OVER (
     PARTITION BY deptid, _source_system
     ORDER BY _bronze_loaded_at DESC
     ) AS rn
     FROM `fpt-fresher-495407.bronze_dataset.hospital_b_departments`
     ),
     source_data AS (
     SELECT dept_id, dept_name, _source_system
     FROM hospital_a_rn
     WHERE rn = 1
     
     UNION ALL
     
     SELECT dept_id, dept_name, _source_system
     FROM hospital_b_rn
     WHERE rn = 1
     )
     SELECT
     dept_id,
     dept_name,
     _source_system
     FROM source_data
     ) AS S
     ON T.dept_id = S.dept_id
     AND T._source_system = S._source_system
     
     WHEN MATCHED THEN
     UPDATE SET 
     dept_name = S.dept_name,
     _silver_updated_at = CURRENT_TIMESTAMP()
     
     WHEN NOT MATCHED THEN
     INSERT (
     dept_id,
     dept_name,
     _source_system,
     _silver_updated_at
     )
     VALUES (
     S.dept_id,
     S.dept_name,
     S._source_system,
     CURRENT_TIMESTAMP()
     );
     */