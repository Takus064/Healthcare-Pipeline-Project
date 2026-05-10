-- 1. Khởi tạo bảng Silver nếu chưa có
--DROP TABLE IF EXISTS `fpt-fresher-495407.silver_dataset.encounters`;
CREATE TABLE IF NOT EXISTS `fpt-fresher-495407.silver_dataset.encounters` (
    encounter_id STRING,
    src_encounter_id STRING,
    patient_id STRING,
    dept_id STRING,
    provider_id STRING,
    encounter_type STRING,
    encounter_date DATE,
    procedure_code STRING,
    created_at TIMESTAMP,
    _is_quarantined BOOLEAN,
    _source_system STRING,
    _silver_updated_at TIMESTAMP
);
-- 2. Append-Only: Chèn các giao dịch mới chưa từng tồn tại
INSERT INTO `fpt-fresher-495407.silver_dataset.encounters`
SELECT CONCAT('HA-', CAST(encounterid AS STRING)) AS encounter_id,
    CAST(encounterid AS STRING) AS src_encounter_id,
    REPLACE(CAST(patientid AS STRING), 'HOSP1', 'HA-') AS patient_id,
    CONCAT('HA-', CAST(departmentid AS STRING)) AS dept_id,
    CONCAT('HA-', CAST(providerid AS STRING)) AS provider_id,
    CAST(encountertype AS STRING) AS encounter_type,
    -- Giả sử encounterdate cũng là Epoch milliseconds giống các bảng khác
    EXTRACT(
        DATE
        FROM TIMESTAMP_MILLIS(CAST(encounterdate AS INT64))
    ) AS encounter_date,
    CAST(procedurecode AS STRING) AS procedure_code,
    TIMESTAMP_MILLIS(CAST(inserteddate AS INT64)) AS created_at,
    CASE
        WHEN patientid IS NULL
        OR departmentid IS NULL
        OR providerid IS NULL
        OR encountertype IS NULL
        OR encounterdate IS NULL
        OR procedurecode IS NULL THEN TRUE
        ELSE FALSE
    END AS _is_quarantined,
    'hospital_a' AS _source_system,
    CURRENT_TIMESTAMP() AS _silver_updated_at
FROM `fpt-fresher-495407.bronze_dataset.ha_encounters` AS src
WHERE NOT EXISTS (
        SELECT 1
        FROM `fpt-fresher-495407.silver_dataset.encounters` AS tgt
        WHERE tgt.encounter_id = CONCAT('HA-', CAST(src.encounterid AS STRING))
            AND tgt._source_system = 'hospital_a'
    )
UNION ALL
SELECT CONCAT('HB-', CAST(encounterid AS STRING)) AS encounter_id,
    CAST(encounterid AS STRING) AS src_encounter_id,
    REPLACE(CAST(patientid AS STRING), 'HOSP2', 'HB-') AS patient_id,
    CONCAT('HB-', CAST(departmentid AS STRING)) AS dept_id,
    CONCAT('HB-', CAST(providerid AS STRING)) AS provider_id,
    CAST(encountertype AS STRING) AS encounter_type,
    EXTRACT(
        DATE
        FROM TIMESTAMP_MILLIS(CAST(encounterdate AS INT64))
    ) AS encounter_date,
    CAST(procedurecode AS STRING) AS procedure_code,
    TIMESTAMP_MILLIS(CAST(inserteddate AS INT64)) AS created_at,
    CASE
        WHEN patientid IS NULL
        OR departmentid IS NULL
        OR providerid IS NULL
        OR encountertype IS NULL
        OR encounterdate IS NULL
        OR procedurecode IS NULL THEN TRUE
        ELSE FALSE
    END AS _is_quarantined,
    'hospital_b' AS _source_system,
    CURRENT_TIMESTAMP() AS _silver_updated_at
FROM `fpt-fresher-495407.bronze_dataset.hb_encounters` AS src
WHERE NOT EXISTS (
        SELECT 1
        FROM `fpt-fresher-495407.silver_dataset.encounters` AS tgt
        WHERE tgt.encounter_id = CONCAT('HB-', CAST(src.encounterid AS STRING))
            AND tgt._source_system = 'hospital_b'
    );