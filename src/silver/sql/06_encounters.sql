-- 1. Khởi tạo bảng Silver nếu chưa có
CREATE TABLE IF NOT EXISTS `fpt-fresher-495407.silver_dataset.encounters` (
    encounter_id STRING,
    patient_id STRING,
    dept_id STRING,
    provider_id STRING,
    encounter_type STRING,
    encounter_date DATE,
    procedure_code STRING,
    created_at TIMESTAMP,
    _source_system STRING,
    _silver_updated_at TIMESTAMP
);

-- 2. Append-Only: Chèn các giao dịch mới chưa từng tồn tại
INSERT INTO `fpt-fresher-495407.silver_dataset.encounters`
SELECT
    CAST(encounterid AS STRING) AS encounter_id,
    CAST(patientid AS STRING) AS patient_id,
    CAST(departmentid AS STRING) AS dept_id,
    CAST(providerid AS STRING) AS provider_id,
    CAST(encountertype AS STRING) AS encounter_type,
    -- Giả sử encounterdate cũng là Epoch milliseconds giống các bảng khác
    EXTRACT(DATE FROM TIMESTAMP_MILLIS(CAST(encounterdate AS INT64))) AS encounter_date,
    CAST(procedurecode AS STRING) AS procedure_code,
    TIMESTAMP_MILLIS(CAST(inserteddate AS INT64)) AS created_at,
    'hospital_a' AS _source_system,
    CURRENT_TIMESTAMP() AS _silver_updated_at
FROM `fpt-fresher-495407.bronze_dataset.hospital_a_encounters` AS src
WHERE NOT EXISTS (
    SELECT 1 
    FROM `fpt-fresher-495407.silver_dataset.encounters` AS tgt
    WHERE tgt.encounter_id = CAST(src.encounterid AS STRING)
      AND tgt._source_system = 'hospital_a'
)
UNION ALL
SELECT
    CAST(encounterid AS STRING),
    CAST(patientid AS STRING),
    CAST(departmentid AS STRING),
    CAST(providerid AS STRING),
    CAST(encountertype AS STRING),
    EXTRACT(DATE FROM TIMESTAMP_MILLIS(CAST(encounterdate AS INT64))),
    CAST(procedurecode AS STRING),
    TIMESTAMP_MILLIS(CAST(inserteddate AS INT64)),
    'hospital_b',
    CURRENT_TIMESTAMP()
FROM `fpt-fresher-495407.bronze_dataset.hospital_b_encounters` AS src
WHERE NOT EXISTS (
    SELECT 1 
    FROM `fpt-fresher-495407.silver_dataset.encounters` AS tgt
    WHERE tgt.encounter_id = CAST(src.encounterid AS STRING)
      AND tgt._source_system = 'hospital_b'
);
