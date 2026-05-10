-- 1. Khởi tạo bảng Silver nếu chưa có
--DROP TABLE IF EXISTS `fpt-fresher-495407.silver_dataset.patients`;
CREATE TABLE IF NOT EXISTS `fpt-fresher-495407.silver_dataset.patients` (
    patient_sk STRING,
    patient_id STRING,
    src_patient_id STRING,
    ssn_hash STRING,
    date_of_birth DATE,
    first_name STRING,
    last_name STRING,
    middle_name STRING,
    phone_number STRING,
    address STRING,
    gender STRING,
    updated_at TIMESTAMP,
    _source_system STRING,
    _row_hash STRING,
    _is_quarantined BOOLEAN,
    is_current BOOLEAN,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP
);
-- 2. Chuẩn bị dữ liệu từ Bronze (Dedup lấy bản ghi mới nhất của mỗi patient)
-- Tính toán _row_hash để phát hiện thay đổi và mã hóa cột SSN (PII Masking)
CREATE TEMP TABLE temp_source_patients AS WITH hospital_union AS(
    SELECT REPLACE(CAST(patientid AS STRING), 'HOSP1', 'HA-') AS patient_id,
        CAST(patientid AS STRING) AS src_patient_id,
        CAST(ssn AS STRING) AS ssn,
        EXTRACT(
            DATE
            FROM TIMESTAMP_MILLIS(CAST(dob AS INT64))
        ) AS date_of_birth,
        CAST(firstname AS STRING) AS first_name,
        CAST(lastname AS STRING) AS last_name,
        CAST(middlename AS STRING) AS middle_name,
        CAST(phonenumber AS STRING) AS phone_number,
        CAST(address AS STRING) AS address,
        CAST(gender AS STRING) AS gender,
        TIMESTAMP_MILLIS(CAST(modifieddate AS INT64)) AS updated_at,
        'hospital_a' AS _source_system,
        FROM `fpt-fresher-495407.bronze_dataset.ha_patients`
    UNION ALL
    SELECT REPLACE(CAST(id AS STRING), 'HOSP2', 'HB-') AS patient_id,
        CAST(id AS STRING) AS src_patient_id,
        CAST(ssn AS STRING) AS ssn,
        EXTRACT(
            DATE
            FROM TIMESTAMP_MILLIS(CAST(dob AS INT64))
        ) AS date_of_birth,
        CAST(f_name AS STRING) AS first_name,
        CAST(l_name AS STRING) AS last_name,
        CAST(m_name AS STRING) AS middle_name,
        CAST(phonenumber AS STRING) AS phone_number,
        CAST(address AS STRING) AS address,
        CAST(gender AS STRING) AS gender,
        TIMESTAMP_MILLIS(CAST(modifieddate AS INT64)) AS updated_at,
        'hospital_b' AS _source_system,
        FROM `fpt-fresher-495407.bronze_dataset.hb_patients`
)
SELECT patient_id,
    src_patient_id,
    -- Mã hóa dữ liệu nhạy cảm PII bằng SHA256
    TO_HEX(SHA256(IFNULL(ssn, ''))) AS ssn_hash,
    date_of_birth,
    first_name,
    last_name,
    middle_name,
    phone_number,
    address,
    gender,
    updated_at,
    _source_system,
    -- Tính hash. Lưu ý: vẫn đưa ssn (bản rõ) vào hash để theo dõi nếu ssn bị sửa đổi
    TO_HEX(
        SHA256(
            CONCAT(
                IFNULL(ssn, ''),
                '|',
                IFNULL(CAST(date_of_birth AS STRING), ''),
                '|',
                IFNULL(first_name, ''),
                '|',
                IFNULL(last_name, ''),
                '|',
                IFNULL(middle_name, ''),
                '|',
                IFNULL(phone_number, ''),
                '|',
                IFNULL(address, ''),
                '|',
                IFNULL(gender, '')
            )
        )
    ) AS _row_hash
FROM hospital_union;
-- 3. SCD Type 2: BƯỚC 1 - UPDATE
-- Hết hạn (expire) các bản ghi cũ đang active nếu phát hiện có sự thay đổi về hash
UPDATE `fpt-fresher-495407.silver_dataset.patients` AS T
SET T.is_current = FALSE,
    T.effective_to = CURRENT_TIMESTAMP()
WHERE T.is_current = TRUE
    AND EXISTS (
        SELECT 1
        FROM temp_source_patients AS S
        WHERE S.patient_id = T.patient_id
            AND S._source_system = T._source_system
            AND S._row_hash != T._row_hash
    );
-- 4. SCD Type 2: BƯỚC 2 - INSERT
-- Insert các bản ghi mới (bao gồm BN mới và BN cũ vừa được UPDATE ở trên)
INSERT INTO `fpt-fresher-495407.silver_dataset.patients` (
        patient_sk,
        patient_id,
        src_patient_id,
        ssn_hash,
        date_of_birth,
        first_name,
        last_name,
        middle_name,
        phone_number,
        address,
        gender,
        updated_at,
        _source_system,
        _row_hash,
        _is_quarantined,
        is_current,
        effective_from,
        effective_to
    )
SELECT GENERATE_UUID() AS patient_sk,
    S.patient_id,
    S.src_patient_id,
    S.ssn_hash,
    S.date_of_birth,
    S.first_name,
    S.last_name,
    S.middle_name,
    S.phone_number,
    S.address,
    S.gender,
    S.updated_at,
    S._source_system,
    S._row_hash,
    CASE
        WHEN S.ssn_hash IS NULL
        OR S.date_of_birth IS NULL
        OR S.first_name IS NULL
        OR S.last_name IS NULL
        OR S.gender IS NULL THEN TRUE
        ELSE FALSE
    END AS _is_quarantined,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS effective_from,
    NULL AS effective_to
FROM temp_source_patients AS S
WHERE NOT EXISTS (
        SELECT 1
        FROM `fpt-fresher-495407.silver_dataset.patients` AS T
        WHERE T.patient_id = S.patient_id
            AND T._source_system = S._source_system
            AND T.is_current = TRUE
            AND T._row_hash = S._row_hash
    );