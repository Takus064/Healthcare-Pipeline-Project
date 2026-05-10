-- 1. Khởi tạo bảng Silver nếu chưa có
--DROP TABLE IF EXISTS `fpt-fresher-495407.silver_dataset.claim_data`;
CREATE TABLE IF NOT EXISTS `fpt-fresher-495407.silver_dataset.claim_data` (
    claim_id STRING,
    src_claim_id STRING,
    transaction_id STRING,
    patient_id STRING,
    encounter_id STRING,
    provider_id STRING,
    dept_id STRING,
    service_date DATE,
    claim_date DATE,
    payor_id STRING,
    claim_amount FLOAT64,
    paid_amount FLOAT64,
    claim_status STRING,
    payor_type STRING,
    deductible FLOAT64,
    coinsurance FLOAT64,
    copay FLOAT64,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _source_system STRING,
    _silver_updated_at TIMESTAMP,
    _is_quarantined BOOLEAN
);
-- 2. Thực hiện SCD Type 1
MERGE INTO `fpt-fresher-495407.silver_dataset.claim_data` AS T USING (
    WITH dedup_data AS (
        -- Lấy bản ghi mới nhất nếu có duplicate trong file CSV (dựa vào modifieddate)
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY claimid,
                _source_system
                ORDER BY CAST(modifieddate AS DATE) DESC
            ) AS rn
        FROM `fpt-fresher-495407.bronze_dataset.claims_data`
    )
    SELECT CONCAT(
            CASE
                WHEN _source_system = 'hospital_a' THEN 'HA-'
                ELSE 'HB-'
            END,
            CAST(claimid AS STRING)
        ) AS claim_id,
        CAST(claimid AS STRING) AS src_claim_id,
        CONCAT(
            CASE
                WHEN _source_system = 'hospital_a' THEN 'HA-'
                ELSE 'HB-'
            END,
            CAST(transactionid AS STRING)
        ) AS transaction_id,
        CASE
            WHEN CAST(patientid AS STRING) LIKE 'HOSP1%' THEN REPLACE(CAST(patientid AS STRING), 'HOSP1', 'HA-')
            WHEN CAST(patientid AS STRING) LIKE 'HOSP2%' THEN REPLACE(CAST(patientid AS STRING), 'HOSP2', 'HB-')
            ELSE CAST(patientid AS STRING)
        END AS patient_id,
        CONCAT(
            CASE
                WHEN _source_system = 'hospital_a' THEN 'HA-'
                ELSE 'HB-'
            END,
            CAST(encounterid AS STRING)
        ) AS encounter_id,
        CONCAT(
            CASE
                WHEN _source_system = 'hospital_a' THEN 'HA-'
                ELSE 'HB-'
            END,
            CAST(providerid AS STRING)
        ) AS provider_id,
        CONCAT(
            CASE
                WHEN _source_system = 'hospital_a' THEN 'HA-'
                ELSE 'HB-'
            END,
            CAST(deptid AS STRING)
        ) AS dept_id,
        -- Dữ liệu trong CSV đang ở dạng YYYY-MM-DD nên có thể CAST trực tiếp sang DATE
        CAST(servicedate AS DATE) AS service_date,
        CAST(claimdate AS DATE) AS claim_date,
        CAST(payorid AS STRING) AS payor_id,
        CAST(claimamount AS FLOAT64) AS claim_amount,
        CAST(paidamount AS FLOAT64) AS paid_amount,
        CAST(claimstatus AS STRING) AS claim_status,
        CAST(payortype AS STRING) AS payor_type,
        CAST(deductible AS FLOAT64) AS deductible,
        CAST(coinsurance AS FLOAT64) AS coinsurance,
        CAST(copay AS FLOAT64) AS copay,
        -- Chuyển Date string thành Timestamp
        CAST(CAST(insertdate AS DATE) AS TIMESTAMP) AS created_at,
        CAST(CAST(modifieddate AS DATE) AS TIMESTAMP) AS updated_at,
        _source_system,
        (claimid IS NULL) as _is_quarantined
    FROM dedup_data
    WHERE rn = 1
) AS S ON T.claim_id = S.claim_id
AND T._source_system = S._source_system
WHEN MATCHED THEN
UPDATE
SET transaction_id = S.transaction_id,
    patient_id = S.patient_id,
    encounter_id = S.encounter_id,
    provider_id = S.provider_id,
    dept_id = S.dept_id,
    service_date = S.service_date,
    claim_date = S.claim_date,
    payor_id = S.payor_id,
    claim_amount = S.claim_amount,
    paid_amount = S.paid_amount,
    claim_status = S.claim_status,
    payor_type = S.payor_type,
    deductible = S.deductible,
    coinsurance = S.coinsurance,
    copay = S.copay,
    updated_at = S.updated_at,
    _silver_updated_at = CURRENT_TIMESTAMP(),
    _is_quarantined = S._is_quarantined
    WHEN NOT MATCHED THEN
INSERT (
        claim_id,
        src_claim_id,
        transaction_id,
        patient_id,
        encounter_id,
        provider_id,
        dept_id,
        service_date,
        claim_date,
        payor_id,
        claim_amount,
        paid_amount,
        claim_status,
        payor_type,
        deductible,
        coinsurance,
        copay,
        created_at,
        updated_at,
        _source_system,
        _silver_updated_at,
        _is_quarantined
    )
VALUES (
        S.claim_id,
        S.src_claim_id,
        S.transaction_id,
        S.patient_id,
        S.encounter_id,
        S.provider_id,
        S.dept_id,
        S.service_date,
        S.claim_date,
        S.payor_id,
        S.claim_amount,
        S.paid_amount,
        S.claim_status,
        S.payor_type,
        S.deductible,
        S.coinsurance,
        S.copay,
        S.created_at,
        S.updated_at,
        S._source_system,
        CURRENT_TIMESTAMP(),
        S._is_quarantined
    );