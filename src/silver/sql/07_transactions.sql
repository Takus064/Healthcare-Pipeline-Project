-- 1. Khởi tạo bảng Silver nếu chưa có
--DROP TABLE IF EXISTS `fpt-fresher-495407.silver_dataset.transactions`;
CREATE TABLE IF NOT EXISTS `fpt-fresher-495407.silver_dataset.transactions` (
    transaction_id STRING,
    src_transaction_id STRING,
    dept_id STRING,
    provider_id STRING,
    claim_id STRING,
    patient_id STRING,
    encounter_id STRING,
    medicare_id STRING,
    medicaid_id STRING,
    payor_id STRING,
    icd_code STRING,
    procedure_code STRING,
    visit_type STRING,
    line_of_business STRING,
    amount_type STRING,
    amount FLOAT64,
    paid_amount FLOAT64,
    service_date DATE,
    visit_date DATE,
    paid_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    _source_system STRING,
    _is_quarantined BOOLEAN,
    _silver_updated_at TIMESTAMP
);
-- 2. Append-Only: Chèn các giao dịch mới chưa từng tồn tại
INSERT INTO `fpt-fresher-495407.silver_dataset.transactions`
SELECT CONCAT('HA-', CAST(transactionid AS STRING)) AS transaction_id,
    CAST(transactionid AS STRING) AS src_transaction_id,
    CONCAT('HA-', CAST(deptid AS STRING)) AS dept_id,
    CONCAT('HA-', CAST(providerid AS STRING)) AS provider_id,
    CONCAT('HA-', CAST(claimid AS STRING)) AS claim_id,
    REPLACE(CAST(patientid AS STRING), 'HOSP1', 'HA-') AS patient_id,
    CONCAT('HA-', CAST(encounterid AS STRING)) AS encounter_id,
    CAST(medicareid AS STRING) AS medicare_id,
    CAST(medicaidid AS STRING) AS medicaid_id,
    CAST(payorid AS STRING) AS payor_id,
    CAST(icdcode AS STRING) AS icd_code,
    CAST(procedurecode AS STRING) AS procedure_code,
    CAST(visittype AS STRING) AS visit_type,
    CAST(lineofbusiness AS STRING) AS line_of_business,
    CAST(amounttype AS STRING) AS amount_type,
    CAST(amount AS FLOAT64) AS amount,
    CAST(paidamount AS FLOAT64) AS paid_amount,
    -- Xử lý các trường thời gian từ INT64 (milliseconds)
    EXTRACT(
        DATE
        FROM TIMESTAMP_MILLIS(servicedate)
    ) AS service_date,
    EXTRACT(
        DATE
        FROM TIMESTAMP_MILLIS(visitdate)
    ) AS visit_date,
    EXTRACT(
        DATE
        FROM TIMESTAMP_MILLIS(paiddate)
    ) AS paid_date,
    TIMESTAMP_MILLIS(insertdate) AS created_at,
    TIMESTAMP_MILLIS(modifieddate) AS updated_at,
    'hospital_a' AS _source_system,
    CASE
        WHEN deptid IS NULL
        OR providerid IS NULL
        OR claimid IS NULL
        OR patientid IS NULL
        OR encounterid IS NULL
        OR medicareid IS NULL
        OR medicaidid IS NULL
        OR payorid IS NULL
        OR icdcode IS NULL
        OR procedurecode IS NULL
        OR visittype IS NULL
        OR lineofbusiness IS NULL
        OR amounttype IS NULL
        OR amount IS NULL
        OR servicedate IS NULL
        OR visitdate IS NULL THEN TRUE
        ELSE FALSE
    END AS _is_quarantined,
    CURRENT_TIMESTAMP() AS _silver_updated_at
FROM `fpt-fresher-495407.bronze_dataset.ha_transactions` AS src
WHERE NOT EXISTS (
        SELECT 1
        FROM `fpt-fresher-495407.silver_dataset.transactions` AS tgt
        WHERE tgt.transaction_id = CONCAT('HA-', CAST(src.transactionid AS STRING))
            AND tgt._source_system = 'hospital_a'
    )
UNION ALL
SELECT CONCAT('HB-', CAST(transactionid AS STRING)) AS transaction_id,
    CAST(transactionid AS STRING) AS src_transaction_id,
    CONCAT('HB-', CAST(deptid AS STRING)) AS dept_id,
    CONCAT('HB-', CAST(providerid AS STRING)) AS provider_id,
    CONCAT('HB-', CAST(claimid AS STRING)) AS claim_id,
    REPLACE(CAST(patientid AS STRING), 'HOSP2', 'HB-') AS patient_id,
    CONCAT('HB-', CAST(encounterid AS STRING)) AS encounter_id,
    CAST(medicareid AS STRING) AS medicare_id,
    CAST(medicaidid AS STRING) AS medicaid_id,
    CAST(payorid AS STRING) AS payor_id,
    CAST(icdcode AS STRING) AS icd_code,
    CAST(procedurecode AS STRING) AS procedure_code,
    CAST(visittype AS STRING) AS visit_type,
    CAST(lineofbusiness AS STRING) AS line_of_business,
    CAST(amounttype AS STRING) AS amount_type,
    CAST(amount AS FLOAT64) AS amount,
    CAST(paidamount AS FLOAT64) AS paid_amount,
    -- Xử lý các trường thời gian từ INT64 (milliseconds)
    EXTRACT(
        DATE
        FROM TIMESTAMP_MILLIS(servicedate)
    ) AS service_date,
    EXTRACT(
        DATE
        FROM TIMESTAMP_MILLIS(visitdate)
    ) AS visit_date,
    EXTRACT(
        DATE
        FROM TIMESTAMP_MILLIS(paiddate)
    ) AS paid_date,
    TIMESTAMP_MILLIS(insertdate) AS created_at,
    TIMESTAMP_MILLIS(modifieddate) AS updated_at,
    'hospital_b' AS _source_system,
    CASE
        WHEN deptid IS NULL
        OR providerid IS NULL
        OR claimid IS NULL
        OR patientid IS NULL
        OR encounterid IS NULL
        OR medicareid IS NULL
        OR medicaidid IS NULL
        OR payorid IS NULL
        OR icdcode IS NULL
        OR procedurecode IS NULL
        OR visittype IS NULL
        OR lineofbusiness IS NULL
        OR amounttype IS NULL
        OR amount IS NULL
        OR servicedate IS NULL
        OR visitdate IS NULL THEN TRUE
        ELSE FALSE
    END AS _is_quarantined,
    CURRENT_TIMESTAMP() AS _silver_updated_at
FROM `fpt-fresher-495407.bronze_dataset.hb_transactions` AS src
WHERE NOT EXISTS (
        SELECT 1
        FROM `fpt-fresher-495407.silver_dataset.transactions` AS tgt
        WHERE tgt.transaction_id = CONCAT('HB-', CAST(src.transactionid AS STRING))
            AND tgt._source_system = 'hospital_b'
    );