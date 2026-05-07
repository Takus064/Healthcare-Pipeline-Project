-- 1. Khởi tạo bảng Silver nếu chưa có
CREATE TABLE IF NOT EXISTS `fpt-fresher-495407.silver_dataset.transactions` (
    transaction_id STRING,
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
    _silver_updated_at TIMESTAMP
);

-- 2. Append-Only: Chèn các giao dịch mới chưa từng tồn tại
INSERT INTO `fpt-fresher-495407.silver_dataset.transactions`
SELECT
    CAST(transactionid AS STRING) AS transaction_id,
    CAST(deptid AS STRING) AS dept_id,
    CAST(providerid AS STRING) AS provider_id,
    CAST(claimid AS STRING) AS claim_id,
    CAST(patientid AS STRING) AS patient_id,
    CAST(encounterid AS STRING) AS encounter_id,
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
    EXTRACT(DATE FROM TIMESTAMP_MILLIS(servicedate)) AS service_date,
    EXTRACT(DATE FROM TIMESTAMP_MILLIS(visitdate)) AS visit_date,
    EXTRACT(DATE FROM TIMESTAMP_MILLIS(paiddate)) AS paid_date,
    TIMESTAMP_MILLIS(insertdate) AS created_at,
    TIMESTAMP_MILLIS(modifieddate) AS updated_at,
    
    'hospital_a' AS _source_system,
    CURRENT_TIMESTAMP() AS _silver_updated_at
FROM `fpt-fresher-495407.bronze_dataset.hospital_a_transactions` AS src
WHERE NOT EXISTS (
    SELECT 1 
    FROM `fpt-fresher-495407.silver_dataset.transactions` AS tgt
    WHERE tgt.transaction_id = CAST(src.transactionid AS STRING)
      AND tgt._source_system = 'hospital_a'
)

UNION ALL

SELECT
    CAST(transactionid AS STRING),
    CAST(deptid AS STRING),
    CAST(providerid AS STRING),
    CAST(claimid AS STRING),
    CAST(patientid AS STRING),
    CAST(encounterid AS STRING),
    CAST(medicareid AS STRING),
    CAST(medicaidid AS STRING),
    CAST(payorid AS STRING),
    CAST(icdcode AS STRING),
    CAST(procedurecode AS STRING),
    CAST(visittype AS STRING),
    CAST(lineofbusiness AS STRING),
    CAST(amounttype AS STRING),
    CAST(amount AS FLOAT64),
    CAST(paidamount AS FLOAT64),
    
    -- Xử lý các trường thời gian từ INT64 (milliseconds)
    EXTRACT(DATE FROM TIMESTAMP_MILLIS(servicedate)),
    EXTRACT(DATE FROM TIMESTAMP_MILLIS(visitdate)),
    EXTRACT(DATE FROM TIMESTAMP_MILLIS(paiddate)),
    TIMESTAMP_MILLIS(insertdate),
    TIMESTAMP_MILLIS(modifieddate),
    
    'hospital_b',
    CURRENT_TIMESTAMP()
FROM `fpt-fresher-495407.bronze_dataset.hospital_b_transactions` AS src
WHERE NOT EXISTS (
    SELECT 1 
    FROM `fpt-fresher-495407.silver_dataset.transactions` AS tgt
    WHERE tgt.transaction_id = CAST(src.transactionid AS STRING)
      AND tgt._source_system = 'hospital_b'
);
