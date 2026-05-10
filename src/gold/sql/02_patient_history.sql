-- Requirement 2: Patient History: This table provides a complete history of a patient’s visits, diagnoses, and financial interactions.
-- Bảng OBT (One-Big-Table) chứa toàn bộ lịch sử chi tiết (Event log) của bệnh nhân.
CREATE OR REPLACE TABLE `fpt-fresher-495407.gold_dataset.patient_history` AS
SELECT t.patient_id,
    p.ssn_hash,
    -- Đã được Masking từ Silver
    p.date_of_birth,
    p.first_name AS patient_first_name,
    p.last_name AS patient_last_name,
    CONCAT(p.first_name, ' ', p.last_name) AS patient_full_name,
    p.phone_number,
    p.gender,
    p.address,
    t.encounter_id,
    e.encounter_date,
    e.encounter_type,
    t.transaction_id,
    t.service_date,
    t.visit_type,
    t.visit_date,
    t.icd_code,
    t.line_of_business,
    t.procedure_code,
    c.procedure_code_category,
    c.procedure_code_descriptions AS procedure_description,
    t.claim_id,
    cl.claim_status,
    cl.claim_date,
    t.amount_type,
    t.amount AS transaction_amount,
    t.paid_amount,
    cl.claim_amount,
    t._source_system
FROM `fpt-fresher-495407.silver_dataset.transactions` AS t -- SCD Type 2 Join cho bảng patients (lấy bản ghi hiện tại)
    LEFT JOIN `fpt-fresher-495407.silver_dataset.patients` AS p ON t.patient_id = p.patient_id
    AND t._source_system = p._source_system
    AND p.is_current = TRUE
    AND p._is_quarantined = FALSE -- Encounters (SCD Type 1)
    LEFT JOIN `fpt-fresher-495407.silver_dataset.encounters` AS e ON t.encounter_id = e.encounter_id
    AND t._source_system = e._source_system
    AND e._is_quarantined = FALSE -- CPT Codes (Shared Reference table)
    LEFT JOIN `fpt-fresher-495407.silver_dataset.cpt_codes` AS c ON CAST(t.procedure_code AS STRING) = c.cpt_codes
    AND c._is_quarantined = FALSE -- Claim Data (SCD Type 1)
    LEFT JOIN `fpt-fresher-495407.silver_dataset.claim_data` AS cl ON t.claim_id = cl.claim_id
    AND t._source_system = cl._source_system
    AND cl._is_quarantined = FALSE
WHERE t._is_quarantined = FALSE