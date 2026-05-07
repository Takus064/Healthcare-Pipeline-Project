-- Requirement 2: Patient History
-- Bảng OBT (One-Big-Table) chứa toàn bộ lịch sử chi tiết (Event log) của bệnh nhân.

CREATE OR REPLACE TABLE `fpt-fresher-495407.gold_dataset.patient_history` AS
SELECT
    t.patient_id,
    p.ssn_hash, -- Đã được Masking từ Silver
    p.date_of_birth,
    p.first_name AS patient_first_name,
    p.last_name AS patient_last_name,
    p.gender,
    p.address,
    
    t.encounter_id,
    e.encounter_date,
    e.encounter_type,
    
    t.transaction_id,
    t.service_date,
    t.icd_code,
    t.procedure_code,
    c.procedure_code_descriptions AS procedure_description,
    
    cl.claim_id,
    cl.claim_status,
    t.amount AS transaction_amount,
    t.paid_amount,
    cl.claim_amount,
    
    t._source_system

FROM `fpt-fresher-495407.silver_dataset.transactions` AS t

-- SCD Type 2 Join cho bảng patients (lấy bản ghi hiện tại)
LEFT JOIN `fpt-fresher-495407.silver_dataset.patients` AS p
    ON t.patient_id = p.patient_id 
   AND t._source_system = p._source_system
   AND p.is_current = TRUE

-- Encounters (SCD Type 1)
LEFT JOIN `fpt-fresher-495407.silver_dataset.encounters` AS e
    ON t.encounter_id = e.encounter_id
   AND t._source_system = e._source_system

-- CPT Codes (Shared Reference table)
LEFT JOIN `fpt-fresher-495407.silver_dataset.cpt_codes` AS c
    ON CAST(t.procedure_code AS STRING) = c.cpt_codes

-- Claim Data (SCD Type 1)
LEFT JOIN `fpt-fresher-495407.silver_dataset.claim_data` AS cl
    ON t.claim_id = cl.claim_id
   AND t._source_system = cl._source_system;
