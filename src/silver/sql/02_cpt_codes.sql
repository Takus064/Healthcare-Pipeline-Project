--DROP TABLE IF EXISTS `fpt-fresher-495407.silver_dataset.cpt_codes`;
CREATE TABLE IF NOT EXISTS `fpt-fresher-495407.silver_dataset.cpt_codes`(
    procedure_code_category STRING,
    cpt_codes STRING,
    procedure_code_descriptions STRING,
    code_status STRING,
    _silver_updated_at TIMESTAMP,
    _is_quarantined BOOLEAN
);
--merge dữ liệu từ bronze vào silver
MERGE INTO `fpt-fresher-495407.silver_dataset.cpt_codes` AS T USING (
    SELECT DISTINCT CAST(procedure_code_category AS STRING) AS procedure_code_category,
        CAST(cpt_codes AS STRING) AS cpt_codes,
        CAST(procedure_code_descriptions AS STRING) AS procedure_code_descriptions,
        CAST(code_status AS STRING) AS code_status,
        (
            procedure_code_category IS NULL
            OR cpt_codes IS NULL
            OR procedure_code_descriptions IS NULL
            OR code_status IS NULL
        ) AS _is_quarantined
    FROM `fpt-fresher-495407.bronze_dataset.cpt_codes`
) AS S ON T.cpt_codes = S.cpt_codes
WHEN MATCHED THEN
UPDATE
SET procedure_code_category = S.procedure_code_category,
    procedure_code_descriptions = S.procedure_code_descriptions,
    code_status = S.code_status,
    _silver_updated_at = CURRENT_TIMESTAMP(),
    _is_quarantined = S._is_quarantined
    WHEN NOT MATCHED THEN
INSERT (
        procedure_code_category,
        cpt_codes,
        procedure_code_descriptions,
        code_status,
        _silver_updated_at,
        _is_quarantined
    )
VALUES (
        S.procedure_code_category,
        S.cpt_codes,
        S.procedure_code_descriptions,
        S.code_status,
        CURRENT_TIMESTAMP(),
        S._is_quarantined
    );