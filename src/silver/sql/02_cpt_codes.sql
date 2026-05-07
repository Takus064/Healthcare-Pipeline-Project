CREATE TABLE IF NOT EXISTS `fpt-fresher-495407.silver_dataset.cpt_codes`(
    procedure_code_category STRING,
    cpt_codes STRING,
    procedure_code_descriptions STRING,
    code_status STRING,
    _silver_updated_at TIMESTAMP
);

--merge dữ liệu từ bronze vào silver
MERGE INTO `fpt-fresher-495407.silver_dataset.cpt_codes` AS T
USING (
    SELECT DISTINCT
        CAST(procedure_code_category AS STRING) AS procedure_code_category,
        CAST(cpt_codes AS STRING) AS cpt_codes,
        CAST(procedure_code_descriptions AS STRING) AS procedure_code_descriptions,
        CAST(code_status AS STRING) AS code_status
    FROM `fpt-fresher-495407.bronze_dataset.cpt_codes`
) AS S
ON T.cpt_codes = S.cpt_codes

WHEN MATCHED THEN
    UPDATE SET 
        procedure_code_category = S.procedure_code_category,
        procedure_code_descriptions = S.procedure_code_descriptions,
        code_status = S.code_status,
        _silver_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        procedure_code_category,
        cpt_codes,
        procedure_code_descriptions,
        code_status,
        _silver_updated_at
    )
    VALUES (
        S.procedure_code_category,
        S.cpt_codes,
        S.procedure_code_descriptions,
        S.code_status,
        CURRENT_TIMESTAMP()
    );