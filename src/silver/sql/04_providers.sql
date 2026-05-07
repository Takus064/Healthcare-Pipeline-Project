-- 1. Khởi tạo bảng Silver nếu chưa có
CREATE TABLE IF NOT EXISTS `fpt-fresher-495407.silver_dataset.providers` (
    silver_provider_key STRING,
    provider_id STRING,
    dept_id STRING,
    npi STRING,
    first_name STRING,
    last_name STRING,
    specialization STRING,
    _source_system STRING,
    _row_hash STRING,
    is_current BOOL,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP
);

-- 2. Chuẩn bị dữ liệu từ Bronze (Dedup lấy bản ghi mới nhất của mỗi provider trong batch này)
-- Tính toán _row_hash để phát hiện thay đổi
CREATE TEMP TABLE temp_source_providers AS
WITH hospital_a_rn AS(
    SELECT
        CAST(providerid AS STRING) AS provider_id,
        CAST(deptid AS STRING) AS dept_id,
        CAST(npi AS STRING) AS npi,
        CAST(firstname AS STRING) AS first_name,
        CAST(lastname AS STRING) AS last_name,
        CAST(specialization AS STRING) AS specialization,
        _source_system,
        ROW_NUMBER() OVER(PARTITION BY providerid ORDER BY _bronze_loaded_at DESC) AS rn
    FROM `fpt-fresher-495407.bronze_dataset.hospital_a_providers`
),
hospital_b_rn AS(
    SELECT
        CAST(providerid AS STRING) AS provider_id,
        CAST(deptid AS STRING) AS dept_id,
        CAST(npi AS STRING) AS npi,
        CAST(firstname AS STRING) AS first_name,
        CAST(lastname AS STRING) AS last_name,
        CAST(specialization AS STRING) AS specialization,
        _source_system,
        ROW_NUMBER() OVER(PARTITION BY providerid ORDER BY _bronze_loaded_at DESC) AS rn
    FROM `fpt-fresher-495407.bronze_dataset.hospital_b_providers`
),
source_data AS(
    SELECT * FROM hospital_a_rn WHERE rn = 1
    UNION ALL
    SELECT * FROM hospital_b_rn WHERE rn = 1
)
SELECT 
    *,
    -- Tính hash dựa trên các trường dữ liệu kinh doanh (trừ các trường kỹ thuật)
    TO_HEX(SHA256(CONCAT(
        IFNULL(dept_id, ''), '|',
        IFNULL(npi, ''), '|',
        IFNULL(first_name, ''), '|',
        IFNULL(last_name, ''), '|',
        IFNULL(specialization, '')
    ))) AS _row_hash
FROM source_data;


-- 3. SCD Type 2: BƯỚC 1 - UPDATE
-- Hết hạn (expire) các bản ghi cũ đang active nếu phát hiện có sự thay đổi về hash
UPDATE `fpt-fresher-495407.silver_dataset.providers` AS T
SET 
    T.is_current = FALSE,
    T.effective_to = CURRENT_TIMESTAMP()
WHERE T.is_current = TRUE
AND EXISTS (
    SELECT 1 
    FROM temp_source_providers AS S
    WHERE S.provider_id = T.provider_id 
      AND S._source_system = T._source_system
      AND S._row_hash != T._row_hash -- Phát hiện có thay đổi thông tin
);


-- 4. SCD Type 2: BƯỚC 2 - INSERT
-- Insert các bản ghi HOÀN TOÀN MỚI, hoặc các bản ghi ĐÃ BỊ THAY ĐỔI (phiên bản mới nhất)
INSERT INTO `fpt-fresher-495407.silver_dataset.providers` (
    silver_provider_key,
    provider_id,
    dept_id,
    npi,
    first_name,
    last_name,
    specialization,
    _source_system,
    _row_hash,
    is_current,
    effective_from,
    effective_to
)
SELECT
    GENERATE_UUID() AS silver_provider_key, -- Sinh khóa thay thế duy nhất cho mỗi phiên bản
    S.provider_id,
    S.dept_id,
    S.npi,
    S.first_name,
    S.last_name,
    S.specialization,
    S._source_system,
    S._row_hash,
    TRUE AS is_current,
    CURRENT_TIMESTAMP() AS effective_from,
    NULL AS effective_to
FROM temp_source_providers AS S
WHERE NOT EXISTS (
    -- Chỉ insert nếu bản ghi CURRENT trong Silver KHÔNG giống với hash hiện tại
    SELECT 1 
    FROM `fpt-fresher-495407.silver_dataset.providers` AS T
    WHERE T.provider_id = S.provider_id
      AND T._source_system = S._source_system
      AND T.is_current = TRUE
      AND T._row_hash = S._row_hash
);
