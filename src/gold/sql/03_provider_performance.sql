-- Provider Performance Summary (Gold) : This table summarizes provider activity, including the
-- number of encounters, total billed amount, and claim success rate.
-- Tổng hợp hiệu suất theo Tháng (YYYY-MM)
CREATE OR REPLACE TABLE `fpt-fresher-495407.gold_dataset.provider_performance` AS
SELECT t.provider_id,
    p.first_name AS provider_first_name,
    p.last_name AS provider_last_name,
    p.specialization,
    FORMAT_DATE('%Y-%m', t.service_date) AS report_month,
    t._source_system,
    COUNT(DISTINCT t.encounter_id) AS total_encounters,
    SUM(t.amount) AS total_billed_amount,
    -- Claim metrics
    COUNT(DISTINCT cl.claim_id) AS total_claims,
    COUNT(
        DISTINCT CASE
            WHEN cl.claim_status = 'Approved' THEN cl.claim_id
        END
    ) AS successful_claims,
    -- Claim success rate (%)
    SAFE_DIVIDE(
        COUNT(
            DISTINCT CASE
                WHEN cl.claim_status = 'Approved' THEN cl.claim_id
            END
        ),
        COUNT(DISTINCT cl.claim_id)
    ) * 100 AS claim_success_rate
FROM `fpt-fresher-495407.silver_dataset.transactions` AS t -- Lấy Provider (bản ghi hiện tại)
    LEFT JOIN `fpt-fresher-495407.silver_dataset.providers` AS p ON t.provider_id = p.provider_id
    AND t._source_system = p._source_system
    AND p.is_current = TRUE -- Claim data để tính billed amount và status
    AND p._is_quarantined = FALSE
    LEFT JOIN `fpt-fresher-495407.silver_dataset.claim_data` AS cl ON t.claim_id = cl.claim_id
    AND t._source_system = cl._source_system
    AND cl._is_quarantined = FALSE
WHERE t._is_quarantined = FALSE
GROUP BY t.provider_id,
    p.first_name,
    p.last_name,
    p.specialization,
    report_month,
    t._source_system;