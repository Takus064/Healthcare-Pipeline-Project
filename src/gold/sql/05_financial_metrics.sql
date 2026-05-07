-- Requirement 5: Financial Metrics
-- Tổng hợp chỉ số tài chính theo Tháng và Hospital

CREATE OR REPLACE TABLE `fpt-fresher-495407.gold_dataset.financial_metrics` AS
SELECT
    FORMAT_DATE('%Y-%m', t.service_date) AS report_month,
    t._source_system,
    t.line_of_business,
    
    SUM(t.paid_amount) AS total_revenue,
    SUM(cl.claim_amount) AS total_billed,
    
    -- Outstanding balance = Billed - Paid
    SUM(cl.claim_amount) - SUM(t.paid_amount) AS outstanding_balance,
    
    -- Claim metrics
    COUNT(DISTINCT cl.claim_id) AS total_claims,
    COUNT(DISTINCT CASE WHEN cl.claim_status = 'Approved' THEN cl.claim_id END) AS successful_claims,
    
    -- Claim success rate (%)
    SAFE_DIVIDE(
        COUNT(DISTINCT CASE WHEN cl.claim_status = 'Approved' THEN cl.claim_id END),
        COUNT(DISTINCT cl.claim_id)
    ) * 100 AS claim_success_rate

FROM `fpt-fresher-495407.silver_dataset.transactions` AS t

LEFT JOIN `fpt-fresher-495407.silver_dataset.claim_data` AS cl
    ON t.claim_id = cl.claim_id
   AND t._source_system = cl._source_system

GROUP BY
    report_month,
    t._source_system,
    t.line_of_business;
