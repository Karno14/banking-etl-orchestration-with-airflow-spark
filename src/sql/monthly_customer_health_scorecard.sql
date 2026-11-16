--2.d. optimasi kuery
WITH monthly_transactions AS (
    -- 1. Agregasi Transaksi per Account per Bulan (Pre-aggregation untuk mencegah fan-out join)
    SELECT
        t.account_id,
         DATE_TRUNC('month', txn_date::timestamp) AS reporting_month,
              -- Conditional Aggregation: Jumlah transaksi berdasarkan tipe
        COUNT(*) filter(where t.txn_type = 'DEBIT') AS count_debit,
        COUNT(*) filter(where t.txn_type = 'CREDIT') AS count_credit,
        COUNT(*) filter(where t.txn_type = 'TRANSFER_IN' ) AS count_transfer_in,
        COUNT(*) filter(where t.txn_type = 'TRANSFER_OUT') AS count_transfer_out,
        COUNT(*) filter(where t.txn_type = 'PAYMENT') AS count_payment,
        COUNT(*) filter(where t.txn_type = 'FEE') AS count_fee,
        COUNT(t.txn_id) AS total_txn_count,
        -- Conditional Aggregation: Rata-rata nominal transaksi per channel
        AVG(t.amount) filter(where t.channel = 'MOBILE_APP') AS avg_amount_mobile_app,
        AVG(t.amount) filter(where t.channel = 'WEB') AS avg_amount_web,
        AVG(t.amount) filter(where t.channel = 'QRIS') AS avg_amount_qris,
        AVG(t.amount) filter(where t.channel = 'ATM_NETWORK') AS avg_amount_atm,
        AVG(t.amount) filter(where t.channel = 'AUTO_DEBIT') AS avg_amount_auto_debit,
        AVG(t.amount) AS avg_amount_overall
    FROM bronze.transactions t
    WHERE t.txn_date>='${STARTDATE}' 
    and t.txn_date<'${ENDDATE}'
    and t.status = 'COMPLETED'
    GROUP BY 1,2
),
customer_monthly_accounts AS (
    -- 2. Agregasi Saldo & Limit Kredit per Customer per Bulan
    SELECT
        a.customer_id,
        mt.reporting_month,
          -- Total saldo di seluruh rekening aktif
        SUM(COALESCE(a.balance, 0.0)) AS total_balance,
        -- Khusus Kartu Kredit (Credit Card) untuk kalkulasi utilisasi kredit
        SUM(COALESCE(a.balance, 0.0)) filter(where a.account_type = 'CREDIT_CARD') AS cc_balance,
        SUM(COALESCE(a.credit_limit, 0.0)) filter(where a.account_type = 'CREDIT_CARD') AS cc_credit_limit,
        -- Agregasi volume transaksi dari monthly_transactions
        SUM(COALESCE(mt.count_debit, 0)) AS total_debit_txns,
        SUM(COALESCE(mt.count_credit, 0)) AS total_credit_txns,
        SUM(COALESCE(mt.count_transfer_in, 0)) AS total_transfer_in_txns,
        SUM(COALESCE(mt.count_transfer_out, 0)) AS total_transfer_out_txns,
        SUM(COALESCE(mt.count_payment, 0)) AS total_payment_txns,
        SUM(COALESCE(mt.count_fee, 0)) AS total_fee_txns,
        SUM(COALESCE(mt.total_txn_count, 0)) AS grand_total_txns,
        -- Rata-rata per channel di tingkat nasabah
        AVG(mt.avg_amount_mobile_app) AS avg_amount_mobile_app,
        AVG(mt.avg_amount_web) AS avg_amount_web,
        AVG(mt.avg_amount_qris) AS avg_amount_qris,
        AVG(mt.avg_amount_atm) AS avg_amount_atm,
        AVG(mt.avg_amount_auto_debit) AS avg_amount_auto_debit
    FROM bronze.accounts a
    LEFT JOIN monthly_transactions mt 
        ON a.account_id = mt.account_id
    WHERE a.status = 'ACTIVE'
    GROUP BY a.customer_id, mt.reporting_month
), --select * from customer_monthly_accounts;
customer_latest_credit_scores AS (
    -- 3. Mengambil Credit Score & Probability of Default terbaru di setiap bulan
    SELECT
        customer_id,
        DATE_TRUNC('month', score_date::timestamp) AS reporting_month,
        credit_score,
        probability_of_default,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, DATE_TRUNC('month', score_date::timestamp)
            ORDER BY score_date DESC
        ) AS rn
    FROM bronze.credit_scores scor
    where  scor.score_date>='${STARTDATE}' 
    and scor.score_date<'${ENDDATE}' 
),
mom_balance_metrics AS (
    -- 4. Menghitung Month-over-Month (MoM) Balance Change menggunakan fungsi Window LAG()
    SELECT
        cma.customer_id,
        cma.reporting_month,
        cma.total_balance,
        cma.cc_balance,
        cma.cc_credit_limit,
        -- Saldo bulan sebelumnya menggunakan LAG()
        LAG(cma.total_balance) OVER (
            PARTITION BY cma.customer_id 
            ORDER BY cma.reporting_month ASC
        ) AS prev_month_balance,
        cma.total_debit_txns,
        cma.total_credit_txns,
        cma.total_transfer_in_txns,
        cma.total_transfer_out_txns,
        cma.total_payment_txns,
        cma.total_fee_txns,
        cma.grand_total_txns,
        cma.avg_amount_mobile_app,
        cma.avg_amount_web,
        cma.avg_amount_qris,
        cma.avg_amount_atm,
        cma.avg_amount_auto_debit,
        cs.credit_score,
        COALESCE(cs.probability_of_default, 0.0) AS probability_of_default
    FROM customer_monthly_accounts cma
    LEFT JOIN customer_latest_credit_scores cs
        ON cma.customer_id = cs.customer_id
        AND cma.reporting_month = cs.reporting_month
        AND cs.rn = 1
    WHERE cma.reporting_month IS NOT NULL
)-- select * from mom_balance_metrics order by 1,2;
-- 5. Final Output Scorecard & Flag Risiko (risk_flag)
SELECT
    m.customer_id,
    c.full_name,
    c.segment,
    c.risk_score AS customer_base_risk_score,
    TO_CHAR(m.reporting_month, 'YYYY-MM') AS reporting_period,
      -- a. Total saldo di seluruh rekening
    ROUND(m.total_balance::numeric, 2) AS total_balance,
    ROUND(COALESCE(m.prev_month_balance, m.total_balance)::numeric, 2) AS prev_month_balance,
    -- b. MoM Balance Change (Nominal & Persentase)
    ROUND((m.total_balance - COALESCE(m.prev_month_balance, m.total_balance))::numeric, 2) AS mom_balance_change_nominal,
    ROUND(
        CASE 
            WHEN m.prev_month_balance IS NULL OR m.prev_month_balance = 0 THEN 0.0
            ELSE ((m.total_balance - m.prev_month_balance) / m.prev_month_balance * 100.0)
        END::numeric, 
        2
    ) AS mom_balance_change_pct,
    -- c. Jumlah transaksi berdasarkan tipe (Conditional Aggregation / PIVOT)
    m.total_debit_txns AS count_txn_debit,
    m.total_credit_txns AS count_txn_credit,
    m.total_transfer_in_txns AS count_txn_transfer_in,
    m.total_transfer_out_txns AS count_txn_transfer_out,
    m.total_payment_txns AS count_txn_payment,
    m.total_fee_txns AS count_txn_fee,
    m.grand_total_txns,
    -- d. Rata-rata nominal per channel
    ROUND(COALESCE(m.avg_amount_mobile_app, 0.0)::numeric, 2) AS avg_amount_mobile_app,
    ROUND(COALESCE(m.avg_amount_web, 0.0)::numeric, 2) AS avg_amount_web,
    ROUND(COALESCE(m.avg_amount_qris, 0.0)::numeric, 2) AS avg_amount_qris,
    ROUND(COALESCE(m.avg_amount_atm, 0.0)::numeric, 2) AS avg_amount_atm,
    ROUND(COALESCE(m.avg_amount_auto_debit, 0.0)::numeric, 2) AS avg_amount_auto_debit,
    -- e. Rasio Utilisasi Kredit (Credit Card Balance / Credit Limit)
    ROUND(
        CASE 
            WHEN m.cc_credit_limit > 0 THEN (m.cc_balance / m.cc_credit_limit)
            ELSE 0.0 
        END::numeric, 
        4
    ) AS credit_utilization_ratio,
    -- Metrik Skor Risiko
    m.credit_score,
    ROUND(m.probability_of_default::numeric, 4) AS probability_of_default,
    -- f. Flag Risiko (risk_flag = TRUE jika CC Util > 80% OR PD > 0.3 OR Saldo Drop > 30%)
    CASE
        WHEN (m.cc_credit_limit > 0 AND (m.cc_balance / m.cc_credit_limit) > 0.80)
          OR (m.probability_of_default > 0.30)
          OR (m.prev_month_balance > 0 AND ((m.total_balance - m.prev_month_balance) / m.prev_month_balance) < -0.30)
        THEN TRUE
        ELSE FALSE
    END AS risk_flag,
    -- Detail Alasan Risiko (Auditability untuk Regulator/OJK)
    CONCAT_WS('; ',
        CASE WHEN (m.cc_credit_limit > 0 AND (m.cc_balance / m.cc_credit_limit) > 0.80) 
             THEN 'HIGH_CREDIT_UTILIZATION (>80%)' END,
        CASE WHEN (m.probability_of_default > 0.30) 
             THEN 'HIGH_DEFAULT_PROBABILITY (>0.30)' END,
        CASE WHEN (m.prev_month_balance > 0 AND ((m.total_balance - m.prev_month_balance) / m.prev_month_balance) < -0.30) 
             THEN 'SHARP_BALANCE_DROP (>30% MoM)' END
    ) AS risk_triggers
FROM mom_balance_metrics m
JOIN bronze.customers c 
    ON m.customer_id = c.customer_id
ORDER BY m.customer_id ASC;