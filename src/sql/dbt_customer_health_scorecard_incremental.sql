-- =============================================================================
-- NegaraBank - dbt Incremental Model: Customer Health Scorecard
-- File: src/sql/dbt_customer_health_scorecard_incremental.sql
-- Soal 2.f: Strategi Materialisasi & DDL Incremental Refresh
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key=['customer_id', 'reporting_month'],
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        indexes=[
            {'columns': ['customer_id', 'reporting_month'], 'unique': True},
            {'columns': ['reporting_month']},
            {'columns': ['risk_flag']}
        ],
        tags=['regulatory', 'ojk', 'scorecard', 'gold']
    )
}}

WITH monthly_transactions AS (
    -- 1. Pre-Aggregation Transaksi per Akun per Bulan
    SELECT
        t.account_id,
        DATE_TRUNC('month', t.txn_date::timestamp) AS reporting_month,
        
        COUNT(CASE WHEN t.txn_type = 'DEBIT' THEN 1 END) AS count_debit,
        COUNT(CASE WHEN t.txn_type = 'CREDIT' THEN 1 END) AS count_credit,
        COUNT(CASE WHEN t.txn_type = 'TRANSFER_IN' THEN 1 END) AS count_transfer_in,
        COUNT(CASE WHEN t.txn_type = 'TRANSFER_OUT' THEN 1 END) AS count_transfer_out,
        COUNT(CASE WHEN t.txn_type = 'PAYMENT' THEN 1 END) AS count_payment,
        COUNT(CASE WHEN t.txn_type = 'FEE' THEN 1 END) AS count_fee,
        COUNT(t.txn_id) AS total_txn_count,

        AVG(CASE WHEN t.channel = 'MOBILE_APP' THEN t.amount END) AS avg_amount_mobile_app,
        AVG(CASE WHEN t.channel = 'WEB' THEN t.amount END) AS avg_amount_web,
        AVG(CASE WHEN t.channel = 'QRIS' THEN t.amount END) AS avg_amount_qris,
        AVG(CASE WHEN t.channel = 'ATM_NETWORK' THEN t.amount END) AS avg_amount_atm,
        AVG(CASE WHEN t.channel = 'AUTO_DEBIT' THEN t.amount END) AS avg_amount_auto_debit

    FROM {{ source('bronze', 'transactions') }} t
    WHERE t.status = 'COMPLETED'
    
    {% if is_incremental() %}
    -- =========================================================================
    -- MEKANISME INCREMENTAL FILTER (Hanya memproses data baru + lookback window)
    -- =========================================================================
    -- Lookback 2 bulan untuk memastikan perhitungan LAG() MoM tetap konsisten
    -- dan mengantisipasi transaksi susulan (late-arriving transactions)
    AND t.txn_date >= (
        SELECT DATEADD('month', -1, COALESCE(MAX(reporting_month), '1970-01-01')) 
        FROM {{ this }}
    )
    {% endif %}

    GROUP BY t.account_id, DATE_TRUNC('month', t.txn_date::timestamp)
),

customer_monthly_accounts AS (
    -- 2. Agregasi Saldo & Limit Kredit per Customer per Bulan
    SELECT
        a.customer_id,
        mt.reporting_month,
        
        SUM(COALESCE(a.balance, 0.0)) AS total_balance,
        SUM(CASE WHEN a.account_type = 'CREDIT_CARD' THEN COALESCE(a.balance, 0.0) ELSE 0.0 END) AS cc_balance,
        SUM(CASE WHEN a.account_type = 'CREDIT_CARD' THEN COALESCE(a.credit_limit, 0.0) ELSE 0.0 END) AS cc_credit_limit,

        SUM(COALESCE(mt.count_debit, 0)) AS total_debit_txns,
        SUM(COALESCE(mt.count_credit, 0)) AS total_credit_txns,
        SUM(COALESCE(mt.count_transfer_in, 0)) AS total_transfer_in_txns,
        SUM(COALESCE(mt.count_transfer_out, 0)) AS total_transfer_out_txns,
        SUM(COALESCE(mt.count_payment, 0)) AS total_payment_txns,
        SUM(COALESCE(mt.count_fee, 0)) AS total_fee_txns,
        SUM(COALESCE(mt.total_txn_count, 0)) AS grand_total_txns,

        AVG(mt.avg_amount_mobile_app) AS avg_amount_mobile_app,
        AVG(mt.avg_amount_web) AS avg_amount_web,
        AVG(mt.avg_amount_qris) AS avg_amount_qris,
        AVG(mt.avg_amount_atm) AS avg_amount_atm,
        AVG(mt.avg_amount_auto_debit) AS avg_amount_auto_debit

    FROM {{ source('bronze', 'accounts') }} a
    LEFT JOIN monthly_transactions mt 
        ON a.account_id = mt.account_id
    WHERE a.status = 'ACTIVE'
    GROUP BY a.customer_id, mt.reporting_month
),

customer_latest_credit_scores AS (
    -- 3. Mengambil Credit Score & Probability of Default terbaru
    SELECT
        customer_id,
        DATE_TRUNC('month', score_date::timestamp) AS reporting_month,
        credit_score,
        probability_of_default,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, DATE_TRUNC('month', score_date::timestamp)
            ORDER BY score_date DESC
        ) AS rn
    FROM {{ source('bronze', 'credit_scores') }}
),

mom_balance_metrics AS (
    -- 4. Menghitung MoM Balance Change menggunakan fungsi LAG()
    SELECT
        cma.customer_id,
        cma.reporting_month,
        cma.total_balance,
        cma.cc_balance,
        cma.cc_credit_limit,

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
)

-- 5. Final Output Table (Akan di-MERGE ke tabel target dbt)
SELECT
    m.customer_id,
    m.reporting_month,
    TO_CHAR(m.reporting_month, 'YYYY-MM') AS reporting_period,
    
    ROUND(m.total_balance::numeric, 2) AS total_balance,
    ROUND(COALESCE(m.prev_month_balance, m.total_balance)::numeric, 2) AS prev_month_balance,
    ROUND((m.total_balance - COALESCE(m.prev_month_balance, m.total_balance))::numeric, 2) AS mom_balance_change_nominal,
    ROUND(
        CASE 
            WHEN m.prev_month_balance IS NULL OR m.prev_month_balance = 0 THEN 0.0
            ELSE ((m.total_balance - m.prev_month_balance) / m.prev_month_balance * 100.0)
        END::numeric, 
        2
    ) AS mom_balance_change_pct,

    m.total_debit_txns AS count_txn_debit,
    m.total_credit_txns AS count_txn_credit,
    m.total_transfer_in_txns AS count_txn_transfer_in,
    m.total_transfer_out_txns AS count_txn_transfer_out,
    m.total_payment_txns AS count_txn_payment,
    m.total_fee_txns AS count_txn_fee,
    m.grand_total_txns,

    ROUND(COALESCE(m.avg_amount_mobile_app, 0.0)::numeric, 2) AS avg_amount_mobile_app,
    ROUND(COALESCE(m.avg_amount_web, 0.0)::numeric, 2) AS avg_amount_web,
    ROUND(COALESCE(m.avg_amount_qris, 0.0)::numeric, 2) AS avg_amount_qris,
    ROUND(COALESCE(m.avg_amount_atm, 0.0)::numeric, 2) AS avg_amount_atm,
    ROUND(COALESCE(m.avg_amount_auto_debit, 0.0)::numeric, 2) AS avg_amount_auto_debit,

    ROUND(
        CASE 
            WHEN m.cc_credit_limit > 0 THEN (m.cc_balance / m.cc_credit_limit)
            ELSE 0.0 
        END::numeric, 
        4
    ) AS credit_utilization_ratio,

    m.credit_score,
    ROUND(m.probability_of_default::numeric, 4) AS probability_of_default,

    CASE
        WHEN (m.cc_credit_limit > 0 AND (m.cc_balance / m.cc_credit_limit) > 0.80)
          OR (m.probability_of_default > 0.30)
          OR (m.prev_month_balance > 0 AND ((m.total_balance - m.prev_month_balance) / m.prev_month_balance) < -0.30)
        THEN TRUE
        ELSE FALSE
    END AS risk_flag,

    CONCAT_WS('; ',
        CASE WHEN (m.cc_credit_limit > 0 AND (m.cc_balance / m.cc_credit_limit) > 0.80) 
             THEN 'HIGH_CREDIT_UTILIZATION (>80%)' END,
        CASE WHEN (m.probability_of_default > 0.30) 
             THEN 'HIGH_DEFAULT_PROBABILITY (>0.30)' END,
        CASE WHEN (m.prev_month_balance > 0 AND ((m.total_balance - m.prev_month_balance) / m.prev_month_balance) < -0.30) 
             THEN 'SHARP_BALANCE_DROP (>30% MoM)' END
    ) AS risk_triggers,

    CURRENT_TIMESTAMP AS dbt_updated_at

FROM mom_balance_metrics m;
