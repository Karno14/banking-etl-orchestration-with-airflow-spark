-- =============================================================================
-- NegaraBank - Fraud Detection Patterns Query
-- File: src/sql/fraud_detection_patterns.sql
-- Soal 2.e: Deteksi Transaksi Berpotensi Fraud (OJK & AML/CFT Compliance)
-- Format Output: customer_id, alert_type, alert_date, details_json
-- =============================================================================

WITH completed_transactions AS (
    -- 0. Standardisasi & Filter Transaksi COMPLETED
    SELECT
        t.txn_id,
        t.account_id,
        a.customer_id,
        t.txn_date::timestamp AS txn_timestamp,
        t.txn_date::date AS txn_date,
        t.amount,
        t.channel,
        t.merchant_category,
        COALESCE(t.reference_id, 'UNKNOWN_REF') AS reference_id,
        -- Ekstraksi epoch seconds untuk pemrosesan window RANGE yang efisien di semua engine
        EXTRACT(EPOCH FROM t.txn_date::timestamp) AS txn_epoch
    FROM bronze.transactions t
    JOIN bronze.accounts a 
        ON t.account_id = a.account_id
    WHERE txn_date>='${STARTDATE}'
    and txn_date<'${ENDDATE}'
    and t.status = 'COMPLETED'
),
-- -----------------------------------------------------------------------------
-- POLA 1: Velocity Spike (5+ Transaksi dalam Rentang 1 Jam)
-- Menggunakan Window Function RANGE BETWEEN (3600 detik = 1 jam)
-- -----------------------------------------------------------------------------
pattern_1_velocity AS (
    SELECT
        customer_id,
        txn_id,
        txn_timestamp,
        txn_date,
        amount,
        channel,        
        -- Menghitung total frekuensi transaksi dalam 1 jam ke belakang
        COUNT(txn_id) OVER (
            PARTITION BY customer_id
            ORDER BY txn_epoch
            RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
        ) AS txns_in_1_hour,
        -- Menghitung total volume nominal transaksi dalam 1 jam ke belakang
        SUM(amount) OVER (
            PARTITION BY customer_id
            ORDER BY txn_epoch
            RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW
        ) AS total_amount_1_hour
    FROM completed_transactions
),
alerts_pattern_1 AS (
    SELECT
        customer_id,
        'VELOCITY_SPIKE' AS alert_type,
        txn_date AS alert_date,
        json_build_object(
            'rule_name', 'High-Frequency Transactions (>=5 txns/hour)',
            'trigger_txn_id', txn_id,
            'trigger_timestamp', TO_CHAR(txn_timestamp, 'YYYY-MM-DD HH24:MI:SS'),
            'txns_count_last_1h', txns_in_1_hour,
            'total_amount_last_1h', ROUND(total_amount_1_hour::numeric, 2),
            'latest_channel', channel
        )::text AS details_json
    FROM pattern_1_velocity
    WHERE txns_in_1_hour >= 5
),-- select * from alerts_pattern_1
-- -----------------------------------------------------------------------------
-- POLA 2: Impossible Travel / Multi-City (Transaksi di >= 3 Kota pada Hari yang Sama)
-- Memerlukan join ke data referensi lokasi merchant (dim_merchants / merchant location)
-- -----------------------------------------------------------------------------
mock_merchant_locations AS (
    -- Simulasi tabel dimensi merchant location / POS terminal lokasi kota
    -- tidak ada table merchant di source jadi asumsi kita punya table
    SELECT
        t.reference_id,
        COALESCE(
            CASE 
                WHEN reference_id<='REF3' then 'jAKARTA'
                WHEN reference_id>='REF5' and reference_id<'REF9' then 'BANDUNG'
                WHEN reference_id>='UNKNOWN_REF' then 'SURABAYA'
                ELSE 'Semarang'
            END,
            'DENPASAR'
        ) AS merchant_city
    FROM (SELECT DISTINCT reference_id FROM completed_transactions) t
),
txns_with_merchant_city AS (
    SELECT
        ct.customer_id,
        ct.txn_date,
        ct.txn_id,
        ct.amount,
        m.merchant_city
    FROM completed_transactions ct
    JOIN mock_merchant_locations m
        ON ct.reference_id = m.reference_id
),
alerts_pattern_2 AS (
    SELECT
        customer_id,
        'MULTI_CITY_IMPOSSIBLE_TRAVEL' AS alert_type,
        txn_date AS alert_date,
        json_build_object(
            'rule_name', 'Transactions across >=3 cities in single day',
            'distinct_city_count', COUNT(DISTINCT merchant_city),
            'cities_involved', array_agg(DISTINCT merchant_city),
            'daily_txn_count', COUNT(txn_id),
            'total_daily_volume', ROUND(SUM(amount)::numeric, 2)
        )::text AS details_json
    FROM txns_with_merchant_city
    GROUP BY customer_id, txn_date
    HAVING COUNT(DISTINCT merchant_city) >= 3
),
-- -----------------------------------------------------------------------------
-- POLA 3: Sudden High-Value Spike (> 3x Rata-rata Nilai Transaksi 30 Hari Terakhir)
-- Menggunakan Window Function RANGE BETWEEN (30 hari = 2.592.000 detik)
-- -----------------------------------------------------------------------------
pattern_3_historical_baseline AS (
    SELECT
        customer_id,
        txn_id,
        txn_timestamp,
        txn_date,
        amount,
        -- Rata-rata nominal transaksi nasabah selama 30 hari sebelum transaksi saat ini
        -- (Mengecualikan transaksi saat ini dengan frame PRECEDING ketat)
        AVG(amount) OVER (
            PARTITION BY customer_id
            ORDER BY txn_epoch
            RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING
        ) AS avg_amount_past_30d,
        -- Jumlah riwayat transaksi masa lalu untuk mencegah false positive nasabah baru
        COUNT(txn_id) OVER (
            PARTITION BY customer_id
            ORDER BY txn_epoch
            RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING
        ) AS past_30d_txn_count
    FROM completed_transactions
),
alerts_pattern_3 AS (
    SELECT
        customer_id,
        'HIGH_VALUE_OUTLIER' AS alert_type,
        txn_date AS alert_date,
        json_build_object(
            'rule_name', 'Single txn > 3x 30-day average amount',
            'trigger_txn_id', txn_id,
            'trigger_timestamp', TO_CHAR(txn_timestamp, 'YYYY-MM-DD HH24:MI:SS'),
            'transaction_amount', ROUND(amount::numeric, 2),
            'avg_amount_30d', ROUND(avg_amount_past_30d::numeric, 2),
            'spike_multiplier', ROUND((amount / NULLIF(avg_amount_past_30d, 0.0))::numeric, 2),
            'historical_txn_count_30d', past_30d_txn_count
        )::text AS details_json
    FROM pattern_3_historical_baseline
    WHERE past_30d_txn_count >= 3               
      AND amount > (3.0 * avg_amount_past_30d) 
)
-- -----------------------------------------------------------------------------
-- OUTPUT
-- -----------------------------------------------------------------------------
SELECT 
    customer_id,
    alert_type,
    alert_date,
    details_json
FROM (
    SELECT * FROM alerts_pattern_1
    UNION ALL
    SELECT * FROM alerts_pattern_2
    UNION ALL
    SELECT * FROM alerts_pattern_3
) consolidated_alerts
ORDER BY alert_date DESC, customer_id ASC, alert_type ASC;