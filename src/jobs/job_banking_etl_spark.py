"""
NegaraBank - Production-Grade Daily Account Snapshot Job (Docker / PostgreSQL)
Evaluated under Data Engineer Level 2 Technical Assessment.

Features:
1. Secret & Config Management: Kredensial dinamis via environment variables (tanpa hardcode).
2. Incremental Pushdown: Kueri JDBC difilter langsung di database engine (hanya menarik data tanggal bersangkutan).
3. Parameterisasi Tanggal: Menerima argumen --date YYYY-MM-DD via command line (default kemarin).
4. Validasi Status Transaksi: Hanya memproses transaksi berstatus 'COMPLETED'.
5. Pre-Aggregation Pattern: Mengagregasi transaksi per account_id sebelum join untuk mencegah wide shuffle.
6. Inner Join: Hanya menyertakan akun yang memiliki aktivitas transaksi pada tanggal snapshot.
7. Data Quality Gate: Row count sanity check, null check, rekonsiliasi total nominal & jumlah transaksi.
8. Partisi & Idempoten: Partisi output berdasarkan snapshot_date; penulisan ulang untuk tanggal yang sama idempoten dan dapat di-append untuk tanggal baru.
"""

import argparse
import logging
import os
import shutil
import sys
from datetime import date, datetime, timedelta
from typing import Dict, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count as _count,
    lit,
    sum as _sum,
    when,
)

# Setup Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("NegaraBank.AccountSnapshotDocker")


class DataQualityException(Exception):
    """Raised when data quality gate assertions fail."""
    pass


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Production-Grade Daily Account Snapshot Job (Docker / PostgreSQL)"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Snapshot date in YYYY-MM-DD format (default: yesterday)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=os.getenv("OUTPUT_DIR", "/opt/negarabank/job_soal_1_docker_output"),
        help="Target output directory for partitioned Parquet",
    )
    parser.add_argument(
        "--join-type",
        type=str,
        default="inner",
        choices=["inner", "left"],
        help="Join type between accounts and transactions (default: inner)",
    )
    return parser.parse_args()


def validate_date(date_str: str) -> str:
    """Validate and return date string in YYYY-MM-DD format."""
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d").date()
        return parsed.strftime("%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid date format: '{date_str}'. Expected YYYY-MM-DD.") from exc


def get_db_credentials() -> Dict[str, str]:
    """Retrieve database credentials securely from environment variables."""
    postgres_host = os.getenv("POSTGRES_HOST", "host.docker.internal")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_database = os.getenv("POSTGRES_DB", "belajar_db")
    postgres_user = os.getenv("POSTGRES_USER", "postgres")
    postgres_password = os.getenv("POSTGRES_PASSWORD")

    if not postgres_password:
        raise RuntimeError(
            "POSTGRES_PASSWORD environment variable is missing or empty. "
            "Please provide POSTGRES_PASSWORD in the environment."
        )

    return {
        "host": postgres_host,
        "port": postgres_port,
        "database": postgres_database,
        "user": postgres_user,
        "password": postgres_password,
        "url": f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}",
    }


def init_spark() -> SparkSession:
    """Initialize SparkSession with dynamic partition overwrite and performance configs."""
    return (
        SparkSession.builder.appName("account_snapshot_docker_prod")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def extract_data(spark: SparkSession, creds: Dict[str, str], snapshot_date: str) -> Dict[str, DataFrame]:
    """
    Extract data incrementally from PostgreSQL with predicate pushdown.
    Only transactions for the target date with status 'COMPLETED' are loaded into Spark.
    """
    logger.info("Extracting accounts from PostgreSQL (bronze.accounts)...")
    accounts_df = (
        spark.read.format("jdbc")
        .option("url", creds["url"])
        .option("dbtable", "bronze.accounts")
        .option("user", creds["user"])
        .option("password", creds["password"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )

    logger.info(
        "Extracting incremental transactions for snapshot_date=%s with status='COMPLETED'...",
        snapshot_date,
    )
    # Pushdown subquery directly to PostgreSQL engine to avoid full table scan
    txns_pushdown_query = f"""
    (SELECT txn_id, account_id, txn_date, txn_type, amount, status
     FROM bronze.transactions
     WHERE txn_date >= '{snapshot_date} 00:00:00'
       AND txn_date <= '{snapshot_date} 23:59:59'
       AND status = 'COMPLETED') AS daily_txns
    """

    daily_txns_df = (
        spark.read.format("jdbc")
        .option("url", creds["url"])
        .option("dbtable", txns_pushdown_query)
        .option("user", creds["user"])
        .option("password", creds["password"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )

    return {"accounts": accounts_df, "daily_txns": daily_txns_df}


def transform_data(
    accounts_df: DataFrame,
    daily_txns_df: DataFrame,
    snapshot_date: str,
    join_type: str = "inner",
) -> DataFrame:
    """
    Transform data using Pre-Aggregation pattern:
    1. Pre-aggregate transactions per account_id to eliminate wide shuffle.
    2. Join accounts master with aggregated transactions.
    3. Add snapshot_date column for partitioning.
    """
    logger.info("Pre-aggregating daily transactions by account_id...")
    txn_summary = daily_txns_df.groupBy("account_id").agg(
        _sum("amount").alias("daily_txn_amount"),
        _count("txn_id").alias("daily_txn_count"),
        _sum(
            when(
                col("txn_type").isin("DEBIT", "TRANSFER_OUT", "PAYMENT", "FEE"),
                col("amount"),
            ).otherwise(0)
        ).alias("daily_debit_amount"),
        _sum(
            when(
                col("txn_type").isin("CREDIT", "TRANSFER_IN"),
                col("amount"),
            ).otherwise(0)
        ).alias("daily_credit_amount"),
    )

    logger.info("Joining accounts with transaction summary (join_type='%s')...", join_type)
    snapshot = (
        accounts_df.join(txn_summary, on="account_id", how=join_type)
        .withColumn("daily_txn_amount", coalesce(col("daily_txn_amount"), lit(0.0)))
        .withColumn("daily_txn_count", coalesce(col("daily_txn_count"), lit(0)))
        .withColumn("daily_debit_amount", coalesce(col("daily_debit_amount"), lit(0.0)))
        .withColumn("daily_credit_amount", coalesce(col("daily_credit_amount"), lit(0.0)))
        .withColumn("snapshot_date", lit(snapshot_date))
        .select(
            "snapshot_date",
            "account_id",
            "customer_id",
            "account_type",
            "product_name",
            "opened_date",
            "status",
            "balance",
            "credit_limit",
            "daily_txn_count",
            "daily_txn_amount",
            "daily_debit_amount",
            "daily_credit_amount",
        )
    )

    return snapshot


def validate_data_quality(
    snapshot_df: DataFrame,
    accounts_df: DataFrame,
    daily_txns_df: DataFrame,
    join_type: str = "inner",
) -> None:
    """
    Data Quality Gate: Validates snapshot integrity before persisting.
    - Sanity check row count (non-empty, matches distinct accounts)
    - Primary key uniqueness & critical columns null check
    - Balance & volume reconciliation against raw source transactions
    """
    logger.info("Running Data Quality assertions...")

    snapshot_count = snapshot_df.count()
    logger.info("Total rows in snapshot: %d", snapshot_count)

    if snapshot_count == 0:
        raise DataQualityException("Snapshot dataframe is empty! No records to write.")

    # 1. Null check on critical columns
    null_count = snapshot_df.filter(
        col("account_id").isNull()
        | col("customer_id").isNull()
        | col("balance").isNull()
        | col("snapshot_date").isNull()
        | col("daily_txn_count").isNull()
    ).count()
    if null_count > 0:
        raise DataQualityException(
            f"Null check failed! Found {null_count} rows with NULL in critical columns."
        )

    # 2. Primary key uniqueness check
    distinct_accounts = snapshot_df.select("account_id").distinct().count()
    if distinct_accounts != snapshot_count:
        raise DataQualityException(
            f"Uniqueness check failed! Expected {snapshot_count} unique accounts, got {distinct_accounts}."
        )

    # 3. Non-negative daily transaction count check
    negative_txns = snapshot_df.filter(col("daily_txn_count") < 0).count()
    if negative_txns > 0:
        raise DataQualityException(
            f"Negative count check failed! Found {negative_txns} rows with daily_txn_count < 0."
        )

    # 4. Reconciliation check: sum of amount and transaction count
    raw_metrics = daily_txns_df.select(
        _count("txn_id").alias("raw_txn_count"),
        _sum("amount").alias("raw_txn_amount"),
    ).collect()[0]

    raw_txn_count = raw_metrics["raw_txn_count"] or 0
    raw_txn_sum = float(raw_metrics["raw_txn_amount"] or 0.0)

    snapshot_metrics = snapshot_df.select(
        _sum("daily_txn_count").alias("snap_txn_count"),
        _sum("daily_txn_amount").alias("snap_txn_amount"),
    ).collect()[0]

    snap_txn_count = snapshot_metrics["snap_txn_count"] or 0
    snap_txn_sum = float(snapshot_metrics["snap_txn_amount"] or 0.0)

    if raw_txn_count != snap_txn_count:
        raise DataQualityException(
            f"Reconciliation error (txn_count)! Raw daily count ({raw_txn_count}) "
            f"!= Snapshot sum ({snap_txn_count})"
        )

    if abs(raw_txn_sum - snap_txn_sum) > 0.01:
        raise DataQualityException(
            f"Reconciliation error (amount)! Raw daily sum ({raw_txn_sum:.2f}) "
            f"!= Snapshot sum ({snap_txn_sum:.2f})"
        )

    logger.info("All Data Quality assertions PASSED successfully!")


def save_snapshot_idempotent(
    snapshot_df: DataFrame,
    output_dir: str,
    snapshot_date: str,
) -> None:
    """
    Persist snapshot partitioned by snapshot_date with idempotent upsert/append.
    Re-running for the same snapshot_date replaces that partition cleanly without duplication.
    """
    logger.info(
        "Persisting snapshot to '%s' partitioned by snapshot_date=%s...",
        output_dir,
        snapshot_date,
    )

    partition_path = os.path.join(output_dir, f"snapshot_date={snapshot_date}")
    if os.path.exists(partition_path):
        logger.info(
            "Found existing partition for '%s'. Removing old partition to ensure idempotence...",
            snapshot_date,
        )
        shutil.rmtree(partition_path)

    # Write partitioned Parquet with coalesce(1) to keep 1 file per partition
    (
        snapshot_df.coalesce(1)
        .write.mode("append")
        .partitionBy("snapshot_date")
        .parquet(output_dir)
    )

    logger.info(
        "Snapshot for %s successfully persisted at %s",
        snapshot_date,
        partition_path,
    )


def main() -> None:
    args = parse_arguments()

    if args.date:
        target_date = validate_date(args.date)
    else:
        target_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info("Starting Daily Account Snapshot Job for date: %s", target_date)

    creds = get_db_credentials()
    spark = init_spark()

    try:
        # 1. Extract
        extracted = extract_data(spark, creds, target_date)
        accounts_df = extracted["accounts"]
        daily_txns_df = extracted["daily_txns"]

        # Cache for multiple evaluations (quality checks + persistence)
        daily_txns_df.cache()

        # 2. Transform (Pre-aggregation + Join)
        snapshot_df = transform_data(
            accounts_df=accounts_df,
            daily_txns_df=daily_txns_df,
            snapshot_date=target_date,
            join_type=args.join_type,
        )
        snapshot_df.cache()

        # 3. Data Quality Gate
        validate_data_quality(
            snapshot_df=snapshot_df,
            accounts_df=accounts_df,
            daily_txns_df=daily_txns_df,
            join_type=args.join_type,
        )

        # 4. Load (Idempotent Partitioned Write)
        save_snapshot_idempotent(
            snapshot_df=snapshot_df,
            output_dir=args.output_dir,
            snapshot_date=target_date,
        )

        logger.info("Job completed successfully for snapshot_date=%s!", target_date)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
