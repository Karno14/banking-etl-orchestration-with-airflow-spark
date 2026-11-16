"""
NegaraBank - Daily Account Snapshot ETL Pipeline (Silver Layer)
Level-2 Technical Assessment - Question 1b

Requirements Implemented:
1. Incremental Loading (Processes only transactions for the target snapshot date).
2. Secret Management (AWS Secrets Manager integration with local env fallback).
3. Optimized Pre-Aggregation & Join (Eliminates wide-shuffle OOM risks).
4. Data Quality Gate (Row count sanity check, primary key null/duplicate checks, reconciliation).
5. Idempotent Partitioned Storage (Delta Lake / Parquet partitioned by snapshot_date).
6. Enterprise Logging & Error Handling.
"""

import sys
import os
import argparse
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, sum as _sum, count as _count, when, coalesce
)

# -------------------------------------------------------------------------
# 1. Structured Logging Configuration
# -------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("NegaraBank.AccountSnapshotETL")


# -------------------------------------------------------------------------
# 2. Secret Management Helper
# -------------------------------------------------------------------------
def get_database_credentials(secret_name: str, region_name: str = "ap-southeast-3") -> Dict[str, str]:
    """
    Retrieves database credentials securely from AWS Secrets Manager.
    Falls back to environment variables for local testing / containerized runs.
    """
    # Fallback to environment variables if AWS SDK is unavailable or running locally
    if os.getenv("ENV", "local").lower() == "local":
        logger.info("Running in LOCAL mode. Loading credentials from environment variables.")
        return {
            "username": os.getenv("DB_USER", "etl_user"),
            "password": os.getenv("DB_PASSWORD", "etl_pass123"),
            "host": os.getenv("DB_HOST", "core-db"),
            "port": os.getenv("DB_PORT", "1521"),
            "service": os.getenv("DB_SERVICE", "banking")
        }

    try:
        import boto3
        from botocore.exceptions import ClientError

        logger.info(f"Fetching secret '{secret_name}' from AWS Secrets Manager ({region_name})...")
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        
        secret_dict = json.loads(response["SecretString"])
        return secret_dict
    except Exception as e:
        logger.error(f"Failed to fetch secret from AWS Secrets Manager: {str(e)}")
        raise


# -------------------------------------------------------------------------
# 3. Spark Session Factory
# -------------------------------------------------------------------------
def create_spark_session(app_name: str = "negarabank_account_snapshot_etl") -> SparkSession:
    """
    Initializes a production-tuned Spark Session.
    Configures dynamic partition overwrite for idempotent S3 writes.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


# -------------------------------------------------------------------------
# 4. Data Quality Gate (Assertions)
# -------------------------------------------------------------------------
class DataQualityException(Exception):
    """Raised when data quality gate assertions fail."""
    pass


def run_data_quality_checks(
    snapshot_df: DataFrame,
    accounts_source_df: DataFrame,
    daily_txns_df: DataFrame,
    snapshot_date: str
) -> None:
    """
    Validates data integrity before landing to the Silver layer.
    """
    logger.info("Executing Data Quality Gate assertions...")
    
    total_snapshot_records = snapshot_df.count()
    total_accounts_source = accounts_source_df.count()

    # Check 1: Row count sanity check
    # Snapshot must have 1-to-1 match with the accounts master table
    logger.info(f"Sanity Check: accounts_source={total_accounts_source}, snapshot={total_snapshot_records}")
    if total_snapshot_records != total_accounts_source:
        raise DataQualityException(
            f"Row count mismatch! Source accounts ({total_accounts_source}) != Snapshot records ({total_snapshot_records})"
        )

    # Check 2: Primary Key & Mandatory Columns Null Check
    null_counts = snapshot_df.filter(
        col("account_id").isNull() | 
        col("customer_id").isNull() | 
        col("balance").isNull()
    ).count()

    if null_counts > 0:
        raise DataQualityException(f"Found {null_counts} records with NULL primary or critical keys in snapshot!")

    # Check 3: Uniqueness of account_id
    distinct_accounts = snapshot_df.select("account_id").distinct().count()
    if distinct_accounts != total_snapshot_records:
        duplicates = total_snapshot_records - distinct_accounts
        raise DataQualityException(f"Duplicate account_id detected in snapshot! Count: {duplicates}")

    # Check 4: Transaction Aggregation Sanity & Reconciliation
    negative_counts = snapshot_df.filter(col("daily_txn_count") < 0).count()
    if negative_counts > 0:
        raise DataQualityException(f"Found {negative_counts} records with negative daily_txn_count!")

    # Check 5: Total volume reconciliation between raw txns and snapshot sum
    raw_txns_sum = daily_txns_df.select(_sum("amount")).collect()[0][0] or 0.0
    snapshot_txns_sum = snapshot_df.select(_sum("daily_txn_amount")).collect()[0][0] or 0.0
    
    # Toleransi selisih floating point (Rp 1.00)
    if abs(float(raw_txns_sum) - float(snapshot_txns_sum)) > 1.0:
        raise DataQualityException(
            f"Reconciliation error! Raw transactions sum ({raw_txns_sum}) != Snapshot sum ({snapshot_txns_sum})"
        )

    logger.info(" All Data Quality Gate assertions PASSED successfully!")


# -------------------------------------------------------------------------
# 5. Pipeline Execution
# -------------------------------------------------------------------------
def run_pipeline(snapshot_date: str, data_dir: str, output_dir: str) -> None:
    """
    Main ETL workflow:
    1. Reads accounts and incremental transactions (date filtered, status COMPLETED).
    2. Performs pre-aggregation on transactions by account_id.
    3. Performs left join to attach daily activity to accounts snapshot.
    4. Executes data quality assertions.
    5. Writes partitioned output idempotently.
    """
    logger.info(f"============================================================")
    logger.info(f"Starting NegaraBank Daily Account Snapshot ETL for: {snapshot_date}")
    logger.info(f"============================================================")

    spark = create_spark_session()

    try:
        # Step A: Load Accounts Master Data
        accounts_path = os.path.join(data_dir, "accounts.csv")
        logger.info(f"Loading Accounts from: {accounts_path}")
        accounts_df = spark.read.csv(accounts_path, header=True, inferSchema=True)
        
        # Step B: Load Transactions with Incremental Filter
        txns_path = os.path.join(data_dir, "transactions.csv")
        logger.info(f"Loading Incremental Transactions for {snapshot_date} from: {txns_path}")
        
        raw_txns_df = spark.read.csv(txns_path, header=True, inferSchema=True)
        
        # Incremental filter: strictly between [snapshot_date 00:00:00, snapshot_date 23:59:59]
        # Business rule: Only COMPLETED transactions count towards official daily aggregates
        start_ts = f"{snapshot_date} 00:00:00"
        end_ts = f"{snapshot_date} 23:59:59"
        
        incremental_txns = raw_txns_df.filter(
            (col("txn_date") >= lit(start_ts)) &
            (col("txn_date") <= lit(end_ts)) &
            (col("status") == "COMPLETED")
        )

        # Step C: Pre-Aggregation by account_id (prevents wide shuffle OOM)
        logger.info("Executing Pre-Aggregation on transactions by account_id...")
        txn_summary = incremental_txns.groupBy("account_id").agg(
            _sum("amount").alias("daily_txn_amount"),
            _count("txn_id").alias("daily_txn_count"),
            _sum(when(col("txn_type").isin("DEBIT", "TRANSFER_OUT", "PAYMENT", "FEE"), col("amount")).otherwise(0)).alias("daily_debit_amount"),
            _sum(when(col("txn_type").isin("CREDIT", "TRANSFER_IN"), col("amount")).otherwise(0)).alias("daily_credit_amount")
        )

        # Step D: Left Join Accounts with Daily Aggregates
        logger.info("Joining Accounts with Daily Transaction Summary...")
        snapshot_df = (
            accounts_df
            .join(txn_summary, on="account_id", how="left")
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
                "interest_rate",
                "daily_txn_amount",
                "daily_txn_count",
                "daily_debit_amount",
                "daily_credit_amount"
            )
        )

        # Cache snapshot dataframe for DQ checks and writing
        snapshot_df.cache()

        # Step E: Data Quality Gate Assertions
        run_data_quality_checks(
            snapshot_df=snapshot_df,
            accounts_source_df=accounts_df,
            daily_txns_df=incremental_txns,
            snapshot_date=snapshot_date
        )

        # Step F: Idempotent Partitioned Write
        target_silver_path = os.path.join(output_dir, "account_snapshots")
        logger.info(f"Writing idempotent Silver snapshot to: {target_silver_path} (partition: snapshot_date={snapshot_date})")
        
        (
            snapshot_df
            .coalesce(2)
            .write
            .mode("overwrite")
            .partitionBy("snapshot_date")
            .parquet(target_silver_path)
        )

        logger.info(f" Successfully processed and wrote snapshot for date: {snapshot_date}")
        logger.info(f"Total Snapshot Records Landed: {snapshot_df.count()}")

    except Exception as e:
        logger.critical(f"Pipeline execution FAILED: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped cleanly.")


# -------------------------------------------------------------------------
# 6. Entry Point CLI
# -------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NegaraBank Daily Account Snapshot ELT")
    parser.add_argument(
        "--snapshot_date",
        type=str,
        default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Target snapshot date in YYYY-MM-DD format (defaults to yesterday)."
    )
    parser.add_argument(
        "--data_dir",
        type=str,
        default="/opt/negarabank/negarabank_dataset",
        help="Input dataset directory."
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="/opt/negarabank/silver_output",
        help="Silver layer destination directory."
    )

    args = parser.parse_args()

    # Validate date format
    try:
        datetime.strptime(args.snapshot_date, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid date format: {args.snapshot_date}. Expected YYYY-MM-DD.")
        sys.exit(1)

    run_pipeline(
        snapshot_date=args.snapshot_date,
        data_dir=args.data_dir,
        output_dir=args.output_dir
    )
