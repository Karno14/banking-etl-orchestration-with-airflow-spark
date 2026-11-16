## versi docker
import os
import sys
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterator, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, sum as _sum, count as _count, when, coalesce

from src.db.secret_manager import SecretManager

logger = logging.getLogger("NegaraBank.AccountSnapshotJob")


def _parse_date(value: str, argument_name: str) -> date:
    try:
        parsed_date = datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{argument_name} must use YYYY-MM-DD format: {value!r}") from exc

    if parsed_date.isoformat() != value:
        raise ValueError(f"{argument_name} must use YYYY-MM-DD format: {value!r}")
    return parsed_date


def resolve_date_range(
    execution_date: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Tuple[str, str]:
    range_requested = start_date is not None or end_date is not None
    if range_requested:
        if start_date is None or end_date is None:
            raise ValueError("Both --startdate and --enddate must be provided.")
        if execution_date is not None:
            raise ValueError("Use either --date or the --startdate/--enddate range, not both.")
        first_date = _parse_date(start_date, "startdate")
        last_date = _parse_date(end_date, "enddate")
    else:
        selected_date = (
            execution_date
            if execution_date is not None
            else (date.today() - timedelta(days=1)).isoformat()
        )
        first_date = last_date = _parse_date(selected_date, "date")

    if first_date > last_date:
        raise ValueError("startdate must be on or before enddate.")
    return first_date.isoformat(), last_date.isoformat()


def iter_dates(start_date: str, end_date: str) -> Iterator[str]:
    current_date = _parse_date(start_date, "startdate")
    last_date = _parse_date(end_date, "enddate")
    if current_date > last_date:
        raise ValueError("startdate must be on or before enddate.")
    while current_date <= last_date:
        yield current_date.isoformat()
        current_date += timedelta(days=1)


class DataQualityException(Exception):
    """Raised when data quality gate assertions fail."""
    pass

class DataQualityValidator:
    """Quality Gate for validating snapshot data integrity."""
    @staticmethod
    def validate_snapshot(snapshot_df: DataFrame, source_accounts_df: DataFrame, daily_txns_df: DataFrame) -> None:
        logger.info("Executing Data Quality assertions...")

        total_snapshot = snapshot_df.count()
        total_source = source_accounts_df.count()
        if total_snapshot != total_source:
            raise DataQualityException(
                f"Row count mismatch! Source accounts ({total_source}) != Snapshot ({total_snapshot})"
            )

        null_counts = snapshot_df.filter(
            col("account_id").isNull() | 
            col("customer_id").isNull() | 
            col("balance").isNull()
        ).count()
        if null_counts > 0:
            raise DataQualityException(f"Detected {null_counts} rows with NULL in primary/critical columns!")

        distinct_accounts = snapshot_df.select("account_id").distinct().count()
        if distinct_accounts != total_snapshot:
            raise DataQualityException("Duplicate account_id detected in snapshot!")

        negative_counts = snapshot_df.filter(col("daily_txn_count") < 0).count()
        if negative_counts > 0:
            raise DataQualityException("Found negative daily_txn_count values!")

        raw_txns_sum = daily_txns_df.select(_sum("amount")).collect()[0][0] or 0.0
        snapshot_txns_sum = snapshot_df.select(_sum("daily_txn_amount")).collect()[0][0] or 0.0
        if abs(float(raw_txns_sum) - float(snapshot_txns_sum)) > 1.0:
            raise DataQualityException(
                f"Reconciliation error! Raw sum ({raw_txns_sum}) != Snapshot sum ({snapshot_txns_sum})"
            )

        logger.info(" All Data Quality assertions PASSED!")

class AccountSnapshotJob:
    """
    Production ETL Job: Daily Account Snapshot
    Reads accounts master and incremental transactions, performs pre-aggregation,
    runs DQ checks, and idempotently writes partitioned snapshot to Silver layer.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        execution_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ):
        self.config = config
        self.start_date, self.end_date = resolve_date_range(
            execution_date=execution_date,
            start_date=start_date,
            end_date=end_date,
        )
        self.execution_date = self.start_date
        self.spark = self._init_spark()

    def _init_spark(self) -> SparkSession:
        app_name = self.config.get("spark", {}).get("app_name", "negarabank_account_snapshot_etl")
        master = self.config.get("spark", {}).get("master", "local[*]")
        return (
            SparkSession.builder
            .appName(app_name)
            .master(master)
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

    def extract(self) -> Dict[str, Any]:
        raw_dir = self.config["paths"]["raw_data_dir"]
        logger.info(f"Extracting source data from {raw_dir}...")

        accounts_df = self.spark.read.csv(os.path.join(raw_dir, "accounts.csv"), header=True, inferSchema=True)
        raw_txns_df = self.spark.read.csv(os.path.join(raw_dir, "transactions.csv"), header=True, inferSchema=True)

        start_ts = f"{self.execution_date} 00:00:00"
        end_ts = f"{self.execution_date} 23:59:59"

        # Incremental filter
        incremental_txns_df = raw_txns_df.filter(
            (col("txn_date") >= lit(start_ts)) &
            (col("txn_date") <= lit(end_ts)) &
            (col("status") == "COMPLETED")
        )

        return {
            "accounts": accounts_df,
            "incremental_txns": incremental_txns_df
        }

    def transform(self, extracted: Dict[str, Any]) -> Any:
        logger.info("Transforming: Pre-aggregating transactions by account_id...")
        accounts_df = extracted["accounts"]
        incremental_txns_df = extracted["incremental_txns"]

        txn_summary = incremental_txns_df.groupBy("account_id").agg(
            _sum("amount").alias("daily_txn_amount"),
            _count("txn_id").alias("daily_txn_count"),
            _sum(when(col("txn_type").isin("DEBIT", "TRANSFER_OUT", "PAYMENT", "FEE"), col("amount")).otherwise(0)).alias("daily_debit_amount"),
            _sum(when(col("txn_type").isin("CREDIT", "TRANSFER_IN"), col("amount")).otherwise(0)).alias("daily_credit_amount")
        )

        logger.info("Joining Accounts with Transaction Summary...")
        snapshot_df = (
            accounts_df
            .join(txn_summary, on="account_id", how="left")
            .withColumn("daily_txn_amount", coalesce(col("daily_txn_amount"), lit(0.0)))
            .withColumn("daily_txn_count", coalesce(col("daily_txn_count"), lit(0)))
            .withColumn("daily_debit_amount", coalesce(col("daily_debit_amount"), lit(0.0)))
            .withColumn("daily_credit_amount", coalesce(col("daily_credit_amount"), lit(0.0)))
            .withColumn("snapshot_date", lit(self.execution_date))
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
        snapshot_df.cache()
        return snapshot_df

    def validate(self, transformed_df: Any, extracted: Dict[str, Any]) -> None:
        DataQualityValidator.validate_snapshot(
            snapshot_df=transformed_df,
            source_accounts_df=extracted["accounts"],
            daily_txns_df=extracted["incremental_txns"]
        )

    def load(self, validated_df: Any) -> None:
        out_dir = os.path.join(self.config["paths"]["silver_output_dir"], "account_snapshots")
        logger.info(f"Writing idempotent snapshot to: {out_dir} (partition snapshot_date={self.execution_date})")

        (
            validated_df
            .coalesce(2)
            .write
            .mode("overwrite")
            .partitionBy("snapshot_date")
            .parquet(out_dir)
        )
        logger.info(f"Snapshot Job for {self.execution_date} completed successfully!")

    def run(self) -> None:
        for execution_date in iter_dates(self.start_date, self.end_date):
            self.execution_date = execution_date
            logger.info("Starting account snapshot for %s", execution_date)
            extracted = self.extract()
            transformed = self.transform(extracted)
            try:
                self.validate(transformed, extracted)
                self.load(transformed)
            finally:
                transformed.unpersist()
