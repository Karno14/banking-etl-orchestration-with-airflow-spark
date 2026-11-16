from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, count

spark = SparkSession.builder.master("local[2]").appName("check_dates").getOrCreate()
df = spark.read.csv("/opt/negarabank/negarabank_dataset/transactions.csv", header=True, inferSchema=True)
print("=== Transaction Stats ===")
df.select(min("txn_date").alias("min_date"), max("txn_date").alias("max_date"), count("*").alias("total_txns")).show()

# Sample of transactions on a specific date
sample_date = "2026-01-15"
daily_df = df.filter((col("txn_date") >= f"{sample_date} 00:00:00") & (col("txn_date") <= f"{sample_date} 23:59:59"))
print(f"Transactions on {sample_date}: {daily_df.count()}")
daily_df.groupBy("status").count().show()

spark.stop()
