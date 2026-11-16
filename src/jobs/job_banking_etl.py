from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("account_snapshot").getOrCreate()

accounts = (
    spark.read.format("jdbc")
    .option("url", "jdbc:oracle:thin:@core-db:1521/banking")
    .option("dbtable", "ACCOUNTS")
    .option("user", "etl_user")
    .option("password", "etl_pass123") #1. credential harus disimpan terpisah di config
    .load()
)

txns = (
    spark.read.format("jdbc")  #2. membaca full table tanpa filter, bisa menggunakan partisioning untuk filter data, misal berdasarkan tanggal transaksi
    .option("url", "jdbc:oracle:thin:@core-db:1521/banking")
    .option("dbtable", "TRANSACTIONS") #4 Transaksi berstatus `FAILED`, `PENDING`, dan `REVERSED` ikut terhitung ke dalam volume transaksi | Tambahkan filter validasi status transaksi: `WHERE status = 'COMPLETED'` |
    .load()
)

today_txns = txns.filter(txns.txn_date >= "2026-01-01")     #3. gunakan parameter untuk filter tanggal berdasarkan argumen date yang diterima dari comand line

snapshot = (
    accounts.join(today_txns, "account_id", "left")   #5.Join dilakukan sebelum agregasi, sehingga transaksi yang tidak memiliki akun terkait akan tetap muncul di hasil snapshot. 
    .groupBy("account_id", "customer_id", "account_type", "balance") #| Gunakan join type `inner` untuk hanya menyertakan akun yang memiliki transaksi terkait.
    .agg({"amount": "sum", "txn_id": "count"})
)

snapshot.write.mode("overwrite").parquet( #6. replace/upsert data lama dengan data baru, gunakan mode `append` untuk menambahkan data baru ke hasil snapshot yang sudah ada.
    "s3://negarabank-silver/account_snapshots/"
)


##perbaikan tambahan:
#1. credential harus disimpan terpisah di config
#2. membaca full table tanpa filter, gunakan partisioning untuk filter data, misal berdasarkan tanggal transaksi dan status transaksi, sehingga tidak semua data di load ke memory
#3. gunakan parameter untuk filter tanggal berdasarkan argumen date yang diterima dari comand line
#4 Transaksi berstatus `FAILED`, `PENDING`, dan `REVERSED` ikut terhitung ke dalam volume transaksi | Tambahkan filter validasi status transaksi: `WHERE status = 'COMPLETED'` |
#5.Join dilakukan sebelum agregasi, sehingga transaksi yang tidak memiliki akun terkait akan tetap muncul di hasil snapshot. 
#6. Gunakan join type `inner` untuk hanya menyertakan akun yang memiliki transaksi terkait.
#7. replace/upsert data lama dengan data baru, gunakan mode `append` untuk menambahkan data baru ke hasil snapshot yang sudah ada.