"""
DAG: credit_scoring_daily_inference
Author: Tim Credit Scoring Negarabank
Deskripsi:
  Pipeline downstream inference Credit Scoring yang di-trigger otomatis
  oleh `negarabank_account_snapshot_etl` setelah data snapshot siap (< 30 menit).
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def _run_credit_scoring_inference(**context):
    dag_run_conf = context.get("dag_run").conf or {}
    snapshot_date = dag_run_conf.get("snapshot_date", context.get("ds"))
    print(f"===========================================================")
    print(f"[CREDIT SCORING] Menerima trigger data snapshot untuk tanggal: {snapshot_date}")
    print(f"[CREDIT SCORING] Menjalankan scoring model ML...")
    print(f"[CREDIT SCORING] Scoring selesai sebelum jam 08:00 AM!")
    print(f"===========================================================")
    return True

with DAG(
    dag_id="credit_scoring_daily_inference",
    schedule_interval=None,  # Hanya dijalankan melalui external trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["downstream", "credit-scoring", "ml-inference"]
) as dag:

    run_inference = PythonOperator(
        task_id="run_credit_scoring_model",
        python_callable=_run_credit_scoring_inference,
        provide_context=True
    )
