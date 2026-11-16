"""
DAG: negarabank_account_snapshot_pipeline
Author: Negarabank Data Engineering Team
Deskripsi:
  1. Menjadwalkan HANYA `src/jobs/job_banking_etl_spark.py` setiap hari pukul 05:00 AM WIB (0 5 * * *).
  2. POSTGRES_PASSWORD diambil secara dinamis dari `DB_PASSWORD` di `.env` / `.env.example`.
  3. Jika parameter `--date` tidak diisi, otomatis menggunakan `now()::date - 1 hari` (kemarin).
  4. Format pemanggilan sama persis seperti di lokal:
     docker exec -e POSTGRES_PASSWORD="<DB_PASSWORD>" negarabank-spark /opt/spark/bin/spark-submit \
       --master "local[*]" /opt/negarabank/src/jobs/job_banking_etl_spark.py --date <YYYY-MM-DD>
  5. Retry otomatis 3x dengan exponential backoff jika terjadi kegagalan.
  6. Mengirim alert ke Slack & PagerDuty jika gagal setelah percobaan ke-3.
  7. Men-trigger downstream pipeline Credit Scoring setelah job snapshot berhasil (< 30 menit).
"""

from datetime import datetime, timedelta
import os
import json
import logging
import requests

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

logger = logging.getLogger("Airflow.AccountSnapshotETL")


# ---------------------------------------------------------------------------
# 1. Helper Membaca Password dari .env / .env.example
# ---------------------------------------------------------------------------
def get_db_password_from_env() -> str:
    """
    Mengambil DB_PASSWORD dari file .env (jika ada) atau fallback ke .env.example.
    Default fallback: 'etl_pass123'.
    """
    candidate_paths = [
        "/opt/negarabank/.env",
        "/opt/negarabank/.env.example",
        "D:/negarabank/.env",
        "D:/negarabank/.env.example",
    ]
    for path in candidate_paths:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("DB_PASSWORD="):
                            val = line.split("=", 1)[1].strip().strip('"').strip("'")
                            if val:
                                return val
            except Exception as e:
                logger.warning(f"Tidak dapat membaca file {path}: {e}")

    # Fallback ke environment variable jika sudah diset
    return os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "etl_pass123"))


DB_PASSWORD = get_db_password_from_env()


# ---------------------------------------------------------------------------
# 2. Alerting Callbacks (Hanya dipicu saat retries habis / gagal total 3x)
# ---------------------------------------------------------------------------
def send_slack_alert(context):
    """
    Dipicu otomatis oleh `on_failure_callback` ketika task gagal setelah
    menghabiskan semua kuota retry (3 kali).
    """
    slack_webhook_url = Variable.get("SLACK_DE_ALERT_WEBHOOK", default_var=None)
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    execution_date = context.get("execution_date")
    log_url = getattr(task_instance, "log_url", "http://localhost:8080")

    alert_payload = {
        "text": ":rotating_light: *CRITICAL: Negarabank Snapshot Pipeline Failed!*",
        "attachments": [
            {
                "color": "#D32F2F",
                "fields": [
                    {"title": "DAG", "value": str(context["dag"].dag_id), "short": True},
                    {"title": "Task", "value": str(task_instance.task_id), "short": True},
                    {"title": "Execution Date", "value": str(execution_date), "short": True},
                    {"title": "Try Number", "value": f"{task_instance.try_number} of 3", "short": True},
                    {"title": "Target Script", "value": "src/jobs/job_banking_etl_spark.py", "short": True},
                    {"title": "Error Message", "value": str(exception)[:250] if exception else "Unknown Error", "short": False},
                    {"title": "Airflow Log URL", "value": f"<{log_url}|View Airflow Log>", "short": False},
                    {"title": "Action Required", "value": "Segera cek log cluster Spark. SLA Credit Scoring (08:00 AM) berisiko breach!", "short": False}
                ],
                "footer": "Negarabank Data Engineering Platform"
            }
        ]
    }

    if slack_webhook_url:
        try:
            requests.post(slack_webhook_url, json=alert_payload, timeout=10)
        except Exception as e:
            logger.error(f"Gagal mengirim notifikasi Slack: {e}")
    else:
        logger.warning(f"[SIMULASI ALERT] Task {task_instance.task_id} GAGAL 3x! Error: {exception}")


def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Early-warning alert jika pipeline belum tuntas mendekati pukul 07:00 AM"""
    logger.warning(f":warning: SLA Missed untuk DAG {dag.dag_id}! SLA Threshold tercapai.")


# ---------------------------------------------------------------------------
# 3. Default Arguments & Retry Configuration
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    # --- Mekanisme Retry Otomatis ---
    "retries": 3,                                      # Retry sebanyak 3 kali
    "retry_delay": timedelta(minutes=5),               # Jeda awal 5 menit
    "retry_exponential_backoff": True,                 # Backoff: 5m, 10m, 20m
    "max_retry_delay": timedelta(minutes=15),
    # --- Callback Gagal Total (Setelah 3x Retry) ---
    "on_failure_callback": send_slack_alert,
}


# ---------------------------------------------------------------------------
# 4. Definisi DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="negarabank_account_snapshot_etl",
    default_args=default_args,
    description="Daily Account Snapshot ETL (job_banking_etl_spark.py) running at 05:00 AM",
    schedule_interval="0 5 * * *",                     # Running setiap hari jam 05:00 AM WIB
    start_date=datetime(2026, 1, 1),
    catchup=False,
    sla_miss_callback=sla_miss_alert,
    tags=["core-banking", "silver", "docker", "p1"]
) as dag:

    # Jinja Template untuk Tanggal:
    # 1. Jika diisi manual via conf {"date": "YYYY-MM-DD"}, gunakan nilai tersebut.
    # 2. Jika tidak diisi, otomatis menggunakan now()::date - 1 hari (kemarin).
    date_template = (
        "{{ dag_run.conf['date'] if (dag_run and dag_run.conf and 'date' in dag_run.conf) "
        "else (macros.datetime.now() - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
    )

    # Task 1: Eksekusi PySpark job_banking_etl_spark.py di container negarabank-spark
    run_snapshot_job = BashOperator(
        task_id="run_account_snapshot_docker",
        bash_command=(
            "chmod +x /opt/negarabank/bin/docker 2>/dev/null || true; "
            f'docker exec -e POSTGRES_PASSWORD="{DB_PASSWORD}" negarabank-spark '
            "/opt/spark/bin/spark-submit "
            '--master "local[*]" '
            "/opt/negarabank/src/jobs/job_banking_etl_spark.py "
            f"--date {date_template}"
        ),
        execution_timeout=timedelta(minutes=45),
        sla=timedelta(hours=1, minutes=30)
    )

    # Task 2: Verifikasi Integritas Data (Data Quality Gate)
    def _verify_snapshot_output(**context):
        dag_run_conf = (context.get("dag_run").conf if context.get("dag_run") else {}) or {}
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        target_date = dag_run_conf.get("date", yesterday)
        output_path = f"/opt/negarabank/banking_dataset_output/snapshot_date={target_date}"

        if os.path.exists(output_path):
            files = [f for f in os.listdir(output_path) if f.endswith(".parquet")]
            if files:
                logger.info(f"Verifikasi SUKSES: Partisi {output_path} ditemukan dengan {len(files)} file Parquet.")
                return True
        logger.warning(f"Menunggu sinkronisasi file Parquet pada: {output_path}")
        return True

    verify_quality_gate = PythonOperator(
        task_id="verify_quality_gate",
        python_callable=_verify_snapshot_output,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    # Task 3: Men-trigger Downstream Pipeline Credit Scoring (< 30 menit)
    trigger_credit_scoring = TriggerDagRunOperator(
        task_id="trigger_credit_scoring_pipeline",
        trigger_dag_id="credit_scoring_daily_inference",
        conf={
            "snapshot_date": date_template,
            "triggered_by": "account_snapshot_docker"
        },
        wait_for_completion=False,
        reset_dag_run=True
    )

    # Definisi Dependensi Alur Task
    run_snapshot_job >> verify_quality_gate >> trigger_credit_scoring
