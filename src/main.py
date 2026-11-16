import argparse
import sys
import logging
from pathlib import Path

# Ensure project root is in PYTHONPATH
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.settings import load_config
from src.jobs.account_snapshot_job import AccountSnapshotJob, resolve_date_range

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s"
)
logger = logging.getLogger("NegaraBank.Main")

JOBS_REGISTRY = {
    "ACCOUNT_SNAPSHOT": AccountSnapshotJob,
    "DAILY_ACCOUNT_SNAPSHOT": AccountSnapshotJob
}

def main():
    parser = argparse.ArgumentParser(description="NegaraBank Data Platform Job Runner")
    parser.add_argument(
        "--job",
        type=str,
        required=True,
        choices=list(JOBS_REGISTRY.keys()),
        help="Name of the ETL job to execute."
    )
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--date",
        type=str,
        help="Target execution date (YYYY-MM-DD); defaults to yesterday."
    )
    date_group.add_argument(
        "--startdate",
        type=str,
        help="Inclusive start date for a date range (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--enddate",
        type=str,
        help="Inclusive end date for a date range (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--env",
        type=str,
        default="local",
        choices=["local", "dev", "prod"],
        help="Environment to load config for."
    )

    args = parser.parse_args()
    try:
        start_date, end_date = resolve_date_range(
            execution_date=args.date,
            start_date=args.startdate,
            end_date=args.enddate,
        )
    except ValueError as exc:
        parser.error(str(exc))

    logger.info(
        "Starting runner: Job=%s, DateRange=%s..%s, Env=%s",
        args.job,
        start_date,
        end_date,
        args.env,
    )

    config = load_config(args.env)
    job_class = JOBS_REGISTRY[args.job]
    job = None

    try:
        job = job_class(config=config, start_date=start_date, end_date=end_date)
        job.run()
        logger.info(f"Job {args.job} finished successfully.")
    except Exception as e:
        logger.critical(f"Job {args.job} FAILED: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if job is not None and hasattr(job, 'spark') and job.spark:
            job.spark.stop()
            logger.info("Spark session stopped cleanly.")

if __name__ == "__main__":
    main()
