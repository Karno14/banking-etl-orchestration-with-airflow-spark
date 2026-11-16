import os
from pathlib import Path
from typing import Any, Dict

CONFIG_DIR = Path(__file__).resolve().parent / "config"

DEFAULT_LOCAL_CONFIG = {
    "app": {
        "name": "NegaraBank Data Platform",
        "env": "local"
    },
    "spark": {
        "app_name": "negarabank_account_snapshot_etl",
        "master": "local[*]",
        "adaptive_enabled": True,
        "partition_overwrite_mode": "dynamic"
    },
    "paths": {
        "raw_data_dir": "/opt/negarabank/negarabank_dataset",
        "silver_output_dir": "/opt/negarabank/data/silver",
        "logs_dir": "/opt/negarabank/logs"
    },
    "database": {
        "host": "core-db",
        "port": 1521,
        "service": "banking",
        "user": "etl_user",
        "password": "etl_pass123"
    },
    "aws": {
        "region": "ap-southeast-3",
        "secrets_manager": {
            "secret_name": "negarabank/core-db/credentials"
        }
    },
    "data_quality": {
        "strict_mode": True,
        "max_row_count_drop_percentage": 0.05,
        "reconciliation_tolerance": 1.0
    }
}

DEFAULT_PROD_CONFIG = {
    "app": {
        "name": "NegaraBank Data Platform",
        "env": "production"
    },
    "spark": {
        "app_name": "negarabank_account_snapshot_etl",
        "master": "yarn",
        "adaptive_enabled": True,
        "partition_overwrite_mode": "dynamic"
    },
    "paths": {
        "raw_data_dir": "s3://negarabank-bronze/cdc/",
        "silver_output_dir": "s3://negarabank-silver/account_snapshots/",
        "logs_dir": "s3://negarabank-logs/spark/"
    },
    "database": {
        "secrets_manager_key": "negarabank/production/oracle"
    },
    "aws": {
        "region": "ap-southeast-3",
        "secrets_manager": {
            "secret_name": "negarabank/core-db/credentials"
        }
    },
    "data_quality": {
        "strict_mode": True,
        "max_row_count_drop_percentage": 0.01,
        "reconciliation_tolerance": 0.01
    }
}

def load_config(env: str = None) -> Dict[str, Any]:
    if not env:
        env = os.getenv("APP_ENV", "local").lower()

    # Try loading from YAML if PyYAML is installed
    try:
        import yaml
        config_file = CONFIG_DIR / f"{env}.yaml"
        if config_file.exists():
            with open(config_file, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
    except ImportError:
        pass

    # Built-in fallback
    if env == "prod" or env == "production":
        return DEFAULT_PROD_CONFIG
    return DEFAULT_LOCAL_CONFIG

settings = load_config()
