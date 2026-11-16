import os
import json
import logging
from typing import Dict, Any

logger = logging.getLogger("NegaraBank.SecretManager")

class SecretManager:
    """Retrieves secrets from AWS Secrets Manager with local fallback."""
    
    @staticmethod
    def get_credentials(secret_name: str, region_name: str = "ap-southeast-3") -> Dict[str, str]:
        env = os.getenv("APP_ENV", "local").lower()
        if env == "local":
            logger.info("Using local environment credentials.")
            return {
                "username": os.getenv("DB_USER", "etl_user"),
                "password": os.getenv("DB_PASSWORD", "etl_pass123"),
                "host": os.getenv("DB_HOST", "core-db"),
                "port": os.getenv("DB_PORT", "1521"),
                "service": os.getenv("DB_SERVICE", "banking")
            }

        try:
            import boto3
            session = boto3.session.Session()
            client = session.client(service_name="secretsmanager", region_name=region_name)
            response = client.get_secret_value(SecretId=secret_name)
            return json.loads(response["SecretString"])
        except Exception as e:
            logger.error(f"Failed to fetch secrets from AWS Secrets Manager: {e}")
            raise
