"""Google Secret Manager integration for credential storage"""

from google.cloud import secretmanager
import json
import logging
from typing import Dict, Any, Optional
from threading import Lock
import time
from urllib.parse import quote

logger = logging.getLogger(__name__)


def _encode_userinfo_component(value: Any) -> str:
    """Percent-encode username/password values used in URI userinfo."""
    return quote(str(value), safe="")


class SecretManagerService:
    """Manages credentials in Google Secret Manager with caching"""

    def __init__(self, project_id: str, default_ttl: int = 3600):
        self.client = secretmanager.SecretManagerServiceClient()
        self.project_id = project_id
        self.default_ttl = default_ttl
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()

    def store_credentials(
        self,
        secret_id: str,
        credentials: Dict[str, Any],
        labels: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Store credentials in Secret Manager

        Args:
            secret_id: Unique identifier for the secret
            credentials: Credential data to store
            labels: Optional labels for the secret

        Returns:
            The secret ID
        """
        parent = f"projects/{self.project_id}"

        try:
            # Try to get existing secret
            secret_name = f"{parent}/secrets/{secret_id}"
            try:
                self.client.get_secret(request={"name": secret_name})
                logger.info("Secret already exists, adding new version")
            except Exception:
                # Create new secret
                secret = self.client.create_secret(
                    request={
                        "parent": parent,
                        "secret_id": secret_id,
                        "secret": {
                            "replication": {"automatic": {}},
                            "labels": labels or {}
                        }
                    }
                )
                logger.info("Created new secret")

            # Add secret version
            payload = json.dumps(credentials).encode("UTF-8")
            self.client.add_secret_version(
                request={
                    "parent": secret_name,
                    "payload": {"data": payload}
                }
            )

            logger.info("✅ Stored credentials in Secret Manager")
            return secret_id

        except Exception as e:
            logger.error(f"❌ Failed to store credentials: {str(e)}")
            raise

    def get_credentials(self, secret_id: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Retrieve credentials from Secret Manager with caching

        Args:
            secret_id: Secret identifier
            use_cache: Whether to use cache (default: True)

        Returns:
            Credential dictionary
        """
        payload = self.get_secret_payload(secret_id=secret_id, version="latest", use_cache=use_cache)
        try:
            credentials = json.loads(payload)
            return credentials
        except Exception as e:
            logger.error(f"❌ Failed to retrieve credentials: {str(e)}")
            raise

    def get_secret_payload(
        self,
        secret_id: str,
        version: str = "latest",
        use_cache: bool = True,
    ) -> str:
        """Retrieve a generic secret payload from Secret Manager with caching."""
        cache_key = f"{secret_id}:{version}"

        if use_cache:
            with self._lock:
                cached = self._cache.get(cache_key)
                if cached and (time.time() - cached["fetched_at"]) < self.default_ttl:
                    logger.debug("📦 Retrieved secret payload from cache")
                    return str(cached["value"])

        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version}"
            response = self.client.access_secret_version(request={"name": name})
            payload = response.payload.data.decode("UTF-8")

            with self._lock:
                self._cache[cache_key] = {
                    "value": payload,
                    "fetched_at": time.time()
                }

            logger.info("🔐 Retrieved secret payload from Secret Manager")
            return payload
        except Exception as e:
            logger.error(f"❌ Failed to retrieve secret payload: {str(e)}")
            raise

    def delete_credentials(self, secret_id: str) -> None:
        """Delete credentials from Secret Manager"""
        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}"
            self.client.delete_secret(request={"name": name})

            # Remove from cache
            with self._lock:
                self._cache.pop(secret_id, None)

            logger.info("🗑️ Deleted credentials")

        except Exception as e:
            logger.error(f"❌ Failed to delete credentials: {str(e)}")
            raise

    def build_connection_string(
        self,
        db_type: str,
        credentials: Dict[str, Any]
    ) -> str:
        """
        Build database connection string from credentials

        Args:
            db_type: Database type (postgresql, mysql, etc.)
            credentials: Credential dictionary

        Returns:
            Connection string
        """
        if db_type == "postgresql":
            return (
                f"postgresql://{_encode_userinfo_component(credentials['username'])}:{_encode_userinfo_component(credentials['password'])}"
                f"@{credentials['host']}:{credentials.get('port', 5432)}"
                f"/{credentials['database']}"
            )
        elif db_type == "mysql":
            return (
                f"mysql://{_encode_userinfo_component(credentials['username'])}:{_encode_userinfo_component(credentials['password'])}"
                f"@{credentials['host']}:{credentials.get('port', 3306)}"
                f"/{credentials['database']}"
            )
        elif db_type == "bigquery":
            # BigQuery uses project_id and dataset
            return f"bigquery://{credentials['project_id']}"
        elif db_type == "snowflake":
            return (
                f"snowflake://{_encode_userinfo_component(credentials['username'])}:{_encode_userinfo_component(credentials['password'])}"
                f"@{credentials['account']}/{credentials['database']}"
                f"?warehouse={credentials.get('warehouse', '')}"
            )
        else:
            raise ValueError(f"Unsupported database type: {db_type}")


# Global instance
_secret_manager: Optional[SecretManagerService] = None

def get_secret_manager() -> SecretManagerService:
    """Get or create global SecretManagerService instance"""
    global _secret_manager
    if _secret_manager is None:
        from ..config import settings
        _secret_manager = SecretManagerService(project_id=settings.GCP_PROJECT_ID)
    return _secret_manager
