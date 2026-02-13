from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List, Optional, Any, Dict, ClassVar
import json
import os


class Settings(BaseSettings):
    """Settings for Halatio Tundra conversion service"""

    _secrets_cache: ClassVar[Optional[Dict[str, Any]]] = None
    _secret_payload_cache: ClassVar[Dict[str, str]] = {}

    ENV: str = Field(..., description="Environment: dev, staging, production")

    HOST: str = "0.0.0.0"
    PORT: int = 8080

    MAX_FILE_SIZE_MB: int = 250
    MAX_PROCESSING_TIME_MINUTES: int = 10

    SUPPORTED_FILE_FORMATS: List[str] = ["csv", "tsv", "json", "geojson", "excel", "parquet"]

    LOG_LEVEL: str = "INFO"

    # Google Cloud (Secret Manager for database credentials)
    GCP_PROJECT_ID: str = Field(..., description="Google Cloud Project ID")

    # Optional mounted consolidated secrets file
    SECRETS_JSON_PATH: str = Field(
        "/secrets/credentials.json",
        description="Mounted JSON file path containing runtime secrets",
    )

    # Cloudflare R2 secrets
    R2_SECRET_ID: Optional[str] = Field(
        None,
        description="Secret Manager secret ID containing R2 key material"
    )
    R2_SECRET_VERSION: str = Field("latest", description="Secret version for R2_SECRET_ID")
    R2_ACCESS_KEY_ID_PATH: Optional[str] = Field(
        None,
        description="Mounted file path containing the R2 access key ID"
    )
    R2_SECRET_ACCESS_KEY_PATH: Optional[str] = Field(
        None,
        description="Mounted file path containing the R2 secret access key"
    )
    CLOUDFLARE_ACCOUNT_ID: str = Field(..., description="Cloudflare account ID (32-char hex)")

    # Supabase secrets
    SUPABASE_URL: str = Field(..., description="Supabase project URL")
    SUPABASE_SECRET_ID: Optional[str] = Field(
        None,
        description="Secret Manager secret ID containing Supabase service key"
    )
    SUPABASE_SECRET_VERSION: str = Field("latest", description="Secret version for SUPABASE_SECRET_ID")
    SUPABASE_SECRET_KEY_PATH: Optional[str] = Field(
        None,
        description="Mounted file path containing Supabase service key"
    )

    # DuckDB
    DUCKDB_MEMORY_LIMIT: str = Field("5GB", description="DuckDB memory limit")
    DUCKDB_THREADS: int = Field(2, description="DuckDB thread count")
    DUCKDB_TEMP_DIR: str = Field("/tmp/duckdb_swap", description="DuckDB spill directory")
    DUCKDB_MAX_TEMP_DIR_SIZE: str = Field("2GB", description="Max DuckDB temp directory size")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    def _load_secrets(self) -> Dict[str, Any]:
        if self.__class__._secrets_cache is None:
            secret_path = self.SECRETS_JSON_PATH
            if os.path.exists(secret_path):
                with open(secret_path, "r", encoding="utf-8") as f:
                    self.__class__._secrets_cache = json.load(f)
            else:
                self.__class__._secrets_cache = {}
        return self.__class__._secrets_cache

    def _read_secret_file(self, path: Optional[str]) -> Optional[str]:
        if not path:
            return None
        if not os.path.exists(path):
            return None
        value = open(path, "r", encoding="utf-8").read().strip()
        return value or None

    def _get_secret_payload(self, secret_id: Optional[str], version: str) -> Optional[str]:
        if not secret_id:
            return None
        cache_key = f"{secret_id}:{version}"
        cached = self.__class__._secret_payload_cache.get(cache_key)
        if cached:
            return cached

        from .services.secret_manager import get_secret_manager
        payload = get_secret_manager().get_secret_payload(secret_id=secret_id, version=version)
        self.__class__._secret_payload_cache[cache_key] = payload
        return payload

    @property
    def R2_ACCESS_KEY_ID(self) -> str:
        secrets = self._load_secrets()
        value = secrets.get("r2_access_key_id") or secrets.get("R2_ACCESS_KEY_ID")
        if value:
            return str(value)

        file_value = self._read_secret_file(self.R2_ACCESS_KEY_ID_PATH)
        if file_value:
            return file_value

        payload = self._get_secret_payload(self.R2_SECRET_ID, self.R2_SECRET_VERSION)
        if payload:
            parsed = json.loads(payload)
            value = parsed.get("access_key_id") or parsed.get("R2_ACCESS_KEY_ID")
            if value:
                return str(value)

        return os.getenv("R2_ACCESS_KEY_ID", "")

    @property
    def R2_SECRET_ACCESS_KEY(self) -> str:
        secrets = self._load_secrets()
        value = secrets.get("r2_secret_access_key") or secrets.get("R2_SECRET_ACCESS_KEY")
        if value:
            return str(value)

        file_value = self._read_secret_file(self.R2_SECRET_ACCESS_KEY_PATH)
        if file_value:
            return file_value

        payload = self._get_secret_payload(self.R2_SECRET_ID, self.R2_SECRET_VERSION)
        if payload:
            parsed = json.loads(payload)
            value = parsed.get("secret_access_key") or parsed.get("R2_SECRET_ACCESS_KEY")
            if value:
                return str(value)

        return os.getenv("R2_SECRET_ACCESS_KEY", "")

    @property
    def SUPABASE_SECRET_KEY(self) -> str:
        secrets = self._load_secrets()
        value = secrets.get("supabase_secret_key") or secrets.get("SUPABASE_SECRET_KEY")
        if value:
            return str(value)

        file_value = self._read_secret_file(self.SUPABASE_SECRET_KEY_PATH)
        if file_value:
            return file_value

        payload = self._get_secret_payload(self.SUPABASE_SECRET_ID, self.SUPABASE_SECRET_VERSION)
        if payload:
            return payload.strip()

        return os.getenv("SUPABASE_SECRET_KEY", "")

    @property
    def ALLOWED_ORIGINS(self) -> List[str]:
        base = [
            "https://halatio.com",
            "https://www.halatio.com",
            "https://halatio.vercel.app"
        ]
        if self.ENV != "production":
            base.append("http://localhost:3000")
        return base


settings = Settings()  # type: ignore
