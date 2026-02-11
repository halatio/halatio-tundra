from pydantic_settings import BaseSettings
from pydantic import Field, model_validator
from typing import List, Optional


class Settings(BaseSettings):
    """Settings for Halatio Tundra conversion service"""

    ENV: str = Field(..., description="Environment: dev, staging, production")

    HOST: str = "0.0.0.0"
    PORT: int = 8080

    MAX_FILE_SIZE_MB: int = 500
    MAX_PROCESSING_TIME_MINUTES: int = 10

    SUPPORTED_FILE_FORMATS: List[str] = ["csv", "tsv", "json", "geojson", "excel", "parquet"]

    LOG_LEVEL: str = "INFO"

    # Google Cloud (Secret Manager for database credentials)
    GCP_PROJECT_ID: str = Field(..., description="Google Cloud Project ID")

    # Cloudflare R2
    R2_ACCESS_KEY_ID: str = Field(..., description="R2 access key ID")
    R2_SECRET_ACCESS_KEY: str = Field(..., description="R2 secret access key")
    CLOUDFLARE_ACCOUNT_ID: str = Field(..., description="Cloudflare account ID (32-char hex)")
    R2_BUCKET_PREFIX: str = Field("halatio-org", description="Prefix for per-org R2 buckets")

    # Supabase
    SUPABASE_URL: str = Field(..., description="Supabase project URL")
    SUPABASE_SECRET_KEY: Optional[str] = Field(
        default=None,
        description="Supabase secret key (sb_secret_..., preferred)",
    )
    SUPABASE_SERVICE_ROLE_KEY: Optional[str] = Field(
        default=None,
        description="Legacy Supabase service-role key (fallback if secret key is unavailable)",
    )

    # DuckDB
    DUCKDB_MEMORY_LIMIT: str = Field("6GB", description="DuckDB memory limit")
    DUCKDB_THREADS: int = Field(2, description="DuckDB thread count")
    DUCKDB_TEMP_DIR: str = Field("/tmp/duckdb_swap", description="DuckDB spill directory")
    DUCKDB_MAX_TEMP_DIR_SIZE: str = Field("1GB", description="Max DuckDB temp directory size")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @model_validator(mode="after")
    def _validate_supabase_admin_key(self):
        if not self.SUPABASE_SECRET_KEY and not self.SUPABASE_SERVICE_ROLE_KEY:
            raise ValueError(
                "Either SUPABASE_SECRET_KEY (preferred) or SUPABASE_SERVICE_ROLE_KEY must be set"
            )
        return self

    @property
    def SUPABASE_ADMIN_KEY(self) -> str:
        return self.SUPABASE_SECRET_KEY or self.SUPABASE_SERVICE_ROLE_KEY or ""

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
