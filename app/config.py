from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List


class Settings(BaseSettings):
    """Settings for Halatio Tundra conversion service"""

    ENV: str = Field(..., description="Environment: dev, staging, production")

    HOST: str = "0.0.0.0"
    PORT: int = 8080

    MAX_FILE_SIZE_MB: int = 250
    MAX_PROCESSING_TIME_MINUTES: int = 10

    SUPPORTED_FILE_FORMATS: List[str] = ["csv", "tsv", "json", "geojson", "excel", "parquet"]

    LOG_LEVEL: str = "INFO"

    # Google Cloud (Secret Manager for database credentials)
    GCP_PROJECT_ID: str = Field(..., description="Google Cloud Project ID")

    # Cloudflare R2
    R2_ACCESS_KEY_ID: str = Field(..., description="R2 access key ID")
    R2_SECRET_ACCESS_KEY: str = Field(..., description="R2 secret access key")
    CLOUDFLARE_ACCOUNT_ID: str = Field(..., description="Cloudflare account ID (32-char hex)")

    # Supabase
    SUPABASE_URL: str = Field(..., description="Supabase project URL")
    SUPABASE_SECRET_KEY: str = Field(..., description="Supabase secret key (sb_secret_...)")

    # DuckDB
    DUCKDB_MEMORY_LIMIT: str = Field("5GB", description="DuckDB memory limit")
    DUCKDB_THREADS: int = Field(2, description="DuckDB thread count")
    DUCKDB_TEMP_DIR: str = Field("/tmp/duckdb_swap", description="DuckDB spill directory")
    DUCKDB_MAX_TEMP_DIR_SIZE: str = Field("2GB", description="Max DuckDB temp directory size")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

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
