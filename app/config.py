from pydantic_settings import BaseSettings
from pydantic import Field, HttpUrl
from typing import List

class Settings(BaseSettings):
    """Settings for Halatio Tundra conversion service"""

    # Runtime environment: 'dev', 'staging', or 'production'
    ENV: str = Field(..., description="Environment: dev, staging, production")

    # Server configuration
    HOST: str = "0.0.0.0"
    PORT: int = 8080

    # Processing limits
    MAX_FILE_SIZE_MB: int = 500
    MAX_API_RESPONSE_MB: int = 100
    MAX_SQL_RESPONSE_MB: int = 100
    MAX_PROCESSING_TIME_MINUTES: int = 10

    # Performance
    MAX_MEMORY_USAGE_GB: int = 2
    MAX_CONCURRENT_CONVERSIONS: int = 5

    # File formats
    SUPPORTED_FILE_FORMATS: List[str] = ["csv", "tsv", "json", "geojson", "excel", "parquet"]

    # Parquet
    PARQUET_COMPRESSION: str = "snappy"
    PARQUET_ROW_GROUP_SIZE: int = 50000

    # SQL limits
    DEFAULT_SQL_LIMIT: int = 100_000
    MAX_SQL_LIMIT: int = 1_000_000

    # Logging
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def ALLOWED_ORIGINS(self) -> List[str]:
        """Dynamically determine CORS origins based on environment"""
        base = [
            "https://halatio.com",
            "https://www.halatio.com",
            "https://halatio.vercel.app"
        ]
        if self.ENV != "production":
            base.append("http://localhost:3000")
        return base
    
settings = Settings() # type: ignore
