"""MySQL connector using Polars + ConnectorX"""

import polars as pl
import os
import time
import logging
from typing import Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential_jitter
from .base import BaseConnector

logger = logging.getLogger(__name__)

class MySQLConnector(BaseConnector):
    """Production-grade MySQL connector"""

    def __init__(self, credentials: Dict[str, Any]):
        super().__init__(credentials)
        self.connection_string = self._build_connection_string()

    def _build_connection_string(self) -> str:
        """Build MySQL connection string"""
        return (
            f"mysql://{self.credentials['username']}:{self.credentials['password']}"
            f"@{self.credentials['host']}:{self.credentials.get('port', 3306)}"
            f"/{self.credentials['database']}"
        )

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=0.5, max=30),
        reraise=True
    )
    async def extract_to_parquet(
        self,
        output_path: str,
        query: Optional[str] = None,
        table_name: Optional[str] = None,
        partition_column: Optional[str] = None,
        partition_num: int = 4,
        **kwargs
    ) -> Dict[str, Any]:
        """Extract MySQL data to Parquet using ConnectorX"""
        start_time = time.time()

        try:
            # Build query if table_name provided
            if query is None and table_name:
                query = f"SELECT * FROM {table_name}"
            elif query is None:
                raise ValueError("Either query or table_name must be provided")

            logger.info(f"🔄 Extracting MySQL data with query: {query[:100]}...")

            # Extract with ConnectorX
            read_params = {
                "query": query,
                "connection_uri": self.connection_string,
                "engine": "connectorx"
            }

            if partition_column:
                read_params["partition_on"] = partition_column
                read_params["partition_num"] = partition_num

            df = pl.read_database_uri(**read_params)

            logger.info(f"✅ Extracted {len(df)} rows, {len(df.columns)} columns")

            # Write parquet
            df.write_parquet(
                output_path,
                compression="snappy",
                statistics=True,
                row_group_size=50000,
                use_pyarrow=False
            )

            processing_time = time.time() - start_time
            file_size_mb = os.path.getsize(output_path) / 1024 / 1024

            return {
                "rows": len(df),
                "columns": len(df.columns),
                "file_size_mb": round(file_size_mb, 2),
                "processing_time_seconds": round(processing_time, 2),
                "query": query
            }

        except Exception as e:
            logger.error(f"❌ MySQL extraction failed: {str(e)}")
            raise

    async def test_connection(self) -> Dict[str, Any]:
        """Test MySQL connection"""
        try:
            df = pl.read_database_uri(
                query="SELECT 1 as test",
                connection_uri=self.connection_string,
                engine="connectorx"
            )

            return {
                "success": True,
                "message": "Connection successful",
                "metadata": {
                    "database_type": "MySQL",
                    "rows_returned": len(df)
                }
            }

        except Exception as e:
            return {
                "success": False,
                "error": "connection_failed",
                "message": str(e)
            }
