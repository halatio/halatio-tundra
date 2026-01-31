"""PostgreSQL connector using Polars + ConnectorX"""

import polars as pl
import os
import time
import logging
from typing import Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential_jitter
from .base import BaseConnector

logger = logging.getLogger(__name__)

class PostgreSQLConnector(BaseConnector):
    """Production-grade PostgreSQL connector"""

    def __init__(self, credentials: Dict[str, Any]):
        super().__init__(credentials)
        self.connection_string = self._build_connection_string()

    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string"""
        return (
            f"postgresql://{self.credentials['username']}:{self.credentials['password']}"
            f"@{self.credentials['host']}:{self.credentials.get('port', 5432)}"
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
        """
        Extract PostgreSQL data to Parquet using ConnectorX

        Args:
            output_path: Path to write parquet file
            query: SQL query to execute (optional)
            table_name: Table name to extract (optional, alternative to query)
            partition_column: Column to partition on for parallel extraction
            partition_num: Number of parallel partitions

        Returns:
            Extraction metadata
        """
        start_time = time.time()

        try:
            # Build query if table_name provided
            if query is None and table_name:
                query = f"SELECT * FROM {table_name}"
            elif query is None:
                raise ValueError("Either query or table_name must be provided")

            logger.info(f"🔄 Extracting PostgreSQL data with query: {query[:100]}...")

            # Extract with ConnectorX (zero-copy, parallel)
            read_params = {
                "query": query,
                "connection_uri": self.connection_string,
                "engine": "connectorx"
            }

            # Add partitioning if specified
            if partition_column:
                read_params["partition_on"] = partition_column
                read_params["partition_num"] = partition_num
                logger.info(f"📊 Using parallel extraction: {partition_num} partitions on {partition_column}")

            df = pl.read_database_uri(**read_params)

            logger.info(f"✅ Extracted {len(df)} rows, {len(df.columns)} columns")

            # Write optimized parquet
            df.write_parquet(
                output_path,
                compression="snappy",
                statistics=True,
                row_group_size=50000,
                use_pyarrow=False  # Use Polars native (faster)
            )

            processing_time = time.time() - start_time
            file_size_mb = os.path.getsize(output_path) / 1024 / 1024

            logger.info(f"📦 Wrote parquet: {file_size_mb:.2f}MB in {processing_time:.2f}s")

            return {
                "rows": len(df),
                "columns": len(df.columns),
                "file_size_mb": round(file_size_mb, 2),
                "processing_time_seconds": round(processing_time, 2),
                "query": query,
                "partitioned": partition_column is not None
            }

        except Exception as e:
            logger.error(f"❌ PostgreSQL extraction failed: {str(e)}")
            raise

    async def test_connection(self) -> Dict[str, Any]:
        """Test PostgreSQL connection"""
        try:
            # Try a simple query
            df = pl.read_database_uri(
                query="SELECT 1 as test",
                connection_uri=self.connection_string,
                engine="connectorx"
            )

            return {
                "success": True,
                "message": "Connection successful",
                "metadata": {
                    "database_type": "PostgreSQL",
                    "rows_returned": len(df)
                }
            }

        except Exception as e:
            return {
                "success": False,
                "error": "connection_failed",
                "message": str(e)
            }
