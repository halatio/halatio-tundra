"""SQLite connector using Polars + ConnectorX"""

import polars as pl
import os
import time
import logging
import re
from typing import Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential_jitter
from .base import BaseConnector
import anyio

logger = logging.getLogger(__name__)

# Safe table/schema name pattern: alphanumeric, underscores, dots for schema.table
SAFE_SQL_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_\.]+$")

class SQLiteConnector(BaseConnector):
    """Production-grade SQLite connector"""

    def __init__(self, credentials: Dict[str, Any]):
        super().__init__(credentials)
        self.connection_string = self._build_connection_string()

    def _build_connection_string(self) -> str:
        """
        Build SQLite connection string

        SQLite uses file paths instead of host/port/username/password.
        Accepts either:
        - file_path field directly
        - database field as file path (for backwards compatibility)
        """
        file_path = self.credentials.get('file_path') or self.credentials.get('database')

        if not file_path:
            raise ValueError("SQLite requires either 'file_path' or 'database' field with the path to the SQLite file")

        # SQLite connection strings use three slashes for absolute paths
        # sqlite:///absolute/path/to/file.db
        if not file_path.startswith('/'):
            # Relative path - add current directory
            file_path = os.path.abspath(file_path)

        return f"sqlite:///{file_path}"

    def _validate_table_name(self, table_name: str) -> None:
        """Validate table name to prevent SQL injection"""
        if not SAFE_SQL_IDENTIFIER.match(table_name):
            raise ValueError(
                f"Invalid table name: '{table_name}'. "
                "Table names must contain only alphanumeric characters, underscores, and dots."
            )

    def _extract_sync(
        self,
        query: str,
        output_path: str,
        partition_column: Optional[str],
        partition_num: int,
        compression: str
    ) -> Dict[str, Any]:
        """Synchronous extraction logic to run in thread pool"""
        start_time = time.time()

        # Extract with ConnectorX
        # Note: SQLite doesn't support partitioning in ConnectorX
        read_params = {
            "query": query,
            "uri": self.connection_string,
            "engine": "connectorx"
        }

        # SQLite doesn't support partition_on in ConnectorX
        if partition_column:
            logger.warning("SQLite does not support parallel partitioning - ignoring partition settings")

        df = pl.read_database_uri(**read_params)

        # Write parquet
        df.write_parquet(
            output_path,
            compression=compression,
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
            "query": query,
            "partitioned": False  # SQLite doesn't support partitioning
        }

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
        compression: str = "snappy",
        **kwargs
    ) -> Dict[str, Any]:
        """
        Extract SQLite data to Parquet using ConnectorX

        Args:
            output_path: Path to write parquet file
            query: SQL query to execute (optional)
            table_name: Table name to extract (optional, alternative to query)
            partition_column: Column to partition on (not supported for SQLite)
            partition_num: Number of parallel partitions (not supported for SQLite)
            compression: Compression algorithm (snappy, zstd, none)

        Returns:
            Extraction metadata
        """
        try:
            # Build query if table_name provided
            if query is None and table_name:
                self._validate_table_name(table_name)
                query = f"SELECT * FROM {table_name}"
            elif query is None:
                raise ValueError("Either query or table_name must be provided")

            logger.info(f"Extracting SQLite data with query: {query[:100]}...")

            # Run CPU-bound operation in thread pool to avoid blocking event loop
            result = await anyio.to_thread.run_sync(
                self._extract_sync,
                query,
                output_path,
                partition_column,
                partition_num,
                compression
            )

            logger.info(f"Extracted {result['rows']} rows, {result['columns']} columns in {result['processing_time_seconds']:.2f}s")

            return result

        except ValueError as e:
            logger.error(f"Validation error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"SQLite extraction failed: {str(e)}")
            raise

    async def test_connection(self) -> Dict[str, Any]:
        """Test SQLite connection"""
        try:
            # Run connection test in thread pool
            def _test_sync():
                df = pl.read_database_uri(
                    query="SELECT 1 as test",
                    uri=self.connection_string,
                    engine="connectorx"
                )
                return len(df)

            rows = await anyio.to_thread.run_sync(_test_sync)

            return {
                "success": True,
                "message": "Connection successful",
                "metadata": {
                    "database_type": "SQLite",
                    "rows_returned": rows
                }
            }

        except Exception as e:
            return {
                "success": False,
                "error": "connection_failed",
                "message": str(e)
            }
