"""Base DuckDB connector with memory management and R2 support"""

import duckdb
import os
import time
import logging
import re
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from tenacity import retry, stop_after_attempt, wait_exponential_jitter
import anyio

logger = logging.getLogger(__name__)

# Safe table/schema name pattern
SAFE_SQL_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_\.]+$")


class DuckDBBaseConnector(ABC):
    """Base class for DuckDB-based connectors with memory management"""

    def __init__(self, credentials: Dict[str, Any]):
        self.credentials = credentials
        self.memory_limit = os.getenv("DUCKDB_MEMORY_LIMIT", "6GB")
        self.temp_dir = os.getenv("DUCKDB_TEMP_DIR", "/tmp/duckdb")
        self.threads = int(os.getenv("DUCKDB_THREADS", "2"))

    def _validate_table_name(self, table_name: str) -> None:
        """Validate table name to prevent SQL injection"""
        if not SAFE_SQL_IDENTIFIER.match(table_name):
            raise ValueError(
                f"Invalid table name: '{table_name}'. "
                "Table names must contain only alphanumeric characters, underscores, and dots."
            )

    def _configure_duckdb(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Configure DuckDB with memory limits and performance settings"""
        # Ensure temp directory exists
        os.makedirs(self.temp_dir, exist_ok=True)

        # Memory and performance settings
        conn.execute(f"SET memory_limit = '{self.memory_limit}'")
        conn.execute(f"SET temp_directory = '{self.temp_dir}'")
        conn.execute(f"SET threads = {self.threads}")
        conn.execute("SET preserve_insertion_order = false")

        logger.info(f"DuckDB configured: memory={self.memory_limit}, temp_dir={self.temp_dir}, threads={self.threads}")

    def _setup_r2_secret(
        self,
        conn: duckdb.DuckDBPyConnection,
        r2_config: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Setup R2 secret for direct write capability

        Args:
            conn: DuckDB connection
            r2_config: Dictionary with keys: key_id, secret, account_id
        """
        if not r2_config:
            return

        conn.execute(f"""
            CREATE SECRET r2_secret (
                TYPE r2,
                KEY_ID '{r2_config['key_id']}',
                SECRET '{r2_config['secret']}',
                ACCOUNT_ID '{r2_config['account_id']}'
            )
        """)
        logger.info("R2 secret configured for direct write")

    @abstractmethod
    def _attach_database(self, conn: duckdb.DuckDBPyConnection) -> str:
        """
        Attach external database to DuckDB

        Returns:
            Database alias to use in queries (e.g., 'pg', 'mysql', 'mssql')
        """
        pass

    def _extract_sync(
        self,
        query: str,
        output_path: str,
        compression: str,
        row_group_size: int,
        r2_config: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Synchronous extraction logic using DuckDB

        Args:
            query: SQL query to execute
            output_path: Local path or R2 URL (r2://bucket/file.parquet)
            compression: Compression algorithm (snappy, zstd, none)
            row_group_size: Parquet row group size
            r2_config: Optional R2 configuration for direct write
        """
        start_time = time.time()

        # Create DuckDB connection
        conn = duckdb.connect()

        try:
            # Configure DuckDB
            self._configure_duckdb(conn)

            # Setup R2 if writing directly to R2
            if output_path.startswith('r2://'):
                self._setup_r2_secret(conn, r2_config)

            # Attach external database
            db_alias = self._attach_database(conn)

            # Get row count for metadata
            count_query = f"SELECT COUNT(*) as count FROM ({query}) AS subquery"
            row_count = conn.execute(count_query).fetchone()[0]

            # Get column count
            sample_query = f"SELECT * FROM ({query}) AS subquery LIMIT 1"
            sample_result = conn.execute(sample_query)
            column_count = len(sample_result.description)

            # Execute COPY TO parquet
            copy_query = f"""
                COPY ({query})
                TO '{output_path}'
                (FORMAT parquet, COMPRESSION {compression}, ROW_GROUP_SIZE {row_group_size})
            """
            conn.execute(copy_query)

            processing_time = time.time() - start_time

            # Get file size (only if local file)
            file_size_mb = 0
            if not output_path.startswith('r2://'):
                file_size_mb = os.path.getsize(output_path) / 1024 / 1024

            return {
                "rows": row_count,
                "columns": column_count,
                "file_size_mb": round(file_size_mb, 2),
                "processing_time_seconds": round(processing_time, 2),
                "query": query,
                "output_path": output_path,
                "engine": "duckdb"
            }

        finally:
            conn.close()

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
        compression: str = "zstd",
        row_group_size: int = 100000,
        r2_config: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Extract data to Parquet using DuckDB

        Args:
            output_path: Local path or R2 URL (r2://bucket/file.parquet)
            query: SQL query to execute (optional)
            table_name: Table name to extract (optional, alternative to query)
            compression: Compression algorithm (snappy, zstd, none)
            row_group_size: Parquet row group size (default 100000)
            r2_config: Optional R2 config dict with keys: key_id, secret, account_id
            **kwargs: Additional connector-specific parameters

        Returns:
            Extraction metadata
        """
        try:
            # Build query if table_name provided
            if query is None and table_name:
                self._validate_table_name(table_name)
                db_alias = self._get_db_alias()
                query = f"SELECT * FROM {db_alias}.{table_name}"
            elif query is None:
                raise ValueError("Either query or table_name must be provided")

            logger.info(f"Extracting data with DuckDB: {query[:100]}...")

            # Run CPU-bound operation in thread pool
            result = await anyio.to_thread.run_sync(
                self._extract_sync,
                query,
                output_path,
                compression,
                row_group_size,
                r2_config
            )

            logger.info(
                f"Extracted {result['rows']} rows, {result['columns']} columns "
                f"in {result['processing_time_seconds']:.2f}s"
            )

            return result

        except ValueError as e:
            logger.error(f"Validation error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"DuckDB extraction failed: {str(e)}")
            raise

    @abstractmethod
    def _get_db_alias(self) -> str:
        """Get the database alias used in queries (e.g., 'pg', 'mysql')"""
        pass

    @abstractmethod
    async def test_connection(self) -> Dict[str, Any]:
        """Test database connection"""
        pass
