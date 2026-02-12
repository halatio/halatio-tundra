"""Base DuckDB connector with config-dict connection and R2 support"""

import asyncio
import duckdb
import os
import re
import time
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from tenacity import retry, stop_after_attempt, wait_exponential_jitter
import anyio

logger = logging.getLogger(__name__)

# Timeout for blocking DB extraction operations (matches MAX_PROCESSING_TIME_MINUTES)
_EXTRACTION_TIMEOUT_SECONDS = int(os.getenv("MAX_PROCESSING_TIME_MINUTES", "10")) * 60

# Safe table/schema name pattern (alphanumeric, underscores, dots)
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_\.]+$")


def _make_duckdb_config() -> Dict[str, Any]:
    """
    Build DuckDB connection config dict.

    Passing config at connect() time avoids the Cloud Run cgroup memory bug
    where DuckDB reads host memory instead of container limits when SET is used
    after connection.

    DuckDB 1.4+ best practices: 1-4 GB per thread is optimal; warn if below 1 GB.
    """
    temp_dir = os.getenv("DUCKDB_TEMP_DIR", "/tmp/duckdb_swap")
    os.makedirs(temp_dir, exist_ok=True)
    threads = int(os.getenv("DUCKDB_THREADS", "2"))
    memory_limit_str = os.getenv("DUCKDB_MEMORY_LIMIT", "6GB")

    # Warn when memory per thread falls below 1 GB (DuckDB 1.4+ recommendation)
    try:
        memory_gb = float(memory_limit_str.upper().rstrip("GB").rstrip("MB"))
        if memory_limit_str.upper().endswith("MB"):
            memory_gb /= 1024
        if memory_gb / threads < 1.0:
            logger.warning(
                "DuckDB memory per thread (%.1f GB) is below the recommended 1 GB minimum. "
                "Consider reducing DUCKDB_THREADS or increasing DUCKDB_MEMORY_LIMIT.",
                memory_gb / threads,
            )
    except ValueError:
        pass  # Non-standard memory string; skip ratio check

    config: Dict[str, Any] = {
        "memory_limit": memory_limit_str,
        "temp_directory": temp_dir,
        "threads": threads,
        "max_temp_directory_size": os.getenv("DUCKDB_MAX_TEMP_DIR_SIZE", "1GB"),
        "preserve_insertion_order": False,
        # Write persistent secrets to /tmp so Cloud Run can create the dir
        "home_directory": "/tmp",
    }
    # Use pre-installed extensions if the image has them
    ext_dir = "/opt/duckdb_extensions"
    if os.path.isdir(ext_dir):
        config["extension_directory"] = ext_dir
    return config


class DuckDBBaseConnector(ABC):
    """Base class for all DuckDB-based database connectors."""

    def __init__(self, credentials: Dict[str, Any]) -> None:
        self.credentials = credentials

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _validate_identifier(self, name: str) -> None:
        """Reject table/schema names that could enable SQL injection."""
        if not _SAFE_IDENTIFIER.match(name):
            raise ValueError(
                f"Invalid identifier '{name}'. "
                "Only alphanumeric characters, underscores, and dots are allowed."
            )

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def _attach_database(self, conn: duckdb.DuckDBPyConnection) -> str:
        """
        Attach the external database to a DuckDB connection.

        Returns:
            The alias used in subsequent queries (e.g. 'pg', 'mysql').
        """

    @abstractmethod
    def _get_db_alias(self) -> str:
        """Return the database alias (e.g. 'pg', 'mysql', 'sqlite')."""

    @abstractmethod
    async def test_connection(self) -> Dict[str, Any]:
        """Test the database connection. Returns a result dict."""

    # ------------------------------------------------------------------
    # Core extraction
    # ------------------------------------------------------------------

    def _extract_sync(
        self,
        query: str,
        output_path: str,
        compression: str,
        row_group_size: int,
    ) -> Dict[str, Any]:
        """
        Synchronous DuckDB extraction.  Runs in a thread pool.

        Writes directly to output_path, which may be a local path or an
        R2 URL (r2://bucket/key.parquet) if the persistent R2 secret has
        been created at lifespan startup.
        """
        start_time = time.time()
        conn = duckdb.connect(":memory:", config=_make_duckdb_config())
        try:
            # Use pre-installed extensions; never attempt network installs at runtime
            conn.execute("SET autoinstall_known_extensions = false")
            conn.execute("SET autoload_known_extensions = true")

            self._attach_database(conn)

            # Row / column count before writing
            row_count: int = conn.execute(
                f"SELECT COUNT(*) FROM ({query}) AS _sub"
            ).fetchone()[0]
            col_count: int = len(
                conn.execute(f"SELECT * FROM ({query}) AS _sub LIMIT 0").description
            )

            conn.execute(f"""
                COPY ({query})
                TO '{output_path}'
                (FORMAT parquet, COMPRESSION {compression}, ROW_GROUP_SIZE {row_group_size})
            """)

            processing_time = time.time() - start_time

            file_size_mb = 0.0
            if not output_path.startswith("r2://"):
                file_size_mb = os.path.getsize(output_path) / 1024 / 1024

            return {
                "rows": row_count,
                "columns": col_count,
                "file_size_mb": round(file_size_mb, 2),
                "processing_time_seconds": round(processing_time, 2),
                "query": query,
                "engine": "duckdb",
            }
        finally:
            conn.close()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential_jitter(initial=0.5, max=30),
        reraise=True,
    )
    async def extract_to_parquet(
        self,
        output_path: str,
        query: Optional[str] = None,
        table_name: Optional[str] = None,
        compression: str = "zstd",
        row_group_size: int = 122880,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Extract data to Parquet using DuckDB.

        Args:
            output_path: Local path or R2 URL (r2://bucket/key.parquet)
            query: SQL query to execute (takes priority over table_name)
            table_name: Table name to fully extract (alternative to query)
            compression: Parquet compression algorithm (zstd, snappy, none)
            row_group_size: Parquet row group size

        Returns:
            Extraction metadata dict
        """
        if query is None and table_name:
            self._validate_identifier(table_name)
            query = f"SELECT * FROM {self._get_db_alias()}.{table_name}"
        elif query is None:
            raise ValueError("Either query or table_name must be provided")

        logger.info(f"Extracting with DuckDB: {query[:120]}…")

        try:
            async with asyncio.timeout(_EXTRACTION_TIMEOUT_SECONDS):
                result = await anyio.to_thread.run_sync(
                    self._extract_sync,
                    query,
                    output_path,
                    compression,
                    row_group_size,
                    cancellable=True,
                )
        except asyncio.TimeoutError:
            logger.error(
                "Extraction timed out after %d seconds: %s",
                _EXTRACTION_TIMEOUT_SECONDS,
                query[:120],
            )
            raise
        except asyncio.CancelledError:
            logger.warning("Extraction cancelled: %s", query[:120])
            raise

        logger.info(
            f"Extracted {result['rows']} rows, {result['columns']} cols "
            f"in {result['processing_time_seconds']:.2f}s"
        )
        return result
