"""PostgreSQL connector using ADBC (Arrow Database Connectivity) driver.

ADBC v1.10.0 (January 2026) provides zero-copy Arrow transfer from PostgreSQL,
giving better memory efficiency than the DuckDB postgres scanner for large
extractions.  DuckDB is still used for Parquet/R2 writing so that direct
R2 writes via the persistent httpfs secret are preserved.

Falls back to the DuckDB postgres scanner if the adbc_driver_postgresql
package is not installed.
"""

import asyncio
import duckdb
import logging
import os
import time
from typing import Any, Dict, Optional
from urllib.parse import quote

import anyio
from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from .duckdb_base import DuckDBBaseConnector, _make_duckdb_config, _EXTRACTION_TIMEOUT_SECONDS
from ..resilience import external_dependency_breaker, should_trip_breaker

logger = logging.getLogger(__name__)


class PostgresADBCConnector(DuckDBBaseConnector):
    """
    PostgreSQL connector via ADBC (adbc_driver_postgresql >= 1.1.0).

    Extraction path:
      1. ADBC connects to PostgreSQL and fetches the result as an Arrow table
         (uses PostgreSQL COPY protocol internally — fast for large result sets).
      2. DuckDB registers the in-memory Arrow table and writes it to Parquet,
         including direct-to-R2 writes via the httpfs persistent secret.

    This hybrid approach combines ADBC's efficient columnar reads with DuckDB's
    R2 write support.
    """

    def __init__(self, credentials: Dict[str, Any]) -> None:
        super().__init__(credentials)
        encoded_username = quote(credentials["username"], safe="")
        encoded_password = quote(credentials["password"], safe="")
        self._uri = (
            f"postgresql://{encoded_username}:{encoded_password}"
            f"@{credentials['host']}:{credentials.get('port', 5432)}"
            f"/{credentials['database']}"
        )

    # ------------------------------------------------------------------
    # DuckDBBaseConnector abstract interface
    # ------------------------------------------------------------------

    def _get_db_alias(self) -> str:
        return "pg"

    def _attach_database(self, conn: duckdb.DuckDBPyConnection) -> str:
        # Not used — ADBC handles the PostgreSQL connection directly.
        raise NotImplementedError("PostgresADBCConnector uses ADBC, not DuckDB ATTACH")

    # ------------------------------------------------------------------
    # ADBC-based synchronous extraction (runs in thread pool)
    # ------------------------------------------------------------------

    def _extract_sync(
        self,
        query: str,
        output_path: str,
        compression: str,
        row_group_size: int,
    ) -> Dict[str, Any]:
        """
        Fetch query results via ADBC, then write Parquet via DuckDB.

        The ADBC driver uses PostgreSQL's COPY protocol internally, making it
        significantly faster than row-by-row fetching for large result sets.
        DuckDB receives the Arrow table and writes to output_path, which may be
        a local path or an R2 URL (r2://...) if the persistent secret exists.
        """
        import adbc_driver_postgresql.dbapi  # deferred — optional dependency

        start_time = time.time()

        # Step 1: fetch via ADBC
        with adbc_driver_postgresql.dbapi.connect(self._uri) as adbc_conn:
            with adbc_conn.cursor() as cur:
                cur.execute(query)
                arrow_table = cur.fetch_arrow_table()

        row_count: int = arrow_table.num_rows
        col_count: int = arrow_table.num_columns

        if row_count == 0:
            logger.warning("ADBC query returned 0 rows: %s", query[:120])

        # Step 2: write Parquet via DuckDB (preserves R2 write support)
        conn = duckdb.connect(":memory:", config=_make_duckdb_config())
        try:
            conn.execute("SET autoinstall_known_extensions = false")
            conn.execute("SET autoload_known_extensions = true")

            # Register the Arrow table so DuckDB can scan it without copying
            conn.register("_adbc_result", arrow_table)

            conn.execute(f"""
                COPY (SELECT * FROM _adbc_result)
                TO '{output_path}'
                (FORMAT parquet, COMPRESSION {compression}, ROW_GROUP_SIZE {row_group_size})
            """)
        finally:
            conn.close()

        processing_time = time.time() - start_time

        file_size_mb = 0.0
        if not output_path.startswith("r2://"):
            try:
                file_size_mb = os.path.getsize(output_path) / 1024 / 1024
            except OSError:
                pass

        return {
            "rows": row_count,
            "columns": col_count,
            "file_size_mb": round(file_size_mb, 2),
            "processing_time_seconds": round(processing_time, 2),
            "query": query,
            "engine": "adbc-postgresql",
        }

    # ------------------------------------------------------------------
    # extract_to_parquet — override to add timeout + cancellable
    # ------------------------------------------------------------------

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
        if query is None and table_name:
            self._validate_identifier(table_name)
            query = f"SELECT * FROM {table_name}"
        elif query is None:
            raise ValueError("Either query or table_name must be provided")

        logger.info("Extracting via ADBC: %s…", query[:120])

        try:
            async with asyncio.timeout(_EXTRACTION_TIMEOUT_SECONDS):
                result = await external_dependency_breaker.call_async(
                    lambda: anyio.to_thread.run_sync(
                        self._extract_sync,
                        query,
                        output_path,
                        compression,
                        row_group_size,
                        cancellable=True,
                    ),
                    should_trip=should_trip_breaker,
                )
        except asyncio.TimeoutError:
            logger.error(
                "ADBC extraction timed out after %d seconds: %s",
                _EXTRACTION_TIMEOUT_SECONDS,
                query[:120],
            )
            raise
        except asyncio.CancelledError:
            logger.warning("ADBC extraction cancelled: %s", query[:120])
            raise

        logger.info(
            "ADBC extracted %d rows, %d cols in %.2fs (engine: %s)",
            result["rows"],
            result["columns"],
            result["processing_time_seconds"],
            result["engine"],
        )
        return result

    # ------------------------------------------------------------------
    # test_connection
    # ------------------------------------------------------------------

    async def test_connection(self) -> Dict[str, Any]:
        try:
            def _test() -> int:
                import adbc_driver_postgresql.dbapi  # deferred — optional dependency

                with adbc_driver_postgresql.dbapi.connect(self._uri) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT COUNT(*) FROM information_schema.tables "
                            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
                        )
                        tbl = cur.fetch_arrow_table()
                        return tbl.column(0)[0].as_py()

            table_count = await anyio.to_thread.run_sync(_test, cancellable=True)
            return {
                "success": True,
                "message": "Connection successful",
                "metadata": {
                    "database_type": "PostgreSQL",
                    "engine": "adbc-postgresql",
                    "table_count": table_count,
                },
            }
        except Exception as e:
            logger.error("PostgreSQL ADBC connection test failed: %s", e)
            return {"success": False, "error": "connection_failed", "message": str(e)}
