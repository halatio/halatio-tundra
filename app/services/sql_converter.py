import duckdb
import pyarrow as pa
import httpx
import json
import time
import logging
import tempfile
import os
from typing import Dict, Any, Optional
from ..models.conversionRequest import ConversionMetadata

logger = logging.getLogger(__name__)


class SqlConverter:
    """SQL API proxy converter - executes remote SQL and converts results to Parquet"""

    MAX_RESPONSE_SIZE = 100 * 1024 * 1024  # 100MB
    TIMEOUT_SECONDS = 600
    DEFAULT_QUERY_LIMIT = 100000

    @staticmethod
    async def convert(
        endpoint: str,
        database: str,
        query: str,
        output_url: str,
        credentials_id: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute SQL query via remote endpoint and convert results to parquet"""

        start_time = time.time()
        query_execution_time_ms = None
        options = options or {}

        try:
            logger.info(f"Starting SQL conversion: {database}")

            max_rows = options.get("max_rows", SqlConverter.DEFAULT_QUERY_LIMIT)
            query_timeout = options.get("query_timeout_seconds", SqlConverter.TIMEOUT_SECONDS)

            safe_query = SqlConverter._add_safety_limit(query, max_rows)

            query_start_time = time.time()
            sql_results = await SqlConverter._execute_sql_query(
                endpoint, database, safe_query, credentials_id, options, query_timeout
            )
            query_execution_time_ms = (time.time() - query_start_time) * 1000

            # Convert results to parquet via DuckDB
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = tmp.name

            try:
                row_count, col_count, schema = SqlConverter._write_parquet(sql_results, tmp_path)

                with open(tmp_path, "rb") as f:
                    parquet_data = f.read()
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

            await SqlConverter._upload_parquet(output_url, parquet_data)

            processing_time = time.time() - start_time
            file_size_mb = len(parquet_data) / 1024 / 1024

            metadata = ConversionMetadata(
                rows=row_count,
                columns=col_count,
                column_schema=schema,
                file_size_mb=round(file_size_mb, 2),
                processing_time_seconds=round(processing_time, 2),
                source_type="sql",
                query_execution_time_ms=round(query_execution_time_ms, 2) if query_execution_time_ms else None,
                connection_info={
                    "database_type": options.get("database_type", "Unknown"),
                    "rows_fetched": row_count,
                    "engine": "duckdb"
                }
            )

            logger.info(f"SQL conversion complete: {row_count} rows, {file_size_mb:.2f}MB")
            return {"success": True, "metadata": metadata.dict()}

        except Exception as e:
            logger.error(f"SQL conversion failed: {str(e)}")
            return {"success": False, "error": str(e)}

    @staticmethod
    def _write_parquet(data: list, output_path: str):
        """Write SQL results to parquet using DuckDB"""

        if not data:
            # Write empty parquet
            table = pa.table({"_empty": pa.array([], type=pa.null())})
        else:
            table = pa.Table.from_pylist(data)

        conn = duckdb.connect()
        try:
            memory_limit = os.getenv("DUCKDB_MEMORY_LIMIT", "6GB")
            conn.execute(f"SET memory_limit = '{memory_limit}'")

            conn.register("sql_results", table)
            conn.execute(f"""
                COPY sql_results
                TO '{output_path}'
                (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
            """)

            row_count = conn.execute("SELECT COUNT(*) FROM sql_results").fetchone()[0]
            desc = conn.execute("DESCRIBE sql_results").fetchall()
            col_count = len(desc)

            schema = {
                "fields": [
                    {"name": row[0], "type": row[1]}
                    for row in desc
                    if not row[0].startswith("_")
                ],
                "format": "parquet",
                "encoding": "utf-8",
                "source": "sql"
            }

            return row_count, col_count, schema
        finally:
            conn.close()

    @staticmethod
    async def _execute_sql_query(
        endpoint: str,
        database: str,
        query: str,
        credentials_id: Optional[str],
        options: Dict[str, Any],
        timeout: int
    ) -> list:
        """Execute SQL query via API endpoint"""

        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        if credentials_id:
            auth_headers = await SqlConverter._get_auth_headers(credentials_id)
            headers.update(auth_headers)

        request_body = {"query": query, "database": database}
        if options.get("port"):
            request_body["port"] = options["port"]
        if options.get("ssl_mode"):
            request_body["ssl_mode"] = options["ssl_mode"]

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(endpoint, headers=headers, json=request_body)
            response.raise_for_status()

            content_length = response.headers.get("content-length")
            if content_length and int(content_length) > SqlConverter.MAX_RESPONSE_SIZE:
                raise ValueError(
                    f"SQL response too large: {int(content_length)/1024/1024:.1f}MB exceeds 100MB limit"
                )

            data = response.json()

            data_size = len(json.dumps(data).encode("utf-8"))
            if data_size > SqlConverter.MAX_RESPONSE_SIZE:
                raise ValueError(
                    f"SQL response too large: {data_size/1024/1024:.1f}MB exceeds 100MB limit"
                )

        rows = SqlConverter._extract_rows_from_response(data)
        logger.info(f"SQL query returned {len(rows)} rows")
        return rows

    @staticmethod
    async def _get_auth_headers(credentials_id: str) -> Dict[str, str]:
        logger.warning(f"SQL credential retrieval not implemented for: {credentials_id}")
        return {}

    @staticmethod
    def _add_safety_limit(query: str, max_rows: Optional[int] = None) -> str:
        if max_rows is None:
            max_rows = SqlConverter.DEFAULT_QUERY_LIMIT

        original_query = query.strip().rstrip(";")

        if "limit" in original_query.lower():
            return original_query

        return f"SELECT * FROM ({original_query}) AS subq LIMIT {max_rows}"

    @staticmethod
    def _extract_rows_from_response(data: Any) -> list:
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            for key in ("rows", "results", "data", "records"):
                if key in data and isinstance(data[key], list):
                    return data[key]
            if any(isinstance(v, (str, int, float, bool, type(None))) for v in data.values()):
                return [data]
            return []
        else:
            logger.warning(f"Unexpected SQL response format: {type(data)}")
            return []

    @staticmethod
    async def _upload_parquet(output_url: str, parquet_data: bytes) -> None:
        async with httpx.AsyncClient(timeout=300) as client:
            response = await client.put(
                output_url,
                content=parquet_data,
                headers={"Content-Type": "application/x-parquet"}
            )
            response.raise_for_status()

        logger.info(f"Uploaded parquet: {len(parquet_data)/1024/1024:.2f}MB")
