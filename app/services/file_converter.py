import duckdb
import httpx
import time
import logging
import tempfile
import os
from typing import Dict, Any, Optional
from ..models.conversionRequest import ConversionMetadata
import anyio

logger = logging.getLogger(__name__)


def _configure_duckdb(conn: duckdb.DuckDBPyConnection) -> None:
    memory_limit = os.getenv("DUCKDB_MEMORY_LIMIT", "6GB")
    temp_dir = os.getenv("DUCKDB_TEMP_DIR", "/tmp/duckdb")
    threads = int(os.getenv("DUCKDB_THREADS", "2"))
    os.makedirs(temp_dir, exist_ok=True)
    conn.execute(f"SET memory_limit = '{memory_limit}'")
    conn.execute(f"SET temp_directory = '{temp_dir}'")
    conn.execute(f"SET threads = {threads}")


class FileConverter:
    """File converter using DuckDB"""

    MAX_DOWNLOAD_SIZE = 500 * 1024 * 1024  # 500MB
    TIMEOUT_SECONDS = 600

    @staticmethod
    async def convert(
        source_url: str,
        output_url: str,
        file_format: str,
        options: Dict[str, Any] = {}
    ) -> Dict[str, Any]:
        """Convert file to parquet using DuckDB"""

        start_time = time.time()
        options = options or {}
        temp_input = None
        temp_output = None

        try:
            temp_input = tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_format}")
            temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
            temp_input.close()
            temp_output.close()

            await FileConverter._download_to_file(source_url, temp_input.name)

            result = await anyio.to_thread.run_sync(
                FileConverter._convert_with_duckdb,
                temp_input.name,
                temp_output.name,
                file_format,
                options
            )

            await FileConverter._upload_from_file(output_url, temp_output.name)

            processing_time = time.time() - start_time
            file_size_mb = os.path.getsize(temp_output.name) / 1024 / 1024

            metadata = ConversionMetadata(
                rows=result["rows"],
                columns=result["columns"],
                column_schema=result.get("column_schema", {"fields": []}),
                file_size_mb=round(file_size_mb, 2),
                processing_time_seconds=round(processing_time, 2),
                source_type="file",
                rows_skipped=result["rows_skipped"] if result["rows_skipped"] > 0 else None,
                warnings=result["warnings"] if result["warnings"] else None
            )

            logger.info(f"Conversion complete: {result['rows']} rows, {file_size_mb:.2f}MB")
            return {"success": True, "metadata": metadata.dict()}

        except Exception as e:
            logger.error(f"Conversion failed: {str(e)}")
            return {"success": False, "error": str(e)}
        finally:
            for temp_file in [temp_input, temp_output]:
                if temp_file and os.path.exists(temp_file.name):
                    try:
                        os.unlink(temp_file.name)
                    except Exception as cleanup_err:
                        logger.warning(f"Failed to clean up temp file: {cleanup_err}")

    @staticmethod
    async def _download_to_file(source_url: str, file_path: str) -> None:
        """Stream download file to disk"""
        async with httpx.AsyncClient(timeout=FileConverter.TIMEOUT_SECONDS) as client:
            async with client.stream("GET", source_url) as response:
                response.raise_for_status()

                content_length = response.headers.get("content-length")
                if content_length and int(content_length) > FileConverter.MAX_DOWNLOAD_SIZE:
                    raise ValueError(
                        f"File too large: {int(content_length)/1024/1024:.1f}MB exceeds 500MB limit"
                    )

                total_size = 0
                with open(file_path, "wb") as f:
                    async for chunk in response.aiter_bytes(chunk_size=8 * 1024 * 1024):
                        total_size += len(chunk)
                        if total_size > FileConverter.MAX_DOWNLOAD_SIZE:
                            raise ValueError("File size exceeds 500MB limit during download")
                        f.write(chunk)

                logger.info(f"Downloaded {total_size/1024/1024:.2f}MB to disk")

    @staticmethod
    def _convert_with_duckdb(
        input_path: str,
        output_path: str,
        file_format: str,
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Perform conversion using DuckDB"""

        conn = duckdb.connect()
        try:
            _configure_duckdb(conn)

            # Build the source read expression
            read_expr = FileConverter._build_read_expr(conn, input_path, file_format, options)

            # Build SELECT with optional transformations
            select_sql, warnings, rows_skipped = FileConverter._build_select(
                conn, read_expr, options
            )

            # Write to parquet
            conn.execute(f"""
                COPY ({select_sql})
                TO '{output_path}'
                (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
            """)

            # Read back metadata
            row_count = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
            ).fetchone()[0]

            desc = conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{output_path}') LIMIT 0"
            ).fetchall()
            column_count = len(desc)

            schema = FileConverter._generate_schema(desc)

            return {
                "rows": row_count,
                "columns": column_count,
                "column_schema": schema,
                "warnings": warnings,
                "rows_skipped": rows_skipped
            }
        finally:
            conn.close()

    @staticmethod
    def _build_read_expr(
        conn: duckdb.DuckDBPyConnection,
        input_path: str,
        file_format: str,
        options: Dict[str, Any]
    ) -> str:
        """Build DuckDB read function call for the given format"""

        if file_format in ("csv", "tsv"):
            sep = options.get("delimiter", "\t" if file_format == "tsv" else ",")
            # Escape single quotes in separator
            sep = sep.replace("'", "\\'")
            encoding = options.get("encoding", "utf-8")
            return (
                f"read_csv('{input_path}', sep='{sep}', "
                f"null_padding=true, ignore_errors=true, try_parse_dates=true)"
            )

        elif file_format == "parquet":
            return f"read_parquet('{input_path}')"

        elif file_format == "json":
            return f"read_json('{input_path}', auto_detect=true)"

        elif file_format == "excel":
            conn.execute("INSTALL excel; LOAD excel;")
            sheet = options.get("sheet_name")
            if sheet:
                return f"read_xlsx('{input_path}', sheet='{sheet}')"
            sheet_index = options.get("sheet_index", 0)
            return f"read_xlsx('{input_path}', sheet={sheet_index})"

        else:
            raise ValueError(f"Unsupported file format: {file_format}")

    @staticmethod
    def _build_select(
        conn: duckdb.DuckDBPyConnection,
        read_expr: str,
        options: Dict[str, Any]
    ) -> tuple:
        """Build SELECT SQL with column mapping, type overrides, and row skipping"""

        warnings = []
        rows_skipped = 0

        # Get column names from the read expression
        desc = conn.execute(f"SELECT * FROM {read_expr} LIMIT 0").description
        columns = [d[0] for d in desc]

        column_mapping = options.get("column_mapping") or {}
        type_overrides = options.get("type_overrides") or {}

        # Build SELECT with column mapping and type casts
        select_parts = []
        for col in columns:
            col_quoted = f'"{col}"'
            new_name = column_mapping.get(col, col)
            new_name_quoted = f'"{new_name}"'

            if new_name in type_overrides:
                dtype = type_overrides[new_name]
                select_parts.append(f"TRY_CAST({col_quoted} AS {dtype}) AS {new_name_quoted}")
            elif col in type_overrides:
                dtype = type_overrides[col]
                select_parts.append(f"TRY_CAST({col_quoted} AS {dtype}) AS {new_name_quoted}")
            elif col != new_name:
                select_parts.append(f"{col_quoted} AS {new_name_quoted}")
            else:
                select_parts.append(col_quoted)

        sql = f"SELECT {', '.join(select_parts)} FROM {read_expr}"

        # Handle skip_rows by filtering on row index
        if options.get("skip_rows"):
            skip_set = set(options["skip_rows"])
            rows_skipped = len(skip_set)
            skip_list = ", ".join(str(i) for i in skip_set)
            sql = f"""
                SELECT * EXCLUDE (___rn)
                FROM (
                    SELECT *, (ROW_NUMBER() OVER ()) - 1 AS ___rn
                    FROM ({sql}) AS _inner
                ) AS _outer
                WHERE ___rn NOT IN ({skip_list})
            """

        return sql, warnings, rows_skipped

    @staticmethod
    def _generate_schema(desc: list) -> Dict[str, Any]:
        """Generate schema from DuckDB DESCRIBE results"""
        fields = [
            {"name": row[0], "type": row[1]}
            for row in desc
            if not row[0].startswith("_")
        ]
        return {"fields": fields, "format": "parquet", "encoding": "utf-8"}

    @staticmethod
    async def _upload_from_file(output_url: str, file_path: str) -> None:
        """Upload parquet file to storage via signed URL"""
        async with httpx.AsyncClient(timeout=300) as client:
            with open(file_path, "rb") as f:
                response = await client.put(
                    output_url,
                    content=f.read(),
                    headers={"Content-Type": "application/x-parquet"}
                )
                response.raise_for_status()

        file_size_mb = os.path.getsize(file_path) / 1024 / 1024
        logger.info(f"Uploaded parquet: {file_size_mb:.2f}MB")
