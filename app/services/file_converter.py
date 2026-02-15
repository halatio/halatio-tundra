import asyncio
import duckdb
import time
import logging
import os
from typing import Any, Dict

from ..models.conversionRequest import ConversionMetadata
from .connectors.duckdb_base import _make_duckdb_config
import anyio

logger = logging.getLogger(__name__)

# Timeout for blocking file conversion operations (matches MAX_PROCESSING_TIME_MINUTES)
_CONVERSION_TIMEOUT_SECONDS = int(os.getenv("MAX_PROCESSING_TIME_MINUTES", "10")) * 60


class FileConverter:
    """
    Convert files to Parquet using DuckDB with direct R2 read/write.

    No temp files or httpx downloads: DuckDB reads from and writes to
    R2 directly via the persistent R2 secret created at lifespan startup.
    """

    @staticmethod
    async def convert(
        source_path: str,
        output_path: str,
        file_format: str,
        options: Dict[str, Any] = {},
        version: int = 1,
    ) -> Dict[str, Any]:
        """
        Convert a file to Parquet.

        Args:
            source_path: R2 URL (r2://bucket/key) or local path
            output_path: R2 URL (r2://bucket/key.parquet) or local path
            file_format: csv, tsv, json, geojson, excel, parquet
            options: Conversion options (column_mapping, type_overrides, skip_rows, …)
            version: Source version number for metadata
        """
        start_time = time.time()
        options = options or {}

        try:
            async with asyncio.timeout(_CONVERSION_TIMEOUT_SECONDS):
                result = await anyio.to_thread.run_sync(
                    FileConverter._convert_with_duckdb,
                    source_path,
                    output_path,
                    file_format,
                    options,
                    cancellable=True,
                )

            processing_time = time.time() - start_time

            # File size is only knowable for local output paths
            file_size_mb = 0.0
            if not output_path.startswith("r2://") and os.path.exists(output_path):
                file_size_mb = os.path.getsize(output_path) / 1024 / 1024

            metadata = ConversionMetadata(
                version=version,
                rows=result["rows"],
                columns=result["columns"],
                column_schema=result.get("column_schema", {"fields": []}),
                file_size_mb=round(file_size_mb, 2),
                processing_time_seconds=round(processing_time, 2),
                source_type="file",
                rows_skipped=result["rows_skipped"] if result["rows_skipped"] else None,
                warnings=result["warnings"] if result["warnings"] else None,
            )

            logger.info(f"Conversion complete: {result['rows']} rows")
            return {"success": True, "metadata": metadata.dict()}

        except asyncio.TimeoutError:
            logger.error(
                "File conversion timed out after %d seconds: %s",
                _CONVERSION_TIMEOUT_SECONDS,
                source_path,
            )
            return {
                "success": False,
                "error": f"Conversion exceeded {_CONVERSION_TIMEOUT_SECONDS}s timeout",
            }
        except asyncio.CancelledError:
            logger.warning("File conversion cancelled: %s", source_path)
            raise
        except Exception as e:
            logger.error(f"Conversion failed: {e}")
            return {"success": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Synchronous DuckDB core (runs in thread pool)
    # ------------------------------------------------------------------

    @staticmethod
    def _convert_with_duckdb(
        source_path: str,
        output_path: str,
        file_format: str,
        options: Dict[str, Any],
    ) -> Dict[str, Any]:
        conn = duckdb.connect(":memory:", config=_make_duckdb_config())
        try:
            conn.execute("SET autoinstall_known_extensions = false")
            conn.execute("SET autoload_known_extensions = true")

            read_expr = FileConverter._build_read_expr(conn, source_path, file_format, options)
            select_sql, warnings, rows_skipped = FileConverter._build_select(conn, read_expr, options)

            conn.execute(f"""
                COPY ({select_sql})
                TO '{output_path}'
                (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 50000)
            """)

            # Read back schema from output (works for both local and R2 paths)
            desc = conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{output_path}') LIMIT 0"
            ).fetchall()
            row_count = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
            ).fetchone()[0]

            schema = {"fields": [{"name": r[0], "type": r[1]} for r in desc]}

            return {
                "rows": row_count,
                "columns": len(desc),
                "column_schema": schema,
                "warnings": warnings,
                "rows_skipped": rows_skipped,
            }
        finally:
            conn.close()

    @staticmethod
    def _build_read_expr(
        conn: duckdb.DuckDBPyConnection,
        source_path: str,
        file_format: str,
        options: Dict[str, Any],
    ) -> str:
        if file_format in ("csv", "tsv"):
            sep = options.get("delimiter", "\t" if file_format == "tsv" else ",")
            sep = sep.replace("'", "\\'")
            return (
                f"read_csv('{source_path}', sep='{sep}', "
                "null_padding=true, ignore_errors=true, try_parse_dates=true)"
            )
        elif file_format == "parquet":
            return f"read_parquet('{source_path}')"
        elif file_format in ("json", "geojson"):
            return f"read_json('{source_path}', auto_detect=true)"
        elif file_format == "excel":
            conn.execute("LOAD excel")
            sheet = options.get("sheet_name")
            if sheet:
                return f"read_xlsx('{source_path}', sheet='{sheet}')"
            sheet_index = options.get("sheet_index", 0)
            return f"read_xlsx('{source_path}', sheet={sheet_index})"
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

    @staticmethod
    def _build_select(
        conn: duckdb.DuckDBPyConnection,
        read_expr: str,
        options: Dict[str, Any],
    ) -> tuple:
        """Build SELECT SQL with optional column mapping, type overrides, and row skipping."""
        warnings: list = []
        rows_skipped = 0

        desc = conn.execute(f"SELECT * FROM {read_expr} LIMIT 0").description
        columns = [d[0] for d in desc]

        column_mapping = options.get("column_mapping") or {}
        type_overrides = options.get("type_overrides") or {}

        select_parts = []
        for col in columns:
            col_q = f'"{col}"'
            new_name = column_mapping.get(col, col)
            new_q = f'"{new_name}"'

            if new_name in type_overrides:
                dtype = type_overrides[new_name]
                select_parts.append(f"TRY_CAST({col_q} AS {dtype}) AS {new_q}")
            elif col in type_overrides:
                dtype = type_overrides[col]
                select_parts.append(f"TRY_CAST({col_q} AS {dtype}) AS {new_q}")
            elif col != new_name:
                select_parts.append(f"{col_q} AS {new_q}")
            else:
                select_parts.append(col_q)

        sql = f"SELECT {', '.join(select_parts)} FROM {read_expr}"

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
