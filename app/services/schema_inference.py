import duckdb
import httpx
import logging
import re
import tempfile
import os
from typing import Dict, Any, List, Optional
from ..models.conversionRequest import (
    ColumnSchema, SchemaInfo, ValidationWarning
)

logger = logging.getLogger(__name__)

_NUMERIC_TYPES = {"INTEGER", "BIGINT", "HUGEINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "DECIMAL", "REAL"}
_DATE_TYPES = {"DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", "INTERVAL"}


def _is_numeric(duckdb_type: str) -> bool:
    return any(t in duckdb_type.upper() for t in _NUMERIC_TYPES)


def _is_date(duckdb_type: str) -> bool:
    return any(t in duckdb_type.upper() for t in _DATE_TYPES)


class SchemaInferenceService:
    """Schema inference service using DuckDB"""

    MAX_SAMPLE_SIZE = 10000
    TIMEOUT_SECONDS = 300

    @staticmethod
    async def infer_schema(
        source_url: str,
        file_format: str,
        sample_size: int = 1000
    ) -> Dict[str, Any]:
        """Infer schema from file and return detailed column information"""

        try:
            logger.info(f"Inferring schema for {file_format} file (sample: {sample_size} rows)")

            file_content = await SchemaInferenceService._download_file(source_url)

            # Write to temp file so DuckDB can read it
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=f".{file_format}"
            ) as tmp:
                tmp.write(file_content)
                tmp_path = tmp.name

            try:
                schema_info, sample_data, warnings = SchemaInferenceService._analyze_file(
                    tmp_path, file_format, sample_size, len(file_content)
                )
            finally:
                os.unlink(tmp_path)

            logger.info(f"Schema inferred: {schema_info.total_columns} columns, {schema_info.total_rows} rows")

            return {
                "success": True,
                "schema_info": schema_info.dict(),
                "sample_data": sample_data,
                "warnings": [w.dict() for w in warnings]
            }

        except Exception as e:
            logger.error(f"Schema inference failed: {str(e)}")
            raise

    @staticmethod
    async def _download_file(source_url: str) -> bytes:
        async with httpx.AsyncClient(timeout=SchemaInferenceService.TIMEOUT_SECONDS) as client:
            response = await client.get(source_url)
            response.raise_for_status()
            return response.content

    @staticmethod
    def _build_read_expr(
        conn: duckdb.DuckDBPyConnection,
        file_path: str,
        file_format: str
    ) -> str:
        if file_format == "csv":
            return f"read_csv('{file_path}', null_padding=true, try_parse_dates=true)"
        elif file_format == "tsv":
            return f"read_csv('{file_path}', sep='\\t', null_padding=true, try_parse_dates=true)"
        elif file_format == "parquet":
            return f"read_parquet('{file_path}')"
        elif file_format == "json":
            return f"read_json('{file_path}', auto_detect=true)"
        elif file_format == "excel":
            conn.execute("INSTALL excel; LOAD excel;")
            return f"read_xlsx('{file_path}')"
        else:
            raise ValueError(f"Unsupported format for schema inference: {file_format}")

    @staticmethod
    def _analyze_file(
        file_path: str,
        file_format: str,
        sample_size: int,
        file_size_bytes: int
    ):
        conn = duckdb.connect()
        try:
            memory_limit = os.getenv("DUCKDB_MEMORY_LIMIT", "6GB")
            conn.execute(f"SET memory_limit = '{memory_limit}'")

            read_expr = SchemaInferenceService._build_read_expr(conn, file_path, file_format)

            # Create an in-memory view limited to sample_size
            conn.execute(f"""
                CREATE VIEW sample_data AS
                SELECT * FROM {read_expr}
                LIMIT {sample_size}
            """)

            # Get column metadata
            desc = conn.execute("DESCRIBE sample_data").fetchall()
            total_rows = conn.execute("SELECT COUNT(*) FROM sample_data").fetchone()[0]

            columns = []
            for row in desc:
                col_name = row[0]
                col_type = row[1]

                # Compute basic stats
                stats = conn.execute(f"""
                    SELECT
                        COUNT(*) - COUNT("{col_name}") AS null_count,
                        COUNT(DISTINCT "{col_name}") AS unique_count
                    FROM sample_data
                """).fetchone()

                null_count = stats[0]
                unique_count = stats[1]

                sample_vals = conn.execute(f"""
                    SELECT "{col_name}" FROM sample_data
                    WHERE "{col_name}" IS NOT NULL
                    LIMIT 5
                """).fetchall()
                sample_values = [row[0] for row in sample_vals]

                col_schema = ColumnSchema(
                    name=col_name,
                    inferred_type=col_type,
                    nullable=null_count > 0,
                    null_count=null_count,
                    unique_count=unique_count,
                    sample_values=sample_values
                )

                # String format detection
                if "VARCHAR" in col_type.upper() or "TEXT" in col_type.upper():
                    detected = SchemaInferenceService._detect_string_format(
                        [str(v) for v in sample_values[:20]]
                    )
                    if detected:
                        col_schema.detected_format = detected

                # Numeric min/max
                elif _is_numeric(col_type):
                    try:
                        minmax = conn.execute(
                            f'SELECT MIN("{col_name}"), MAX("{col_name}") FROM sample_data'
                        ).fetchone()
                        if minmax[0] is not None:
                            col_schema.min_value = float(minmax[0])
                        if minmax[1] is not None:
                            col_schema.max_value = float(minmax[1])
                    except Exception:
                        pass

                # Date min/max
                elif _is_date(col_type):
                    try:
                        minmax = conn.execute(
                            f'SELECT MIN("{col_name}"), MAX("{col_name}") FROM sample_data'
                        ).fetchone()
                        if minmax[0] is not None:
                            col_schema.min_value = str(minmax[0])
                        if minmax[1] is not None:
                            col_schema.max_value = str(minmax[1])
                    except Exception:
                        pass

                columns.append(col_schema)

            schema_info = SchemaInfo(
                columns=columns,
                total_rows=total_rows,
                total_columns=len(desc),
                file_size_bytes=file_size_bytes
            )

            # Get sample data as list of dicts
            raw_sample = conn.execute("SELECT * FROM sample_data LIMIT 100").fetchall()
            col_names = [d[0] for d in desc]
            sample_data = [dict(zip(col_names, row)) for row in raw_sample]

            warnings = SchemaInferenceService._detect_warnings(conn, desc)

            return schema_info, sample_data, warnings

        finally:
            conn.close()

    @staticmethod
    def _detect_string_format(sample: List[str]) -> Optional[str]:
        """Detect special formats in a list of string values"""
        if not sample:
            return None

        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if sum(bool(re.match(email_pattern, v)) for v in sample) / len(sample) > 0.8:
            return "email"

        url_pattern = r'^https?://'
        if sum(bool(re.match(url_pattern, v)) for v in sample) / len(sample) > 0.8:
            return "url"

        phone_pattern = r'^\+?[\d\s\-\(\)]{10,}$'
        if sum(bool(re.match(phone_pattern, v)) for v in sample) / len(sample) > 0.8:
            return "phone"

        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        if sum(bool(re.match(uuid_pattern, v.lower())) for v in sample) / len(sample) > 0.8:
            return "uuid"

        return None

    @staticmethod
    def _detect_warnings(
        conn: duckdb.DuckDBPyConnection,
        desc: list
    ) -> List[ValidationWarning]:
        """Detect data quality warnings using SQL"""
        warnings = []
        total = conn.execute("SELECT COUNT(*) FROM sample_data").fetchone()[0]

        for row in desc:
            col_name = row[0]
            col_type = row[1]

            # High null percentage
            null_count = conn.execute(
                f'SELECT COUNT(*) - COUNT("{col_name}") FROM sample_data'
            ).fetchone()[0]
            if total > 0 and (null_count / total) * 100 > 50:
                warnings.append(ValidationWarning(
                    column=col_name,
                    issue="high_null_percentage",
                    message=f"Column has {null_count / total * 100:.1f}% null values",
                    affected_rows=None
                ))

            # Type inconsistencies: VARCHAR columns that look numeric
            if "VARCHAR" in col_type.upper() or "TEXT" in col_type.upper():
                sample_vals = conn.execute(f"""
                    SELECT "{col_name}", ROW_NUMBER() OVER () AS rn
                    FROM sample_data
                    WHERE "{col_name}" IS NOT NULL
                    LIMIT 100
                """).fetchall()

                if sample_vals:
                    numeric_like = sum(
                        bool(re.match(r'^-?\d+\.?\d*$', str(v[0])))
                        for v in sample_vals[:20]
                    )
                    if len(sample_vals) > 0 and numeric_like / min(20, len(sample_vals)) > 0.5:
                        non_numeric_rows = [
                            int(v[1]) for v in sample_vals
                            if not re.match(r'^-?\d+\.?\d*$', str(v[0]))
                        ]
                        if non_numeric_rows:
                            warnings.append(ValidationWarning(
                                column=col_name,
                                issue="type_inconsistency",
                                message="Column looks numeric but contains non-numeric values",
                                affected_rows=non_numeric_rows[:10]
                            ))

        return warnings
