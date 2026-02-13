import duckdb
import logging
import re
from typing import Any, Dict, List, Optional

from ..models.conversionRequest import ColumnSchema, SchemaInfo, ValidationWarning
from .connectors.duckdb_base import _make_duckdb_config

logger = logging.getLogger(__name__)

_NUMERIC_TYPES = {"INTEGER", "BIGINT", "HUGEINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "DECIMAL", "REAL"}
_DATE_TYPES = {"DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", "INTERVAL"}


def _is_numeric(t: str) -> bool:
    return any(k in t.upper() for k in _NUMERIC_TYPES)


def _is_date(t: str) -> bool:
    return any(k in t.upper() for k in _DATE_TYPES)


class SchemaInferenceService:
    """Schema inference service using DuckDB — reads directly from R2 or local paths."""

    MAX_SAMPLE_SIZE = 10000

    @staticmethod
    def infer_schema(
        source_path: str,
        file_format: str,
        sample_size: int = 1000,
    ) -> Dict[str, Any]:
        """
        Infer schema from a file and return detailed column information.

        Args:
            source_path: R2 URL (r2://bucket/key) or local file path
            file_format: csv, tsv, json, geojson, excel, parquet
            sample_size: Number of rows to analyse (max 10 000)

        Returns:
            Dict with success, schema_info, sample_data, warnings
        """
        sample_size = min(sample_size, SchemaInferenceService.MAX_SAMPLE_SIZE)

        conn = duckdb.connect(":memory:", config=_make_duckdb_config())
        try:
            conn.execute("SET autoinstall_known_extensions = false")
            conn.execute("SET autoload_known_extensions = true")

            read_expr = SchemaInferenceService._build_read_expr(conn, source_path, file_format)

            conn.execute(f"""
                CREATE VIEW sample_data AS
                SELECT * FROM {read_expr}
                LIMIT {sample_size}
            """)

            desc = conn.execute("DESCRIBE sample_data").fetchall()
            total_rows = conn.execute("SELECT COUNT(*) FROM sample_data").fetchone()[0]

            columns: List[ColumnSchema] = []
            for row in desc:
                col_name = row[0]
                col_type = row[1]

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
                sample_values = [r[0] for r in sample_vals]

                col_schema = ColumnSchema(
                    name=col_name,
                    inferred_type=col_type,
                    nullable=null_count > 0,
                    null_count=null_count,
                    unique_count=unique_count,
                    sample_values=sample_values,
                )

                if "VARCHAR" in col_type.upper() or "TEXT" in col_type.upper():
                    detected = SchemaInferenceService._detect_string_format(
                        [str(v) for v in sample_values[:20]]
                    )
                    if detected:
                        col_schema.detected_format = detected

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
                file_size_bytes=None,
            )

            raw_sample = conn.execute("SELECT * FROM sample_data LIMIT 100").fetchall()
            col_names = [d[0] for d in desc]
            sample_data = [dict(zip(col_names, row)) for row in raw_sample]

            warnings = SchemaInferenceService._detect_warnings(conn, desc)

            logger.info(f"Schema inferred: {len(columns)} columns, {total_rows} rows")

            return {
                "success": True,
                "schema_info": schema_info.dict(),
                "sample_data": sample_data,
                "warnings": [w.dict() for w in warnings],
            }

        except Exception as e:
            logger.error(f"Schema inference failed: {e}")
            raise
        finally:
            conn.close()

    @staticmethod
    def _build_read_expr(
        conn: duckdb.DuckDBPyConnection,
        source_path: str,
        file_format: str,
    ) -> str:
        if file_format == "csv":
            return f"read_csv('{source_path}', null_padding=true, try_parse_dates=true)"
        elif file_format == "tsv":
            return f"read_csv('{source_path}', sep='\\t', null_padding=true, try_parse_dates=true)"
        elif file_format == "parquet":
            return f"read_parquet('{source_path}')"
        elif file_format in ("json", "geojson"):
            return f"read_json('{source_path}', auto_detect=true)"
        elif file_format == "excel":
            conn.execute("LOAD excel")
            return f"read_xlsx('{source_path}')"
        else:
            raise ValueError(f"Unsupported format for schema inference: {file_format}")

    @staticmethod
    def _detect_string_format(sample: List[str]) -> Optional[str]:
        if not sample:
            return None
        checks = [
            ("email", r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'),
            ("url",   r'^https?://'),
            ("phone", r'^\+?[\d\s\-\(\)]{10,}$'),
            ("uuid",  r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'),
        ]
        for name, pattern in checks:
            if sum(bool(re.match(pattern, v.lower() if name == "uuid" else v)) for v in sample) / len(sample) > 0.8:
                return name
        return None

    @staticmethod
    def _detect_warnings(
        conn: duckdb.DuckDBPyConnection,
        desc: list,
    ) -> List[ValidationWarning]:
        warnings: List[ValidationWarning] = []
        total = conn.execute("SELECT COUNT(*) FROM sample_data").fetchone()[0]

        for row in desc:
            col_name = row[0]
            col_type = row[1]

            null_count = conn.execute(
                f'SELECT COUNT(*) - COUNT("{col_name}") FROM sample_data'
            ).fetchone()[0]
            if total > 0 and (null_count / total) * 100 > 50:
                warnings.append(ValidationWarning(
                    column=col_name,
                    issue="high_null_percentage",
                    message=f"Column has {null_count / total * 100:.1f}% null values",
                    affected_rows=None,
                ))

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
                        non_numeric = [
                            int(v[1]) for v in sample_vals
                            if not re.match(r'^-?\d+\.?\d*$', str(v[0]))
                        ]
                        if non_numeric:
                            warnings.append(ValidationWarning(
                                column=col_name,
                                issue="type_inconsistency",
                                message="Column looks numeric but contains non-numeric values",
                                affected_rows=non_numeric[:10],
                            ))

        return warnings
