import polars as pl
import httpx
import logging
import re
from io import BytesIO
from typing import Dict, Any, List, Optional
from ..models.conversionRequest import (
    ColumnSchema, SchemaInfo, ValidationWarning
)

logger = logging.getLogger(__name__)

class SchemaInferenceService:
    """Service for inferring schema from data files"""

    MAX_SAMPLE_SIZE = 10000
    TIMEOUT_SECONDS = 300  # 5 minutes for schema inference

    @staticmethod
    async def infer_schema(
        source_url: str,
        file_format: str,
        sample_size: int = 1000
    ) -> Dict[str, Any]:
        """Infer schema from file and return detailed column information"""

        try:
            logger.info(f"🔍 Inferring schema for {file_format} file (sample: {sample_size} rows)")

            # 1. Download file
            file_content = await SchemaInferenceService._download_file(source_url)

            # 2. Parse file and get sample
            df = SchemaInferenceService._parse_file_sample(
                file_content, file_format, sample_size
            )

            # 3. Infer detailed schema
            schema_info = SchemaInferenceService._infer_detailed_schema(df)

            # 4. Generate sample data
            sample_data = df.head(min(100, sample_size)).to_dicts()

            # 5. Detect warnings
            warnings = SchemaInferenceService._detect_warnings(df)

            logger.info(f"✅ Schema inferred: {len(df.columns)} columns, {len(df)} rows")

            return {
                "success": True,
                "schema": schema_info.dict(),
                "sample_data": sample_data,
                "warnings": [w.dict() for w in warnings]
            }

        except Exception as e:
            logger.error(f"❌ Schema inference failed: {str(e)}")
            raise

    @staticmethod
    async def _download_file(source_url: str) -> bytes:
        """Download file from R2"""
        async with httpx.AsyncClient(timeout=SchemaInferenceService.TIMEOUT_SECONDS) as client:
            response = await client.get(source_url)
            response.raise_for_status()
            return response.content

    @staticmethod
    def _parse_file_sample(
        content: bytes,
        file_format: str,
        sample_size: int
    ) -> pl.DataFrame:
        """Parse file and return sample DataFrame"""

        if file_format == "csv":
            return pl.read_csv(
                BytesIO(content),
                n_rows=sample_size,
                try_parse_dates=True,
                null_values=["", "NULL", "null", "N/A", "n/a"],
                ignore_errors=False  # Don't ignore errors for validation
            )

        elif file_format == "tsv":
            return pl.read_csv(
                BytesIO(content),
                separator='\t',
                n_rows=sample_size,
                try_parse_dates=True,
                null_values=["", "NULL", "null", "N/A", "n/a"],
                ignore_errors=False
            )

        elif file_format == "excel":
            # Read full file, then take sample
            df = pl.read_excel(
                BytesIO(content),
                engine='calamine',
                infer_schema_length=sample_size
            )
            return df.head(sample_size)

        elif file_format == "parquet":
            # Read parquet and take sample
            df = pl.read_parquet(BytesIO(content))
            return df.head(sample_size)

        elif file_format == "json":
            df = pl.read_json(BytesIO(content))
            return df.head(sample_size)

        else:
            raise ValueError(f"Unsupported format for schema inference: {file_format}")

    @staticmethod
    def _infer_detailed_schema(df: pl.DataFrame) -> SchemaInfo:
        """Generate detailed schema information"""

        columns = []

        for col_name in df.columns:
            col = df[col_name]
            dtype = col.dtype

            col_schema = ColumnSchema(
                name=col_name,
                inferred_type=str(dtype),
                nullable=col.null_count() > 0,
                null_count=col.null_count(),
                unique_count=col.n_unique(),
                sample_values=col.drop_nulls().head(5).to_list()
            )

            # Detect special formats for string columns
            if dtype == pl.Utf8:
                detected_format = SchemaInferenceService._detect_string_format(col)
                if detected_format:
                    col_schema.detected_format = detected_format

            # Add min/max for numeric and date columns
            elif dtype.is_numeric():
                try:
                    col_schema.min_value = float(col.min())
                    col_schema.max_value = float(col.max())
                except:
                    pass

            elif dtype in [pl.Date, pl.Datetime]:
                try:
                    col_schema.min_value = str(col.min())
                    col_schema.max_value = str(col.max())
                except:
                    pass

            columns.append(col_schema)

        return SchemaInfo(
            columns=columns,
            total_rows=len(df),
            total_columns=len(df.columns),
            file_size_bytes=df.estimated_size()
        )

    @staticmethod
    def _detect_string_format(col: pl.Series) -> Optional[str]:
        """Detect special formats in string columns"""

        # Get non-null values
        valid_values = col.drop_nulls()

        if len(valid_values) == 0:
            return None

        # Take sample
        sample = valid_values.head(20).to_list()

        # Email pattern
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if sum(bool(re.match(email_pattern, str(v))) for v in sample) / len(sample) > 0.8:
            return "email"

        # URL pattern
        url_pattern = r'^https?://'
        if sum(bool(re.match(url_pattern, str(v))) for v in sample) / len(sample) > 0.8:
            return "url"

        # Phone pattern (simple)
        phone_pattern = r'^\+?[\d\s\-\(\)]{10,}$'
        if sum(bool(re.match(phone_pattern, str(v))) for v in sample) / len(sample) > 0.8:
            return "phone"

        # UUID pattern
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        if sum(bool(re.match(uuid_pattern, str(v).lower())) for v in sample) / len(sample) > 0.8:
            return "uuid"

        return None

    @staticmethod
    def _detect_warnings(df: pl.DataFrame) -> List[ValidationWarning]:
        """Detect data quality warnings"""

        warnings = []

        for col_name in df.columns:
            col = df[col_name]
            dtype = col.dtype

            # Check for type inconsistencies (numeric columns with unparseable values)
            if dtype == pl.Utf8:
                # Check if column looks numeric but couldn't be parsed
                non_null = col.drop_nulls()
                if len(non_null) > 0:
                    # Try to see if values look like numbers
                    numeric_like = sum(
                        bool(re.match(r'^-?\d+\.?\d*$', str(v)))
                        for v in non_null.head(20).to_list()
                    )

                    if numeric_like / min(20, len(non_null)) > 0.5:
                        # Find non-numeric values
                        non_numeric_rows = []
                        for i, val in enumerate(col.to_list()[:100]):
                            if val is not None and not re.match(r'^-?\d+\.?\d*$', str(val)):
                                non_numeric_rows.append(i + 1)

                        if non_numeric_rows:
                            warnings.append(ValidationWarning(
                                column=col_name,
                                issue="type_inconsistency",
                                message=f"Column looks numeric but contains non-numeric values",
                                affected_rows=non_numeric_rows[:10]  # Show first 10
                            ))

            # Check for high null percentage
            null_percentage = (col.null_count() / len(col)) * 100
            if null_percentage > 50:
                warnings.append(ValidationWarning(
                    column=col_name,
                    issue="high_null_percentage",
                    message=f"Column has {null_percentage:.1f}% null values",
                    affected_rows=None
                ))

        return warnings
