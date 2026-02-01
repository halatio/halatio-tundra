import polars as pl
import httpx
import json
import time
import logging
import tempfile
import os
from io import BytesIO
from typing import Dict, Any, Optional
from ..models.conversionRequest import ConversionMetadata
import anyio

logger = logging.getLogger(__name__)

class FileConverter:
    """Production file converter with streaming support"""

    # Processing limits for production safety
    MAX_DOWNLOAD_SIZE = 500 * 1024 * 1024  # 500MB max file size
    TIMEOUT_SECONDS = 600  # 10 minutes max processing

    @staticmethod
    async def convert(
        source_url: str,
        output_url: str,
        file_format: str,
        options: Dict[str, Any] = {}
    ) -> Dict[str, Any]:
        """Convert file from R2 source URL to parquet at output URL using streaming where possible"""

        start_time = time.time()
        options = options or {}
        warnings = []
        rows_skipped = 0
        temp_input = None
        temp_output = None

        try:
            logger.info(f"Starting conversion: {file_format} to parquet")

            # Create temp files
            temp_input = tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_format}")
            temp_output = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
            temp_input.close()
            temp_output.close()

            # 1. Download source file to disk (streaming)
            await FileConverter._download_to_file(source_url, temp_input.name)

            # 2. Check if we can use streaming (no transformations)
            has_transformations = any([
                options.get('column_mapping'),
                options.get('type_overrides'),
                options.get('skip_rows')
            ])

            # 3. Process based on format and transformations
            if not has_transformations and file_format in ["csv", "tsv", "parquet"]:
                # Use streaming for memory efficiency
                logger.info("Using streaming conversion (scan/sink)")
                metadata_dict = await anyio.to_thread.run_sync(
                    FileConverter._convert_streaming,
                    temp_input.name,
                    temp_output.name,
                    file_format,
                    options
                )
            else:
                # Use eager loading for transformations or unsupported streaming formats
                logger.info("Using eager conversion (transformations or non-streaming format)")
                df = await anyio.to_thread.run_sync(
                    FileConverter._parse_file,
                    temp_input.name,
                    file_format,
                    options
                )

                # Apply transformations
                df, warnings, rows_skipped = FileConverter._apply_transformations(
                    df, options
                )

                # Write to parquet
                await anyio.to_thread.run_sync(
                    df.write_parquet,
                    temp_output.name,
                    compression="snappy",
                    use_pyarrow=False,
                    statistics=True,
                    row_group_size=50000
                )

                metadata_dict = {
                    "rows": len(df),
                    "columns": len(df.columns),
                    "column_schema": FileConverter._generate_schema(df)
                }

            # 4. Upload parquet from disk (streaming)
            await FileConverter._upload_from_file(output_url, temp_output.name)

            # 5. Generate metadata
            processing_time = time.time() - start_time
            file_size_mb = os.path.getsize(temp_output.name) / 1024 / 1024

            metadata = ConversionMetadata(
                rows=metadata_dict["rows"],
                columns=metadata_dict["columns"],
                column_schema=metadata_dict.get("column_schema", {"fields": []}),
                file_size_mb=round(file_size_mb, 2),
                processing_time_seconds=round(processing_time, 2),
                source_type="file",
                rows_skipped=rows_skipped if rows_skipped > 0 else None,
                warnings=warnings if warnings else None
            )

            logger.info(f"Conversion successful: {metadata_dict['rows']} rows, {file_size_mb:.2f}MB")

            return {
                "success": True,
                "metadata": metadata.dict()
            }

        except Exception as e:
            logger.error(f"Conversion failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
        finally:
            # Clean up temp files
            for temp_file in [temp_input, temp_output]:
                if temp_file and os.path.exists(temp_file.name):
                    try:
                        os.unlink(temp_file.name)
                    except Exception as e:
                        logger.warning(f"Failed to clean up temp file: {e}")

    @staticmethod
    async def _download_to_file(source_url: str, file_path: str) -> None:
        """Download file from R2 to disk with streaming (reduces memory usage)"""

        async with httpx.AsyncClient(timeout=FileConverter.TIMEOUT_SECONDS) as client:
            async with client.stream("GET", source_url) as response:
                response.raise_for_status()

                # Check content length
                content_length = response.headers.get('content-length')
                if content_length and int(content_length) > FileConverter.MAX_DOWNLOAD_SIZE:
                    raise ValueError(f"File too large: {int(content_length)/1024/1024:.1f}MB exceeds 500MB limit")

                # Stream download to disk
                total_size = 0
                with open(file_path, 'wb') as f:
                    async for chunk in response.aiter_bytes(chunk_size=8*1024*1024):  # 8MB chunks
                        total_size += len(chunk)
                        if total_size > FileConverter.MAX_DOWNLOAD_SIZE:
                            raise ValueError("File size exceeds 500MB limit during download")
                        f.write(chunk)

                logger.info(f"Downloaded {total_size/1024/1024:.2f}MB to disk")

    @staticmethod
    def _convert_streaming(
        input_path: str,
        output_path: str,
        file_format: str,
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Stream conversion using scan/sink for constant memory usage"""

        if file_format == "csv":
            lf = pl.scan_csv(
                input_path,
                encoding=options.get('encoding', 'utf-8'),
                separator=options.get('delimiter', ','),
                has_header=options.get('has_header', True),
                try_parse_dates=True,
                null_values=["", "NULL", "null", "N/A", "n/a"],
                ignore_errors=True,
                infer_schema_length=None
            )
        elif file_format == "tsv":
            lf = pl.scan_csv(
                input_path,
                encoding=options.get('encoding', 'utf-8'),
                separator=options.get('delimiter', '\t'),
                has_header=options.get('has_header', True),
                try_parse_dates=True,
                null_values=["", "NULL", "null", "N/A", "n/a"],
                ignore_errors=True,
                infer_schema_length=None
            )
        elif file_format == "parquet":
            lf = pl.scan_parquet(input_path)
        else:
            raise ValueError(f"Streaming not supported for {file_format}")

        # Write with sink for streaming (constant memory usage)
        lf.sink_parquet(
            output_path,
            compression="snappy",
            statistics=True,
            row_group_size=50000
        )

        # Collect metadata (requires reading the output - but this is a one-time sample)
        df_meta = pl.read_parquet(output_path)

        return {
            "rows": len(df_meta),
            "columns": len(df_meta.columns),
            "column_schema": FileConverter._generate_schema(df_meta)
        }

    @staticmethod
    def _parse_file(file_path: str, file_format: str, options: Dict[str, Any]) -> pl.DataFrame:
        """Parse file from disk with Polars native methods"""

        try:
            if file_format == "csv":
                return pl.read_csv(
                    file_path,
                    encoding=options.get('encoding', 'utf-8'),
                    separator=options.get('delimiter', ','),
                    has_header=options.get('has_header', True),
                    try_parse_dates=True,
                    null_values=["", "NULL", "null", "N/A", "n/a"],
                    ignore_errors=True,
                    infer_schema_length=None
                )

            elif file_format == "tsv":
                return pl.read_csv(
                    file_path,
                    encoding=options.get('encoding', 'utf-8'),
                    separator=options.get('delimiter', '\t'),
                    has_header=options.get('has_header', True),
                    try_parse_dates=True,
                    null_values=["", "NULL", "null", "N/A", "n/a"],
                    ignore_errors=True,
                    infer_schema_length=None
                )

            elif file_format == "excel":
                sheet_name = options.get('sheet_name')
                sheet_index = options.get('sheet_index', 0)
                sheet = sheet_name if sheet_name else sheet_index

                logger.info(f"Reading Excel sheet: {sheet}")

                return pl.read_excel(
                    file_path,
                    sheet_name=sheet,
                    engine='calamine',
                    infer_schema_length=10000
                )

            elif file_format == "parquet":
                logger.info("Reading Parquet file")
                return pl.read_parquet(file_path)

            elif file_format == "json":
                return pl.read_json(file_path)

            else:
                raise ValueError(f"Unsupported file format: {file_format}")

        except Exception as e:
            raise ValueError(f"Failed to parse {file_format} file: {str(e)}")

    @staticmethod
    def _apply_transformations(
        df: pl.DataFrame,
        options: Dict[str, Any]
    ) -> tuple[pl.DataFrame, list, int]:
        """Apply transformations to DataFrame"""

        warnings = []
        rows_skipped = 0

        # Column mapping
        if options.get('column_mapping'):
            df = df.rename(options['column_mapping'])
            logger.info(f"Applied column mapping: {len(options['column_mapping'])} columns renamed")

        # Type overrides
        if options.get('type_overrides'):
            for col, dtype in options['type_overrides'].items():
                try:
                    df = df.with_columns(pl.col(col).cast(dtype))
                except Exception as e:
                    warnings.append(f"Could not cast column '{col}' to {dtype}: {str(e)}")
            logger.info(f"Applied type overrides: {len(options['type_overrides'])} columns")

        # Skip rows
        if options.get('skip_rows'):
            skip_rows = options['skip_rows']
            rows_skipped = len(skip_rows)
            keep_mask = [i not in skip_rows for i in range(len(df))]
            df = df.filter(pl.Series(keep_mask))
            logger.info(f"Skipped {rows_skipped} rows")

        return df, warnings, rows_skipped

    @staticmethod
    async def _upload_from_file(output_url: str, file_path: str) -> None:
        """Upload parquet file to R2 using signed URL (streaming reduces memory)"""

        async with httpx.AsyncClient(timeout=300) as client:
            with open(file_path, 'rb') as f:
                response = await client.put(
                    output_url,
                    content=f.read(),
                    headers={"Content-Type": "application/x-parquet"}
                )
                response.raise_for_status()

        file_size_mb = os.path.getsize(file_path) / 1024 / 1024
        logger.info(f"Uploaded parquet to R2: {file_size_mb:.2f}MB")

    @staticmethod
    def _generate_schema(df: pl.DataFrame) -> Dict[str, Any]:
        """Generate schema information for the dataset"""

        fields = []
        for col, dtype in zip(df.columns, df.dtypes):
            if col.startswith('_'):
                continue

            field_info = {
                "name": col,
                "type": str(dtype),
                "polars_type": str(dtype)
            }

            if dtype.is_numeric():
                try:
                    stats = df.select(pl.col(col)).describe()
                    field_info["nullable"] = df.select(pl.col(col).is_null().any()).item()
                except:
                    pass

            fields.append(field_info)

        return {
            "fields": fields,
            "format": "parquet",
            "encoding": "utf-8"
        }
