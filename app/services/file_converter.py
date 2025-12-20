import polars as pl
import httpx
import json
import time
import logging
from io import BytesIO
from typing import Dict, Any
from ..models.conversionRequest import ConversionMetadata

logger = logging.getLogger(__name__)

class FileConverter:
    """Production file converter - R2 to Parquet only"""
    
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
        """Convert file from R2 source URL to parquet at output URL"""

        start_time = time.time()
        options = options or {}
        warnings = []
        rows_skipped = 0

        try:
            logger.info(f"🔄 Starting conversion: {file_format} → parquet")

            # 1. Download source file from R2
            file_content = await FileConverter._download_file(source_url)

            # 2. Parse with Polars based on format
            df = FileConverter._parse_file(file_content, file_format, options)

            # 3. Apply transformations if provided
            if options.get('column_mapping'):
                df = df.rename(options['column_mapping'])
                logger.info(f"✏️ Applied column mapping: {len(options['column_mapping'])} columns renamed")

            if options.get('type_overrides'):
                for col, dtype in options['type_overrides'].items():
                    try:
                        df = df.with_columns(pl.col(col).cast(dtype))
                    except Exception as e:
                        warnings.append(f"Could not cast column '{col}' to {dtype}: {str(e)}")
                logger.info(f"🔄 Applied type overrides: {len(options['type_overrides'])} columns")

            if options.get('skip_rows'):
                skip_rows = options['skip_rows']
                rows_skipped = len(skip_rows)
                # Create a mask for rows to keep
                keep_mask = [i not in skip_rows for i in range(len(df))]
                df = df.filter(pl.Series(keep_mask))
                logger.info(f"⏭️ Skipped {rows_skipped} rows")

            # 4. Convert to parquet
            parquet_buffer = FileConverter._convert_to_parquet(df)

            # 5. Upload parquet to R2
            await FileConverter._upload_parquet(output_url, parquet_buffer)

            # 6. Generate metadata
            processing_time = time.time() - start_time
            file_size_mb = len(parquet_buffer) / 1024 / 1024

            metadata = ConversionMetadata(
                rows=len(df),
                columns=len(df.columns),
                column_schema=FileConverter._generate_schema(df),
                file_size_mb=round(file_size_mb, 2),
                processing_time_seconds=round(processing_time, 2),
                source_type="file",
                rows_skipped=rows_skipped if rows_skipped > 0 else None,
                warnings=warnings if warnings else None
            )

            logger.info(f"✅ Conversion successful: {len(df)} rows, {file_size_mb:.2f}MB")

            return {
                "success": True,
                "metadata": metadata.dict()
            }

        except Exception as e:
            logger.error(f"❌ Conversion failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    @staticmethod
    async def _download_file(source_url: str) -> bytes:
        """Download file from R2 with size and timeout limits"""
        
        async with httpx.AsyncClient(timeout=FileConverter.TIMEOUT_SECONDS) as client:
            async with client.stream("GET", source_url) as response:
                response.raise_for_status()
                
                # Check content length
                content_length = response.headers.get('content-length')
                if content_length and int(content_length) > FileConverter.MAX_DOWNLOAD_SIZE:
                    raise ValueError(f"File too large: {int(content_length)/1024/1024:.1f}MB exceeds 500MB limit")
                
                # Download with size checking
                content = b""
                async for chunk in response.aiter_bytes(chunk_size=8*1024*1024):  # 8MB chunks
                    content += chunk
                    if len(content) > FileConverter.MAX_DOWNLOAD_SIZE:
                        raise ValueError("File size exceeds 500MB limit during download")
                
                logger.info(f"📥 Downloaded {len(content)/1024/1024:.2f}MB from R2")
                return content
    
    @staticmethod
    def _parse_file(content: bytes, file_format: str, options: Dict[str, Any]) -> pl.DataFrame:
        """Parse file content with Polars native methods"""

        try:
            if file_format == "csv":
                return pl.read_csv(
                    BytesIO(content),
                    encoding=options.get('encoding', 'utf-8'),
                    separator=options.get('delimiter', ','),
                    has_header=options.get('has_header', True),
                    try_parse_dates=True,
                    null_values=["", "NULL", "null", "N/A", "n/a"],
                    ignore_errors=True,
                    infer_schema_length=None  # Infer from all rows
                )

            elif file_format == "tsv":
                return pl.read_csv(
                    BytesIO(content),
                    encoding=options.get('encoding', 'utf-8'),
                    separator=options.get('delimiter', '\t'),
                    has_header=options.get('has_header', True),
                    try_parse_dates=True,
                    null_values=["", "NULL", "null", "N/A", "n/a"],
                    ignore_errors=True,
                    infer_schema_length=None
                )

            elif file_format == "excel":
                # Use sheet_name or sheet_index from options
                sheet_name = options.get('sheet_name')
                sheet_index = options.get('sheet_index', 0)
                sheet = sheet_name if sheet_name else sheet_index

                logger.info(f"📊 Reading Excel sheet: {sheet}")

                return pl.read_excel(
                    BytesIO(content),
                    sheet_name=sheet,
                    engine='calamine',  # Fast Rust-based engine
                    infer_schema_length=10000
                )

            elif file_format == "parquet":
                # Parquet is already in the target format, but we might apply transformations
                logger.info(f"📦 Reading Parquet file")
                return pl.read_parquet(BytesIO(content))

            elif file_format == "json":
                return pl.read_json(BytesIO(content))

            else:
                raise ValueError(f"Unsupported file format: {file_format}")

        except Exception as e:
            raise ValueError(f"Failed to parse {file_format} file: {str(e)}")
    
    
    @staticmethod
    def _convert_to_parquet(df: pl.DataFrame) -> bytes:
        """Convert DataFrame to optimized parquet format"""
        
        buffer = BytesIO()
        
        # Write with optimized settings for visualization use cases
        df.write_parquet(
            buffer,
            compression="snappy",  # Good balance of speed vs size
            use_pyarrow=False,     # Use Polars native (faster)
            statistics=True,       # Enable column statistics
            row_group_size=50000   # Optimize for query performance
        )
        
        parquet_data = buffer.getvalue()
        logger.info(f"📦 Generated parquet: {len(parquet_data)/1024/1024:.2f}MB")
        
        return parquet_data
    
    @staticmethod
    async def _upload_parquet(output_url: str, parquet_data: bytes) -> None:
        """Upload parquet data to R2 using signed URL"""
        
        async with httpx.AsyncClient(timeout=300) as client:  # 5 min timeout for upload
            response = await client.put(
                output_url,
                content=parquet_data,
                headers={"Content-Type": "application/x-parquet"}
            )
            response.raise_for_status()
            
        logger.info(f"📤 Uploaded parquet to R2: {len(parquet_data)/1024/1024:.2f}MB")
    
    @staticmethod
    def _generate_schema(df: pl.DataFrame) -> Dict[str, Any]:
        """Generate schema information for the dataset"""
        
        fields = []
        for col, dtype in zip(df.columns, df.dtypes):
            # Skip internal/hidden fields from schema
            if col.startswith('_'):
                continue
                
            field_info = {
                "name": col,
                "type": str(dtype),
                "polars_type": str(dtype)
            }
            
            # Add basic stats for numeric columns
            if dtype.is_numeric():
                try:
                    stats = df.select(pl.col(col)).describe()
                    field_info["nullable"] = df.select(pl.col(col).is_null().any()).item()
                except:
                    pass  # Skip stats if calculation fails
            
            fields.append(field_info)
        
        return {
            "fields": fields,
            "format": "parquet",
            "encoding": "utf-8"
        }