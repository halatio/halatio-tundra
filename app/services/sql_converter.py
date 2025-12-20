import polars as pl
import httpx
import json
import time
import logging
from io import BytesIO
from typing import Dict, Any, Optional
from ..models.conversionRequest import ConversionMetadata

logger = logging.getLogger(__name__)

class SqlConverter:
    """Production SQL data converter - SQL API to Parquet"""
    
    # Processing limits
    MAX_RESPONSE_SIZE = 100 * 1024 * 1024  # 100MB max SQL response
    TIMEOUT_SECONDS = 600  # 10 minutes max for SQL execution
    DEFAULT_QUERY_LIMIT = 100000  # Default row limit for safety
    
    @staticmethod
    async def convert(
        endpoint: str,
        database: str,
        query: str,
        output_url: str,
        credentials_id: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute SQL query and convert results to parquet"""

        start_time = time.time()
        query_start_time = None
        query_execution_time_ms = None
        options = options or {}

        try:
            logger.info(f"💾 Starting SQL conversion: {database}")

            # Extract options
            max_rows = options.get('max_rows', SqlConverter.DEFAULT_QUERY_LIMIT)
            query_timeout = options.get('query_timeout_seconds', SqlConverter.TIMEOUT_SECONDS)

            # 1. Add safety limit to query if not present
            safe_query = SqlConverter._add_safety_limit(query, max_rows)

            # 2. Execute SQL query
            query_start_time = time.time()
            sql_results = await SqlConverter._execute_sql_query(
                endpoint, database, safe_query, credentials_id, options, query_timeout
            )
            query_execution_time_ms = (time.time() - query_start_time) * 1000

            # 3. Convert results to DataFrame
            df = SqlConverter._create_dataframe(sql_results)

            # 4. Convert to parquet
            parquet_buffer = SqlConverter._convert_to_parquet(df)

            # 5. Upload to R2
            await SqlConverter._upload_parquet(output_url, parquet_buffer)

            # 6. Generate metadata
            processing_time = time.time() - start_time
            file_size_mb = len(parquet_buffer) / 1024 / 1024

            metadata = ConversionMetadata(
                rows=len(df),
                columns=len(df.columns),
                column_schema=SqlConverter._generate_schema(df),
                file_size_mb=round(file_size_mb, 2),
                processing_time_seconds=round(processing_time, 2),
                source_type="sql",
                query_execution_time_ms=round(query_execution_time_ms, 2) if query_execution_time_ms else None,
                connection_info={
                    "database_type": options.get('database_type', 'Unknown'),
                    "rows_fetched": len(df)
                }
            )

            logger.info(f"✅ SQL conversion successful: {len(df)} rows, {file_size_mb:.2f}MB")

            return {
                "success": True,
                "metadata": metadata.dict()
            }

        except Exception as e:
            logger.error(f"❌ SQL conversion failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
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

        # Build request headers
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        # Add authentication if credentials provided
        if credentials_id:
            auth_headers = await SqlConverter._get_auth_headers(credentials_id)
            headers.update(auth_headers)

        # Prepare request body
        request_body = {
            "query": query,
            "database": database
        }

        # Add optional parameters from options
        if options.get('port'):
            request_body["port"] = options['port']
        if options.get('ssl_mode'):
            request_body["ssl_mode"] = options['ssl_mode']

        # Execute SQL query
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(endpoint, headers=headers, json=request_body)
            response.raise_for_status()
            
            # Check response size
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > SqlConverter.MAX_RESPONSE_SIZE:
                raise ValueError(f"SQL response too large: {int(content_length)/1024/1024:.1f}MB exceeds 100MB limit")
            
            data = response.json()
            
            # Additional size check
            data_size = len(json.dumps(data).encode('utf-8'))
            if data_size > SqlConverter.MAX_RESPONSE_SIZE:
                raise ValueError(f"SQL response too large after parsing: {data_size/1024/1024:.1f}MB exceeds 100MB limit")
        
        # Extract rows from response
        rows = SqlConverter._extract_rows_from_response(data)
        
        logger.info(f"📥 SQL query returned {len(rows)} rows ({data_size/1024/1024:.2f}MB)")
        return rows
    
    @staticmethod
    async def _get_auth_headers(credentials_id: str) -> Dict[str, str]:
        """Get authentication headers from stored credentials"""
        
        # TODO: Implement credential retrieval from your vault/storage
        # This is a placeholder - you'll need to implement based on your credential storage
        
        logger.warning(f"🔐 SQL credential retrieval not implemented for: {credentials_id}")
        return {}
        
        # Example implementation:
        # credentials = await vault_client.get_credential(credentials_id)
        # 
        # if credentials['type'] == 'bearer':
        #     return {"Authorization": f"Bearer {credentials['value']}"}
        # elif credentials['type'] == 'api-key':
        #     return {"X-API-Key": credentials['value']}
        # elif credentials['type'] == 'basic':
        #     import base64
        #     user_data = json.loads(credentials['value'])
        #     encoded = base64.b64encode(f"{user_data['username']}:{user_data['password']}".encode()).decode()
        #     return {"Authorization": f"Basic {encoded}"}
        # 
        # return {}
    
    @staticmethod
    def _add_safety_limit(query: str, max_rows: int = None) -> str:
        """Add LIMIT clause to query if not present for safety"""

        if max_rows is None:
            max_rows = SqlConverter.DEFAULT_QUERY_LIMIT

        trimmed_query = query.strip().lower()

        # Check if LIMIT already exists
        if 'limit' in trimmed_query:
            return query  # Keep original query

        # Add safety limit
        original_query = query.strip()
        if original_query.endswith(';'):
            return f"{original_query[:-1]} LIMIT {max_rows};"
        else:
            return f"{original_query} LIMIT {max_rows}"
    
    @staticmethod
    def _extract_rows_from_response(data: Any) -> list:
        """Extract rows from various SQL API response formats"""
        
        if isinstance(data, list):
            # Direct array of row objects
            return data
        elif isinstance(data, dict):
            # Try common SQL response formats
            for key in ["rows", "results", "data", "records"]:
                if key in data and isinstance(data[key], list):
                    return data[key]
            
            # If it's a single result object, wrap in array
            if any(isinstance(v, (str, int, float, bool, type(None))) for v in data.values()):
                return [data]
            
            # Empty result
            return []
        else:
            logger.warning(f"⚠️ Unexpected SQL response format: {type(data)}")
            return []
    
    @staticmethod
    def _create_dataframe(data: list) -> pl.DataFrame:
        """Create Polars DataFrame from SQL results"""
        
        if not data:
            # Return empty DataFrame with minimal structure
            return pl.DataFrame({"_empty": []})
        
        try:
            # Use Polars to create DataFrame from list of dictionaries
            df = pl.DataFrame(data)
            logger.info(f"📋 Created DataFrame: {len(df)} rows × {len(df.columns)} columns")
            return df
            
        except Exception as e:
            raise ValueError(f"Failed to create DataFrame from SQL results: {str(e)}")
    
    @staticmethod
    def _convert_to_parquet(df: pl.DataFrame) -> bytes:
        """Convert DataFrame to optimized parquet format"""
        
        buffer = BytesIO()
        
        df.write_parquet(
            buffer,
            compression="snappy",
            use_pyarrow=False,
            statistics=True,
            row_group_size=50000
        )
        
        parquet_data = buffer.getvalue()
        logger.info(f"📦 Generated parquet: {len(parquet_data)/1024/1024:.2f}MB")
        
        return parquet_data
    
    @staticmethod
    async def _upload_parquet(output_url: str, parquet_data: bytes) -> None:
        """Upload parquet data to R2 using signed URL"""
        
        async with httpx.AsyncClient(timeout=300) as client:
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
            # Skip internal fields
            if col.startswith('_') and col != '_empty':
                continue
                
            field_info: Dict[str, Any] = {
                "name": col,
                "type": str(dtype),
                "polars_type": str(dtype)
            }
            
            # Add nullability info
            try:
                field_info["nullable"] = df.select(pl.col(col).is_null().any()).item()
            except:
                field_info["nullable"] = True
            
            fields.append(field_info)
        
        return {
            "fields": fields,
            "format": "parquet",
            "encoding": "utf-8",
            "source": "sql"
        }