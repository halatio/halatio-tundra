from pydantic import BaseModel, Field, HttpUrl
from typing import Any, Dict, List, Optional, Union
from enum import Enum

class FileFormat(str, Enum):
    csv = "csv"
    json = "json"
    tsv = "tsv"
    geojson = "geojson"
    excel = "excel"
    parquet = "parquet"


class HTTPMethod(str, Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"

class DatabaseType(str, Enum):
    postgresql = "postgresql"
    mysql = "mysql"

class SSLMode(str, Enum):
    require = "require"
    prefer = "prefer"
    disable = "disable"

# New models for enhanced file conversion
class FileConversionOptions(BaseModel):
    """Options for file conversion with transformations"""
    column_mapping: Optional[Dict[str, str]] = Field(None, description="Column name mappings (rename columns)")
    type_overrides: Optional[Dict[str, str]] = Field(None, description="Column type overrides (Polars types)")
    skip_rows: Optional[List[int]] = Field(None, description="Row numbers to skip during conversion")
    encoding: Optional[str] = Field(None, description="Force specific encoding (default: auto-detect)")
    delimiter: Optional[str] = Field(None, description="Force specific delimiter for CSV (default: auto-detect)")
    sheet_name: Optional[str] = Field(None, description="Excel sheet name to convert")
    sheet_index: Optional[int] = Field(None, description="Excel sheet index (0-based) to convert")

# New models for enhanced SQL conversion
class SqlConversionOptions(BaseModel):
    """Options for SQL conversion"""
    database_type: Optional[DatabaseType] = Field(None, description="Database type (postgresql, mysql)")
    port: Optional[int] = Field(None, description="Database port")
    ssl_mode: Optional[SSLMode] = Field(SSLMode.prefer, description="SSL connection mode")
    query_timeout_seconds: Optional[int] = Field(300, description="Query execution timeout")
    max_rows: Optional[int] = Field(100000, description="Maximum rows to fetch (safety limit)")

class FileConversionRequest(BaseModel):
    """Request model for file conversions"""
    output_url: HttpUrl = Field(..., description="Signed PUT URL for Parquet output")
    source_url: HttpUrl = Field(..., description="Signed GET URL for source file")
    format: FileFormat = Field(..., description="Source file format")
    options: Optional[FileConversionOptions] = Field(None, description="Conversion options and transformations")
    webhook_url: Optional[HttpUrl] = Field(None, description="Webhook URL for progress updates")

class SqlConversionRequest(BaseModel):
    """Request model for SQL conversions"""
    output_url: HttpUrl = Field(..., description="Signed PUT URL for Parquet output")
    sql_endpoint: HttpUrl = Field(..., description="SQL API endpoint")
    sql_database: str = Field(..., description="Database name")
    sql_query: str = Field(..., description="SQL query to execute")
    credentials_id: Optional[str] = Field(None, description="Credential ID from vault")
    options: Optional[SqlConversionOptions] = Field(None, description="SQL connection options")
    webhook_url: Optional[HttpUrl] = Field(None, description="Webhook URL for progress updates")


class ConversionMetadata(BaseModel):
    """Metadata about the converted dataset"""
    rows: int = Field(..., description="Number of rows in the dataset")
    columns: int = Field(..., description="Number of columns in the dataset")
    column_schema: Dict[str, Any] = Field(..., description="Column schema information")
    file_size_mb: float = Field(..., description="Output parquet file size in MB")
    processing_time_seconds: float = Field(..., description="Time taken to process")
    source_type: str = Field(..., description="Type of source data (file, api, sql)")
    rows_skipped: Optional[int] = Field(default=None, description="Number of rows skipped during conversion")
    warnings: Optional[List[str]] = Field(default=None, description="Warnings during conversion")
    query_execution_time_ms: Optional[float] = Field(default=None, description="SQL query execution time (SQL only)")
    connection_info: Optional[Dict[str, Any]] = Field(default=None, description="Database connection info (SQL only)")

class ConversionResponse(BaseModel):
    """Response from conversion operations"""
    success: bool = Field(..., description="Whether conversion succeeded")
    metadata: ConversionMetadata = Field(..., description="Dataset metadata")
    error: Optional[str] = Field(None, description="Error message if conversion failed")

class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(..., description="Service status")
    service: str = Field(..., description="Service name")
    version: str = Field(..., description="Service version")

class ServiceInfo(BaseModel):
    """Service capabilities and configuration"""
    service: str
    version: str
    capabilities: Dict[str, Any]
    limits: Dict[str, Any]
    features: Dict[str, Any]

# New models for schema inference endpoint
class SchemaInferRequest(BaseModel):
    """Request for schema inference"""
    source_url: HttpUrl = Field(..., description="Signed GET URL for source file")
    format: FileFormat = Field(..., description="File format")
    sample_size: int = Field(1000, description="Number of rows to analyze", ge=1, le=10000)

class ColumnSchema(BaseModel):
    """Schema information for a single column"""
    name: str = Field(..., description="Column name")
    inferred_type: str = Field(..., description="Inferred Polars data type")
    detected_format: Optional[str] = Field(default=None, description="Detected format (email, url, phone, etc.)")
    nullable: bool = Field(..., description="Whether column contains null values")
    null_count: int = Field(..., description="Number of null values")
    unique_count: Optional[int] = Field(default=None, description="Number of unique values")
    sample_values: List[Any] = Field(..., description="Sample values from column")
    min_value: Optional[Union[float, str]] = Field(default=None, description="Minimum value (numeric/date columns)")
    max_value: Optional[Union[float, str]] = Field(default=None, description="Maximum value (numeric/date columns)")

class SchemaInfo(BaseModel):
    """Overall schema information"""
    columns: List[ColumnSchema] = Field(..., description="Column schemas")
    total_rows: int = Field(..., description="Total rows in sample")
    total_columns: int = Field(..., description="Total columns")
    file_size_bytes: Optional[int] = Field(None, description="Estimated file size")

class ValidationWarning(BaseModel):
    """Warning about data quality issues"""
    column: str = Field(..., description="Column with issue")
    issue: str = Field(..., description="Issue type")
    message: str = Field(..., description="Human-readable message")
    affected_rows: Optional[List[int]] = Field(None, description="Row numbers affected")

class SchemaInferResponse(BaseModel):
    """Response from schema inference"""
    success: bool = Field(..., description="Whether inference succeeded")
    schema_info: SchemaInfo = Field(..., description="Inferred schema", serialization_alias="schema")
    sample_data: List[Dict[str, Any]] = Field(..., description="Sample rows")
    warnings: List[ValidationWarning] = Field(default_factory=list, description="Data quality warnings")

    class Config:
        populate_by_name = True

# New models for SQL connection testing
class SqlCredentials(BaseModel):
    """SQL credentials (temporary, not stored)"""
    type: str = Field(..., description="Credential type (basic, bearer, api-key)")
    username: Optional[str] = Field(None, description="Username for basic auth")
    password: Optional[str] = Field(None, description="Password for basic auth")
    token: Optional[str] = Field(None, description="Token for bearer/api-key auth")

class SqlConnectionTestRequest(BaseModel):
    """Request for SQL connection test"""
    sql_endpoint: HttpUrl = Field(..., description="SQL API endpoint")
    sql_database: str = Field(..., description="Database name")
    database_type: Optional[DatabaseType] = Field(None, description="Database type")
    port: Optional[int] = Field(None, description="Database port")
    ssl_mode: Optional[SSLMode] = Field(SSLMode.prefer, description="SSL mode")
    credentials: SqlCredentials = Field(..., description="Temporary credentials for testing")

class SqlConnectionTestResponse(BaseModel):
    """Response from SQL connection test"""
    success: bool = Field(..., description="Whether connection succeeded")
    message: str = Field(..., description="Success or error message")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Connection metadata if successful")
    error: Optional[str] = Field(None, description="Error code if failed")
    details: Optional[Dict[str, Any]] = Field(None, description="Error details")
    suggestions: Optional[List[str]] = Field(None, description="Suggestions for fixing errors")

# Enhanced error response
class ErrorDetail(BaseModel):
    """Detailed error information"""
    technical_error: Optional[str] = Field(None, description="Technical error message")
    file_format: Optional[str] = Field(None, description="File format involved")
    file_size_mb: Optional[float] = Field(None, description="File size in MB")
    affected_rows: Optional[List[int]] = Field(None, description="Row numbers with errors")

class ErrorResponse(BaseModel):
    """Enhanced error response"""
    success: bool = Field(False, description="Always false for errors")
    error: str = Field(..., description="Error code")
    message: str = Field(..., description="User-friendly error message")
    details: Optional[ErrorDetail] = Field(None, description="Error details")
    timestamp: Optional[str] = Field(None, description="Error timestamp")
    request_id: Optional[str] = Field(None, description="Request ID for tracking")
    recoverable: bool = Field(True, description="Whether error is recoverable")
    suggestions: List[str] = Field(default_factory=list, description="Suggestions for fixing")

# New models for database connectors (Phase 1)
class ConnectorType(str, Enum):
    """Supported connector types"""
    postgresql = "postgresql"
    mysql = "mysql"
    bigquery = "bigquery"
    snowflake = "snowflake"
    google_sheets = "google_sheets"
    stripe = "stripe"

class DatabaseCredentials(BaseModel):
    """Database connection credentials"""
    host: str = Field(..., description="Database host")
    port: Optional[int] = Field(None, description="Database port")
    database: str = Field(..., description="Database name")
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")
    ssl_mode: Optional[str] = Field("prefer", description="SSL mode")

class ConnectionTestRequest(BaseModel):
    """Request to test database connection"""
    connector_type: ConnectorType = Field(..., description="Type of connector")
    credentials: DatabaseCredentials = Field(..., description="Connection credentials")

class ConnectionTestResponse(BaseModel):
    """Response from connection test"""
    success: bool = Field(..., description="Whether test succeeded")
    message: str = Field(..., description="Success or error message")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Connection metadata")
    error: Optional[str] = Field(None, description="Error code if failed")

class DatabaseConversionRequest(BaseModel):
    """Request for database data conversion"""
    output_url: HttpUrl = Field(..., description="Signed PUT URL for Parquet output")
    connector_type: ConnectorType = Field(..., description="Database type")
    credentials_id: str = Field(..., description="Secret Manager credential ID")
    query: Optional[str] = Field(None, description="SQL query to execute")
    table_name: Optional[str] = Field(None, description="Table name to extract (alternative to query)")
    partition_column: Optional[str] = Field(None, description="Column for parallel extraction")
    partition_num: int = Field(4, description="Number of parallel partitions", ge=1, le=16)
    webhook_url: Optional[HttpUrl] = Field(None, description="Webhook for progress updates")