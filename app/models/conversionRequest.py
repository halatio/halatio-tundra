from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional, Union
from enum import Enum


class FileFormat(str, Enum):
    csv = "csv"
    json = "json"
    tsv = "tsv"
    geojson = "geojson"
    excel = "excel"
    parquet = "parquet"


class FileConversionOptions(BaseModel):
    """Options for file conversion with transformations"""
    column_mapping: Optional[Dict[str, str]] = Field(None, description="Rename columns (old → new)")
    type_overrides: Optional[Dict[str, str]] = Field(None, description="Cast columns to DuckDB types")
    skip_rows: Optional[List[int]] = Field(None, description="Row indices to skip (0-based)")
    encoding: Optional[str] = Field(None, description="Force file encoding (default: auto-detect)")
    delimiter: Optional[str] = Field(None, description="Force CSV delimiter (default: auto-detect)")
    sheet_name: Optional[str] = Field(None, description="Excel sheet name")
    sheet_index: Optional[int] = Field(None, description="Excel sheet index (0-based)")


class FileConversionRequest(BaseModel):
    """Request model for file-to-Parquet conversion"""
    source_id: str = Field(..., description="Source UUID from Supabase sources table")
    format: Optional[FileFormat] = Field(None, description="Override file format (default: from sources table)")
    options: Optional[FileConversionOptions] = Field(None, description="Conversion options and transformations")


class SchemaInferRequest(BaseModel):
    """Request for schema inference"""
    source_id: str = Field(..., description="Source UUID from Supabase sources table")
    format: Optional[FileFormat] = Field(None, description="Override file format (default: from sources table)")
    sample_size: int = Field(1000, description="Number of rows to analyse", ge=1, le=10000)


class ConversionMetadata(BaseModel):
    """Metadata about the converted dataset"""
    rows: int = Field(..., description="Number of rows in the output dataset")
    columns: int = Field(..., description="Number of columns")
    column_schema: Dict[str, Any] = Field(..., description="Column schema information")
    file_size_mb: float = Field(..., description="Output Parquet file size in MB (0 for R2 output)")
    processing_time_seconds: float = Field(..., description="Wall-clock processing time")
    source_type: str = Field(..., description="Source type (file, database)")
    rows_skipped: Optional[int] = Field(default=None, description="Rows skipped during conversion")
    warnings: Optional[List[str]] = Field(default=None, description="Conversion warnings")
    connection_info: Optional[Dict[str, Any]] = Field(default=None, description="Database connector info")


class ConversionResponse(BaseModel):
    """Response from conversion operations"""
    success: bool = Field(..., description="Whether conversion succeeded")
    metadata: ConversionMetadata = Field(..., description="Dataset metadata")
    error: Optional[str] = Field(None, description="Error message if failed")


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    service: str
    version: str


class ColumnSchema(BaseModel):
    """Schema information for a single column"""
    name: str
    inferred_type: str = Field(..., description="Inferred DuckDB data type")
    detected_format: Optional[str] = Field(default=None, description="Detected string format (email, url, …)")
    nullable: bool
    null_count: int
    unique_count: Optional[int] = None
    sample_values: List[Any]
    min_value: Optional[Union[float, str]] = None
    max_value: Optional[Union[float, str]] = None


class SchemaInfo(BaseModel):
    """Overall schema information"""
    columns: List[ColumnSchema]
    total_rows: int
    total_columns: int
    file_size_bytes: Optional[int] = None


class ValidationWarning(BaseModel):
    """Warning about data quality issues"""
    column: str
    issue: str
    message: str
    affected_rows: Optional[List[int]] = None


class SchemaInferResponse(BaseModel):
    """Response from schema inference"""
    success: bool
    schema_info: SchemaInfo = Field(..., serialization_alias="schema")
    sample_data: List[Dict[str, Any]]
    warnings: List[ValidationWarning] = Field(default_factory=list)

    class Config:
        populate_by_name = True


# ---------------------------------------------------------------------------
# Database connector models
# ---------------------------------------------------------------------------

class ConnectorType(str, Enum):
    """Supported database connector types"""
    postgresql = "postgresql"
    mysql = "mysql"
    sqlite = "sqlite"
    mssql = "mssql"
    mariadb = "mariadb"
    redshift = "redshift"


class DatabaseCredentials(BaseModel):
    """Database connection credentials (temporary — for connection testing only)"""
    host: Optional[str] = Field(None, description="Database host (not required for SQLite)")
    port: Optional[int] = Field(None, description="Database port")
    database: Optional[str] = Field(None, description="Database name or SQLite file path")
    username: Optional[str] = Field(None, description="Username")
    password: Optional[str] = Field(None, description="Password")
    ssl_mode: Optional[str] = Field("prefer", description="SSL mode")
    file_path: Optional[str] = Field(None, description="SQLite file path")


class ConnectionTestRequest(BaseModel):
    """Request to test a database connection with temporary credentials"""
    connector_type: ConnectorType
    credentials: DatabaseCredentials


class ConnectionTestResponse(BaseModel):
    """Response from connection test"""
    success: bool
    message: str
    metadata: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class ParquetCompression(str, Enum):
    snappy = "snappy"
    zstd = "zstd"
    none = "none"


class DatabaseConversionRequest(BaseModel):
    """Request for database-to-Parquet conversion"""
    source_id: str = Field(..., description="Source UUID from Supabase sources table")
    credentials_id: str = Field(..., description="Secret Manager credential ID")
    query: Optional[str] = Field(None, description="SQL query to execute")
    table_name: Optional[str] = Field(None, description="Table name to fully extract")
    compression: ParquetCompression = Field(ParquetCompression.zstd, description="Parquet compression")
