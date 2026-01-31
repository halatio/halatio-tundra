# Halatio Tundra - Data Conversion Service

**Version 2.0.0** - High-performance data conversion service using Python + Polars to convert various data formats to optimized Parquet files. Designed for enterprise-scale datasets with advanced schema inference and transformation capabilities.

## Overview

Halatio Tundra is a specialized conversion service that transforms data from multiple sources into optimized Parquet format for efficient storage and querying. The service handles files (CSV, TSV, Excel, JSON, Parquet), and SQL queries, processing them through Polars for maximum performance.

## Features

### v2.0 Enhancements
- **Excel Support**: Native `.xlsx` and `.xls` file conversion with sheet selection
- **Schema Inference**: Advanced schema detection with format recognition (email, URL, phone, UUID)
- **Column Transformations**: Rename columns, override types, skip problematic rows
- **SQL Connection Testing**: Pre-validate database connections before saving credentials
- **Rate Limiting**: Prevent API abuse with configurable request limits
- **Parquet Pass-through**: Apply transformations to existing Parquet files

### Core Features
- **Multi-format conversion**: CSV, TSV, Excel, JSON, Parquet, and SQL queries
- **Polars-powered**: Lightning-fast data processing and transformation
- **Parquet optimization**: Compressed, columnar storage with statistics
- **Production-ready**: Built for enterprise scale with proper limits and error handling
- **Cloud-native**: Designed for Cloud Run with automatic scaling

## Architecture

```
halatio-tundra/
├── app/
│   ├── main.py                          # FastAPI app + routing + rate limiting
│   ├── config.py                        # Environment configuration
│   ├── models/
│   │   └── conversionRequest.py         # Pydantic request/response models
│   └── services/                        # Core conversion logic
│       ├── file_converter.py            # R2 file → Parquet conversion
│       ├── sql_converter.py             # SQL results → Parquet conversion
│       ├── schema_inference.py          # Schema detection and analysis
│       └── sql_connection_test.py       # Database connection validation
├── requirements.txt
└── README.md
```

## API Endpoints

### Health & Info

#### `GET /`
Root endpoint - service status check

**Response:**
```json
{
  "status": "running",
  "service": "halatio-tundra",
  "version": "2.0.0"
}
```

#### `GET /health`
Health check endpoint

**Response:**
```json
{
  "status": "healthy",
  "service": "halatio-tundra",
  "version": "2.0.0"
}
```

#### `GET /info`
Service capabilities and limits

**Response:**
```json
{
  "service": "halatio-tundra",
  "version": "2.0.0",
  "capabilities": {
    "file_formats": ["csv", "tsv", "excel", "json", "parquet"],
    "output_format": "parquet",
    "max_file_size_mb": 500,
    "supported_sources": ["file", "sql"]
  },
  "limits": {
    "max_processing_time_minutes": 10,
    "max_memory_usage_gb": 2,
    "max_rows_processed": 10000000
  },
  "features": {
    "polars_native": true,
    "streaming_processing": true,
    "automatic_schema_inference": true,
    "optimized_parquet_output": true,
    "excel_support": true,
    "column_transformations": true,
    "schema_inference": true,
    "sql_connection_testing": true
  }
}
```

---

### File Conversion

#### `POST /convert/file`
Convert files from R2 storage to Parquet format

**Rate Limit:** 10 requests/minute

**Request Body:**
```json
{
  "source_url": "https://r2-signed-url...",
  "output_url": "https://r2-signed-url...",
  "format": "csv|tsv|excel|json|parquet",
  "options": {
    "column_mapping": {
      "Email Address": "email",
      "Created Date": "created_at"
    },
    "type_overrides": {
      "amount": "Float64",
      "created_at": "Date"
    },
    "skip_rows": [45, 89, 120],
    "encoding": "utf-8",
    "delimiter": ",",
    "sheet_name": "Sheet1",
    "sheet_index": 0
  },
  "webhook_url": "https://api.example.com/webhook"
}
```

**Request Fields:**
- `source_url` (required): Signed R2 GET URL for source file
- `output_url` (required): Signed R2 PUT URL for output Parquet
- `format` (required): File format - `csv`, `tsv`, `excel`, `json`, or `parquet`
- `options` (optional): Transformation options
  - `column_mapping`: Rename columns (e.g., `"Old Name": "new_name"`)
  - `type_overrides`: Force Polars types (`Utf8`, `Int64`, `Float64`, `Date`, `Datetime`, `Boolean`)
  - `skip_rows`: Array of row indices to skip (0-indexed)
  - `encoding`: Force encoding (default: auto-detect)
  - `delimiter`: Force delimiter for CSV/TSV (default: auto-detect)
  - `sheet_name`: Excel sheet name to convert (required for multi-sheet files)
  - `sheet_index`: Excel sheet index (0-based, default: 0)
- `webhook_url` (optional): URL for progress updates

**Response (200 OK):**
```json
{
  "success": true,
  "metadata": {
    "rows": 1234,
    "columns": 8,
    "column_schema": {
      "fields": [
        {
          "name": "email",
          "type": "Utf8",
          "polars_type": "Utf8",
          "nullable": true
        }
      ],
      "format": "parquet",
      "encoding": "utf-8"
    },
    "file_size_mb": 2.5,
    "processing_time_seconds": 1.25,
    "source_type": "file",
    "rows_skipped": 3,
    "warnings": [
      "Could not cast column 'amount' to Float64: invalid value"
    ]
  }
}
```

**Response (500 Error):**
```json
{
  "success": false,
  "error": "Failed to parse excel file: Sheet 'InvalidSheet' not found"
}
```

---

### Schema Inference

#### `POST /infer/schema`
Infer schema from a file for backend verification (optional - mainly for Parquet or large Excel files)

**Rate Limit:** 20 requests/minute

**Request Body:**
```json
{
  "source_url": "https://r2-signed-url...",
  "format": "csv|tsv|excel|json|parquet",
  "sample_size": 1000
}
```

**Request Fields:**
- `source_url` (required): Signed R2 GET URL for source file
- `format` (required): File format
- `sample_size` (optional): Number of rows to analyze (default: 1000, max: 10000)

**Response (200 OK):**
```json
{
  "success": true,
  "schema": {
    "columns": [
      {
        "name": "email",
        "inferred_type": "Utf8",
        "detected_format": "email",
        "nullable": true,
        "null_count": 15,
        "unique_count": 985,
        "sample_values": ["john@example.com", "jane@test.com"],
        "min_value": null,
        "max_value": null
      },
      {
        "name": "created_at",
        "inferred_type": "Date",
        "detected_format": null,
        "nullable": false,
        "null_count": 0,
        "unique_count": 500,
        "sample_values": ["2024-01-15", "2024-02-20"],
        "min_value": "2024-01-01",
        "max_value": "2024-12-20"
      },
      {
        "name": "amount",
        "inferred_type": "Float64",
        "detected_format": null,
        "nullable": false,
        "null_count": 0,
        "unique_count": 800,
        "sample_values": [1234.56, 500.0, 2500.5],
        "min_value": 0.0,
        "max_value": 9999.99
      }
    ],
    "total_rows": 1234,
    "total_columns": 8,
    "file_size_bytes": 2457600
  },
  "sample_data": [
    {
      "email": "john@example.com",
      "created_at": "2024-01-15",
      "amount": 1234.56
    }
  ],
  "warnings": [
    {
      "column": "amount",
      "issue": "type_inconsistency",
      "message": "Column looks numeric but contains non-numeric values",
      "affected_rows": [45, 89, 120]
    }
  ]
}
```

**Detected Formats:**
- `email` - Email addresses (80%+ match `user@domain.com` pattern)
- `url` - URLs (80%+ start with `http://` or `https://`)
- `phone` - Phone numbers (80%+ match phone patterns)
- `uuid` - UUIDs (80%+ match UUID format)

---

### SQL Conversion

#### `POST /convert/sql`
Execute SQL query and convert results to Parquet

**Rate Limit:** 10 requests/minute

**Request Body:**
```json
{
  "output_url": "https://r2-signed-url...",
  "sql_endpoint": "https://sql-api.example.com/query",
  "sql_database": "production",
  "sql_query": "SELECT * FROM customers WHERE created_at >= '2024-01-01'",
  "credentials_id": "datasource-uuid-123",
  "options": {
    "database_type": "postgresql",
    "port": 5432,
    "ssl_mode": "require",
    "query_timeout_seconds": 300,
    "max_rows": 100000
  },
  "webhook_url": "https://api.example.com/webhook"
}
```

**Request Fields:**
- `output_url` (required): Signed R2 PUT URL for output Parquet
- `sql_endpoint` (required): SQL API endpoint URL
- `sql_database` (required): Database name
- `sql_query` (required): SQL query to execute (SELECT only)
- `credentials_id` (optional): Credential ID from vault
- `options` (optional): SQL connection options
  - `database_type`: `postgresql` or `mysql`
  - `port`: Database port
  - `ssl_mode`: `require`, `prefer`, or `disable`
  - `query_timeout_seconds`: Query timeout (default: 300)
  - `max_rows`: Maximum rows to fetch (default: 100000)
- `webhook_url` (optional): URL for progress updates

**Response (200 OK):**
```json
{
  "success": true,
  "metadata": {
    "rows": 5432,
    "columns": 12,
    "column_schema": {
      "fields": [
        {
          "name": "customer_id",
          "type": "Int64",
          "polars_type": "Int64"
        }
      ],
      "format": "parquet",
      "encoding": "utf-8",
      "source": "sql"
    },
    "file_size_mb": 1.2,
    "processing_time_seconds": 4.5,
    "source_type": "sql",
    "query_execution_time_ms": 3200,
    "connection_info": {
      "database_type": "postgresql",
      "rows_fetched": 5432
    }
  }
}
```

---

### SQL Connection Testing

#### `POST /test/sql-connection`
Test SQL database connection before saving credentials

**Rate Limit:** 20 requests/minute

**Request Body:**
```json
{
  "sql_endpoint": "https://sql-api.example.com/query",
  "sql_database": "production",
  "database_type": "postgresql",
  "port": 5432,
  "ssl_mode": "require",
  "credentials": {
    "type": "basic",
    "username": "readonly_user",
    "password": "temp_password"
  }
}
```

**Request Fields:**
- `sql_endpoint` (required): SQL API endpoint URL
- `sql_database` (required): Database name
- `database_type` (optional): `postgresql` or `mysql`
- `port` (optional): Database port
- `ssl_mode` (optional): `require`, `prefer`, or `disable`
- `credentials` (required): Temporary credentials (not stored)
  - `type`: `basic`, `bearer`, or `api-key`
  - `username`: For basic auth
  - `password`: For basic auth
  - `token`: For bearer/api-key auth

**Response (200 OK - Success):**
```json
{
  "success": true,
  "message": "Connection successful",
  "metadata": {
    "database_type": "PostgreSQL",
    "database_version": "14.2",
    "server_timezone": "UTC",
    "estimated_latency_ms": 120
  }
}
```

**Response (400 Bad Request - Failed):**
```json
{
  "success": false,
  "error": "connection_refused",
  "message": "Couldn't reach the database server. Check that the server is running and the host/port are correct.",
  "details": {
    "technical_error": "Connection refused (ECONNREFUSED)",
    "endpoint_tested": "https://sql-api.example.com:5432",
    "ssl_attempted": true
  },
  "suggestions": [
    "Verify the hostname and port are correct",
    "Check your database server is running",
    "Ensure your firewall allows connections from Halatio IPs"
  ]
}
```

**Error Codes:**
- `connection_refused` - Server unreachable
- `authentication_failed` - Invalid credentials
- `connection_timeout` - Server took too long to respond
- `ssl_required` - Database requires SSL but it wasn't enabled
- `unknown_host` - DNS resolution failed
- `invalid_database` - Database name doesn't exist

---

## Supported File Formats

### CSV/TSV
- Auto-detection of encoding and delimiter
- Configurable null value handling
- Type inference from all rows
- Row-level error skipping

### Excel (.xlsx, .xls)
- Powered by Polars `calamine` engine (Rust-based, fast)
- Multi-sheet support with selection
- Formula handling (calculated values)
- Date parsing and type inference

### JSON
- Direct parsing with Polars
- Nested object flattening
- Array handling

### Parquet
- Pass-through with optional transformations
- Column mapping and type overrides
- Schema validation

---

## Data Transformations

### Column Mapping
Rename columns during conversion:
```json
{
  "column_mapping": {
    "Email Address": "email",
    "First Name": "first_name",
    "Created Date": "created_at"
  }
}
```

### Type Overrides
Force specific Polars data types:
```json
{
  "type_overrides": {
    "amount": "Float64",
    "created_at": "Date",
    "is_active": "Boolean",
    "user_id": "Int64"
  }
}
```

**Supported Polars Types:**
- `Utf8` - String
- `Int32`, `Int64` - Integers
- `Float32`, `Float64` - Floating point
- `Boolean` - True/False
- `Date` - Date (no time)
- `Datetime` - Date with time

### Row Skipping
Skip problematic rows by index:
```json
{
  "skip_rows": [0, 45, 89, 120]
}
```

---

## Processing Limits

| Resource | Limit | Note |
|----------|-------|------|
| File size | 500 MB | Cloud Run request limit |
| API response | 100 MB | Safety limit |
| SQL response | 100 MB | Configurable via `max_rows` |
| Processing time | 10 minutes | Cloud Run timeout |
| Memory usage | 2 GB | Cloud Run allocation |
| Concurrent conversions | 5 | Service-wide limit |
| Rate limit (conversions) | 10/minute | Per client IP |
| Rate limit (testing) | 20/minute | Per client IP |

---

## Configuration

### Environment Variables

```bash
# Required
ENV=production|staging|dev

# Optional (with defaults)
MAX_FILE_SIZE_MB=500
MAX_API_RESPONSE_MB=100
MAX_SQL_RESPONSE_MB=100
MAX_PROCESSING_TIME_MINUTES=10
MAX_MEMORY_USAGE_GB=2
MAX_CONCURRENT_CONVERSIONS=5
LOG_LEVEL=INFO

# SQL Defaults
DEFAULT_SQL_LIMIT=100000
MAX_SQL_LIMIT=1000000

# Parquet Optimization
PARQUET_COMPRESSION=snappy
PARQUET_ROW_GROUP_SIZE=50000
```

### CORS Configuration

CORS origins are automatically configured based on environment:
- **Production**: `halatio.com`, `www.halatio.com`, `halatio.vercel.app`
- **Non-production**: Above + `http://localhost:3000`

---

## Output Format

All conversions produce optimized Parquet files with:
- **Compression**: Snappy (balance of speed and size)
- **Row groups**: 50,000 rows for optimal query performance
- **Statistics**: Column-level metadata for query optimization
- **Schema**: Detailed field information and type mapping

---

## Development

### Local Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variable
export ENV=dev

# Run the service
uvicorn app.main:app --reload --host 0.0.0.0 --port 8080
```

### Testing Endpoints
```bash
# Health check
curl http://localhost:8080/health

# Service info
curl http://localhost:8080/info

# File conversion (requires R2 signed URLs)
curl -X POST http://localhost:8080/convert/file \
  -H "Content-Type: application/json" \
  -d '{
    "source_url": "https://...",
    "output_url": "https://...",
    "format": "csv"
  }'

# Schema inference
curl -X POST http://localhost:8080/infer/schema \
  -H "Content-Type: application/json" \
  -d '{
    "source_url": "https://...",
    "format": "csv",
    "sample_size": 1000
  }'

# SQL connection test
curl -X POST http://localhost:8080/test/sql-connection \
  -H "Content-Type: application/json" \
  -d '{
    "sql_endpoint": "https://...",
    "sql_database": "test",
    "credentials": {
      "type": "basic",
      "username": "user",
      "password": "pass"
    }
  }'
```

---

## Deployment

The service uses automated CI/CD with GitHub Actions and Cloud Run. Every push to `main` automatically creates a new Cloud Run revision.

### Cloud Run Configuration
- **CPU**: 1 vCPU (boosted during request)
- **Memory**: 2 GB
- **Timeout**: 600 seconds (10 minutes)
- **Concurrency**: 5 requests per instance
- **Autoscaling**: 0-10 instances

---

## Error Handling

### Error Response Format
```json
{
  "success": false,
  "error": "error_code",
  "message": "User-friendly error message",
  "details": {
    "technical_error": "Detailed error information",
    "file_format": "csv",
    "file_size_mb": 150.5
  },
  "timestamp": "2024-12-20T10:30:00Z",
  "request_id": "uuid",
  "recoverable": true,
  "suggestions": [
    "Try reducing file size",
    "Check file format"
  ]
}
```

### Common Error Scenarios

**File Too Large**
```json
{
  "detail": "Conversion failed: File size exceeds 500MB limit during download"
}
```

**Invalid File Format**
```json
{
  "detail": "Conversion failed: Failed to parse excel file: Sheet 'InvalidSheet' not found"
}
```

**Rate Limit Exceeded**
```json
{
  "error": "Rate limit exceeded",
  "detail": "Too many requests. Please try again later."
}
```

---

## Version History

### v2.0.0 (Current)
- ✨ Excel file support (.xlsx, .xls) with sheet selection
- ✨ Parquet file pass-through with transformations
- ✨ Schema inference endpoint for backend verification
- ✨ SQL connection testing endpoint
- ✨ Column mapping and type overrides
- ✨ Row skipping for data quality
- ✨ Rate limiting on all endpoints
- ✨ Enhanced error handling with suggestions
- 🔧 Removed API converter (not needed)
- 🔧 Removed GeoJSON support (moved to frontend)

### v1.0.0
- Initial release with CSV, TSV, JSON, GeoJSON support
- Basic file and SQL conversion
- Polars-based processing

---

## License

Proprietary - Halatio Analytics Platform

## Support

For issues or questions, contact the Halatio development team.
