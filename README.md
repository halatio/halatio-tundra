# Halatio Tundra

**Version 3.0.0** - Data conversion service for files and databases to Parquet format.

## Overview

Halatio Tundra converts data from various sources (files, databases) into optimized Parquet format. It provides fast, parallel database extraction using ConnectorX and supports multiple file formats with schema inference.

## Quick Start

All endpoints accept/return JSON and require signed URLs for storage operations.

**Base URL:** `https://your-service.run.app`

## Authentication & Credentials

### Database Credentials
Database credentials are stored in Google Secret Manager for security:

1. **Test connection** with temporary credentials using `/test/database-connection`
2. **Store credentials** in Secret Manager (external to this service)
3. **Reference by ID** using `credentials_id` in conversion requests
4. Credentials are cached for 1 hour

### Signed URLs
All `output_url` parameters must be signed PUT URLs from one of:
- `r2.cloudflarestorage.com` (Cloudflare R2)
- `s3.amazonaws.com` (AWS S3)
- `storage.googleapis.com` (Google Cloud Storage)

## API Endpoints

### Health & Info

#### `GET /health`
Basic health check for load balancers.

**Response:**
```json
{
  "status": "healthy",
  "service": "halatio-tundra",
  "version": "3.0.0"
}
```

---

#### `GET /health/deep`
Health check with Secret Manager connectivity verification.

**Response:**
```json
{
  "status": "healthy",
  "service": "halatio-tundra",
  "version": "3.0.0",
  "checks": {
    "secret_manager": "healthy"
  }
}
```

---

#### `GET /info`
Get service capabilities and limits.

**Response:**
```json
{
  "service": "halatio-tundra",
  "version": "3.0.0",
  "capabilities": {
    "file_formats": ["csv", "tsv", "excel", "json", "parquet"],
    "output_format": "parquet",
    "max_file_size_mb": 500,
    "supported_sources": ["file", "sql", "database"],
    "database_connectors": ["postgresql", "mysql"]
  },
  "limits": {
    "max_processing_time_minutes": 10,
    "max_memory_usage_gb": 2,
    "max_rows_processed": 10000000
  }
}
```

---

#### `GET /connectors`
List available database connector types.

**Response:**
```json
{
  "connectors": ["postgresql", "mysql"],
  "count": 2
}
```

**Note:** BigQuery, Snowflake, Google Sheets, and Stripe connectors are planned but not yet implemented.

---

### Database Operations

#### `POST /test/database-connection`
Test database connection before saving credentials.

**Rate Limit:** 20 requests/minute

**Request:**
```json
{
  "connector_type": "postgresql",
  "credentials": {
    "host": "db.example.com",
    "port": 5432,
    "database": "production",
    "username": "readonly",
    "password": "temp_password",
    "ssl_mode": "prefer"
  }
}
```

**Parameters:**
- `connector_type` (required): `"postgresql"` or `"mysql"`
- `credentials` (required):
  - `host` (required): Database hostname
  - `port` (optional): Database port (defaults: PostgreSQL=5432, MySQL=3306)
  - `database` (required): Database name
  - `username` (required): Database username
  - `password` (required): Database password
  - `ssl_mode` (optional): `"require"`, `"prefer"`, or `"disable"` (default: `"prefer"`)

**Success Response (200):**
```json
{
  "success": true,
  "message": "Connection successful",
  "metadata": {
    "database_type": "PostgreSQL",
    "rows_returned": 1
  }
}
```

**Error Response (400):**
```json
{
  "success": false,
  "error": "connection_failed",
  "message": "connection refused: check host and port"
}
```

---

#### `POST /convert/database`
Extract database table or query results to Parquet.

**Rate Limit:** 10 requests/minute

**Request:**
```json
{
  "output_url": "https://account.r2.cloudflarestorage.com/bucket/output.parquet?X-Amz-Signature=...",
  "connector_type": "postgresql",
  "credentials_id": "postgres_prod_readonly",
  "query": "SELECT * FROM customers WHERE created_at > '2024-01-01'",
  "partition_column": "id",
  "partition_num": 8,
  "compression": "snappy"
}
```

**Parameters:**
- `output_url` (required): Signed PUT URL for Parquet output
- `connector_type` (required): `"postgresql"` or `"mysql"`
- `credentials_id` (required): Secret Manager secret ID containing database credentials
- `query` (optional): SQL query to execute
- `table_name` (optional): Table name to extract (alternative to `query`, validated for SQL injection)
- `partition_column` (optional): Column name for parallel extraction (improves performance for large tables)
- `partition_num` (optional): Number of parallel partitions (1-16, default: 4)
- `compression` (optional): `"snappy"` (default, fastest), `"zstd"` (better compression), or `"none"`

**Note:** Must specify either `query` OR `table_name`, not both.

**Success Response (200):**
```json
{
  "success": true,
  "metadata": {
    "rows": 1234567,
    "columns": 15,
    "file_size_mb": 45.2,
    "processing_time_seconds": 12.5,
    "source_type": "database",
    "connection_info": {
      "connector_type": "postgresql",
      "query": "SELECT * FROM customers WHERE...",
      "partitioned": true,
      "compression": "snappy"
    }
  }
}
```

**Error Responses:**
- `400 Bad Request`: Invalid table name, missing required fields, or both query and table_name specified
- `403 Forbidden`: Secret Manager permission denied
- `404 Not Found`: Secret or table not found
- `502 Bad Gateway`: Database unreachable, connection refused, or query timeout
- `500 Internal Server Error`: Unexpected server error

---

### File Operations

#### `POST /convert/file`
Convert file from various formats to Parquet.

**Rate Limit:** 10 requests/minute

**Request:**
```json
{
  "source_url": "https://account.r2.cloudflarestorage.com/bucket/source.csv?X-Amz-Signature=...",
  "output_url": "https://account.r2.cloudflarestorage.com/bucket/output.parquet?X-Amz-Signature=...",
  "format": "csv",
  "options": {
    "column_mapping": {
      "old_name": "new_name"
    },
    "type_overrides": {
      "column_name": "Int64"
    },
    "skip_rows": [0, 1],
    "encoding": "utf-8",
    "delimiter": ",",
    "sheet_name": "Sheet1",
    "sheet_index": 0
  }
}
```

**Parameters:**
- `source_url` (required): Signed GET URL for source file
- `output_url` (required): Signed PUT URL for Parquet output
- `format` (required): `"csv"`, `"tsv"`, `"excel"`, `"json"`, or `"parquet"`
- `options` (optional):
  - `column_mapping` (optional): Rename columns `{"old_name": "new_name"}`
  - `type_overrides` (optional): Override column types `{"column": "Int64"}` (Polars types)
  - `skip_rows` (optional): Row indices to skip `[0, 1, 5]`
  - `encoding` (optional): Force encoding (default: auto-detect)
  - `delimiter` (optional): Force delimiter for CSV (default: auto-detect)
  - `sheet_name` (optional): Excel sheet name to convert
  - `sheet_index` (optional): Excel sheet index (0-based)

**Success Response (200):**
```json
{
  "success": true,
  "metadata": {
    "rows": 50000,
    "columns": 12,
    "file_size_mb": 8.5,
    "processing_time_seconds": 3.2,
    "source_type": "file",
    "rows_skipped": 2,
    "warnings": ["Column 'price' had 5 null values"]
  }
}
```

**Error Responses:**
- `400 Bad Request`: Invalid format, missing required fields, or malformed file
- `502 Bad Gateway`: Source file unreachable or storage upload failed
- `500 Internal Server Error`: Conversion failed or unexpected error

---

#### `POST /infer/schema`
Infer schema from file without converting (useful for validation).

**Rate Limit:** 20 requests/minute

**Request:**
```json
{
  "source_url": "https://account.r2.cloudflarestorage.com/bucket/data.csv?X-Amz-Signature=...",
  "format": "csv",
  "sample_size": 1000
}
```

**Parameters:**
- `source_url` (required): Signed GET URL for source file
- `format` (required): `"csv"`, `"tsv"`, `"excel"`, `"json"`, or `"parquet"`
- `sample_size` (optional): Number of rows to analyze (1-10000, default: 1000)

**Success Response (200):**
```json
{
  "success": true,
  "schema": {
    "columns": [
      {
        "name": "customer_id",
        "inferred_type": "Int64",
        "nullable": false,
        "null_count": 0,
        "unique_count": 1000,
        "sample_values": [1, 2, 3, 4, 5],
        "min_value": 1,
        "max_value": 1000
      },
      {
        "name": "email",
        "inferred_type": "Utf8",
        "detected_format": "email",
        "nullable": true,
        "null_count": 5,
        "unique_count": 995,
        "sample_values": ["user@example.com", "test@test.com"]
      }
    ],
    "total_rows": 1000,
    "total_columns": 2,
    "file_size_bytes": 45000
  },
  "sample_data": [
    {"customer_id": 1, "email": "user@example.com"},
    {"customer_id": 2, "email": "test@test.com"}
  ],
  "warnings": [
    {
      "column": "email",
      "issue": "null_values",
      "message": "Column has 5 null values",
      "affected_rows": [10, 25, 30, 45, 67]
    }
  ]
}
```

**Error Responses:**
- `400 Bad Request`: Invalid format or sample_size
- `502 Bad Gateway`: Source file unreachable
- `500 Internal Server Error`: Schema inference failed

---

### Legacy Endpoints

#### `POST /convert/sql`
**Note:** This endpoint exists for backwards compatibility. New integrations should use `/convert/database` instead.

Execute SQL query via SQL API endpoint and convert results to Parquet.

**Rate Limit:** 10 requests/minute

**Request:**
```json
{
  "output_url": "https://account.r2.cloudflarestorage.com/bucket/output.parquet?X-Amz-Signature=...",
  "sql_endpoint": "https://sql-api.example.com/query",
  "sql_database": "production",
  "sql_query": "SELECT * FROM orders",
  "credentials_id": "sql_api_creds",
  "options": {
    "database_type": "postgresql",
    "port": 5432,
    "ssl_mode": "prefer",
    "query_timeout_seconds": 300,
    "max_rows": 100000
  }
}
```

**Parameters:**
- `output_url` (required): Signed PUT URL for Parquet output
- `sql_endpoint` (required): SQL API endpoint URL
- `sql_database` (required): Database name
- `sql_query` (required): SQL query to execute
- `credentials_id` (optional): Credential ID from vault
- `options` (optional):
  - `database_type` (optional): `"postgresql"` or `"mysql"`
  - `port` (optional): Database port
  - `ssl_mode` (optional): `"require"`, `"prefer"`, or `"disable"` (default: `"prefer"`)
  - `query_timeout_seconds` (optional): Query timeout (default: 300)
  - `max_rows` (optional): Maximum rows to fetch (default: 100000)

**Response:** Same format as `/convert/database`

---

#### `POST /test/sql-connection`
**Note:** This endpoint exists for backwards compatibility. New integrations should use `/test/database-connection` instead.

Test SQL API connection.

**Rate Limit:** 20 requests/minute

**Request:**
```json
{
  "sql_endpoint": "https://sql-api.example.com/query",
  "sql_database": "production",
  "database_type": "postgresql",
  "port": 5432,
  "ssl_mode": "prefer",
  "credentials": {
    "type": "basic",
    "username": "user",
    "password": "pass"
  }
}
```

**Response:** Same format as `/test/database-connection`

---

## Error Handling

All endpoints return consistent error responses:

```json
{
  "detail": "Error message describing what went wrong"
}
```

### HTTP Status Codes
- `200 OK`: Request succeeded
- `400 Bad Request`: Invalid input (malformed request, invalid table name, missing required fields)
- `403 Forbidden`: Permission denied (Secret Manager access)
- `404 Not Found`: Resource not found (secret, table, file)
- `429 Too Many Requests`: Rate limit exceeded
- `502 Bad Gateway`: Upstream service error (database unreachable, storage unavailable)
- `500 Internal Server Error`: Unexpected server error

### Common Error Examples

**Invalid table name (SQL injection attempt):**
```json
{
  "detail": "Invalid table name: 'users; DROP TABLE users'. Table names must contain only alphanumeric characters, underscores, and dots."
}
```

**Secret not found:**
```json
{
  "detail": "Secret not found: postgres_prod_readonly"
}
```

**Database unreachable:**
```json
{
  "detail": "Database connection failed: connection refused"
}
```

---

## Performance Tips

### Database Extraction
1. **Use partitioning** for tables with >100K rows to enable parallel extraction
2. **Specify partition_column** with an indexed column for best performance
3. **Choose appropriate partition_num** (1-16): 4-8 partitions work well for most datasets
4. **Use column subsets** in queries instead of `SELECT *` to reduce transfer size
5. **Use snappy compression** for balanced performance (default)

### Compression Options
| Compression | Speed | Ratio | Best For |
|------------|-------|-------|----------|
| `snappy` (default) | Fastest | 2-4x | General use, real-time processing |
| `zstd` | Slower | 4-8x | Archival, cold storage |
| `none` | Fastest write | 1x | Immediate downstream processing |

### Example: Parallel Extraction
```json
{
  "table_name": "large_orders_table",
  "partition_column": "order_id",
  "partition_num": 8,
  "compression": "snappy"
}
```
This splits the table into 8 chunks based on `order_id` ranges and extracts them in parallel, typically 4-8x faster than single-threaded extraction.

---

## Security

### SQL Injection Protection
- Table names are validated against pattern: `^[a-zA-Z0-9_\.]+$`
- Queries are parameterized when using `query` parameter
- Use `table_name` for simple table extraction when possible

### URL Validation
- Only signed URLs from approved domains are accepted
- Prevents abuse where malicious users provide URLs to unauthorized storage

### Credential Management
- Never send credentials in API calls (except for connection testing)
- Store credentials in Google Secret Manager
- Reference credentials by ID in conversion requests
- Credentials are cached for 1 hour to reduce Secret Manager API calls

---

## Limits & Quotas

| Resource | Default Limit | Configurable |
|----------|---------------|--------------|
| Max file size | 500 MB | Yes (env var) |
| Max processing time | 10 minutes | Yes (Cloud Run) |
| Max memory | 2 GB | Yes (Cloud Run) |
| Max rows processed | 10 million | Yes (env var) |
| Max partitions | 16 | Yes (in code) |
| Rate limit: health | None | No |
| Rate limit: conversions | 10/min | Yes (in code) |
| Rate limit: tests | 20/min | Yes (in code) |

---

## Support

For issues or questions, contact the Halatio development team.

**License:** Proprietary - Halatio Analytics Platform
