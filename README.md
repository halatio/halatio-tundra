# Halatio Tundra

**Version 4.0.0** - Data conversion service for files and databases to Parquet format.

## Overview

Halatio Tundra converts data from various sources (files, databases) into optimized Parquet format. Built on **DuckDB**, it provides unified SQL query engine, direct R2/S3 writes, and efficient memory management. Supports multiple database types and file formats with automatic schema inference.

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
  "version": "4.0.0"
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
  "version": "4.0.0",
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
  "version": "4.0.0",
  "capabilities": {
    "file_formats": ["csv", "tsv", "excel", "json", "parquet"],
    "output_format": "parquet",
    "max_file_size_mb": 500,
    "supported_sources": ["file", "database"],
    "database_connectors": ["postgresql", "mysql", "sqlite", "mssql", "mariadb", "redshift"],
    "query_engine": "duckdb",
    "direct_r2_write": true
  },
  "limits": {
    "max_processing_time_minutes": 10,
    "max_memory_usage_gb": 6,
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
  "connectors": ["postgresql", "mysql", "sqlite", "mssql", "mariadb", "redshift"],
  "count": 6
}
```

**Supported Databases (via DuckDB):**
- **PostgreSQL** - Native DuckDB postgres scanner
- **MySQL** - Native DuckDB mysql scanner
- **SQLite** - Native DuckDB support
- **MS SQL Server** (`mssql`) - DuckDB ADBC driver (requires `dbc add mssql`)
- **MariaDB** - Uses MySQL protocol (alias for mysql connector)
- **Redshift** - Uses PostgreSQL protocol (alias for postgresql connector)

**Note:** Snowflake, BigQuery, Databricks, and Trino support is possible via ADBC but not yet implemented.

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
- `connector_type` (required): `"postgresql"`, `"mysql"`, `"sqlite"`, `"mssql"`, `"mariadb"`, or `"redshift"`
- `credentials` (required):
  - `host` (optional): Database hostname (not required for SQLite)
  - `port` (optional): Database port (defaults: PostgreSQL/Redshift=5432, MySQL/MariaDB=3306, MSSQL=1433)
  - `database` (optional): Database name (or file path for SQLite)
  - `username` (optional): Database username (not required for SQLite)
  - `password` (optional): Database password (not required for SQLite)
  - `file_path` (optional): File path for SQLite databases (alternative to using `database` field)

**SQLite Example:**
```json
{
  "connector_type": "sqlite",
  "credentials": {
    "file_path": "/path/to/database.db"
  }
}
```
Or using `database` field:
```json
{
  "connector_type": "sqlite",
  "credentials": {
    "database": "/path/to/database.db"
  }
}
```

**Success Response (200):**
```json
{
  "success": true,
  "message": "Connection successful",
  "metadata": {
    "database_type": "PostgreSQL (DuckDB)",
    "engine": "duckdb-postgres-scanner",
    "test_rows": 1
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
  "compression": "zstd"
}
```

**Parameters:**
- `output_url` (required): Signed PUT URL for Parquet output
- `connector_type` (required): `"postgresql"`, `"mysql"`, `"sqlite"`, `"mssql"`, `"mariadb"`, or `"redshift"`
- `credentials_id` (required): Secret Manager secret ID containing database credentials
- `query` (optional): SQL query to execute
- `table_name` (optional): Table name to extract (alternative to `query`, validated for SQL injection)
- `compression` (optional): `"zstd"` (default, best compression), `"snappy"` (faster), or `"none"`
- `row_group_size` (optional): Parquet row group size (default: 100000)

**Notes:**
- Must specify either `query` OR `table_name`, not both
- DuckDB automatically optimizes query execution and memory usage
- MariaDB uses MySQL protocol internally
- Redshift uses PostgreSQL protocol internally
- For MS SQL Server, ensure ADBC driver is installed (`pip install dbc && dbc add mssql`)

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
    "engine": "duckdb",
    "connection_info": {
      "connector_type": "postgresql",
      "query": "SELECT * FROM customers WHERE...",
      "compression": "zstd"
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

### Database Connector Details

All connectors use **DuckDB** as the unified query engine.

#### PostgreSQL
- **Default Port:** 5432
- **DuckDB Extension:** `postgres` (native scanner)
- **Connection:** `postgresql://user:pass@host:5432/database`
- **Auto-installed:** Yes (first use)

#### MySQL
- **Default Port:** 3306
- **DuckDB Extension:** `mysql` (native scanner)
- **Connection:** `mysql://user:pass@host:3306/database`
- **Auto-installed:** Yes (first use)

#### SQLite
- **Default Port:** N/A (file-based)
- **DuckDB Support:** Native (built-in)
- **Connection:** File path via `database` or `file_path` field
- **Credentials:** Only requires file path (no host/username/password)
- **Note:** Use absolute paths for reliability

#### MS SQL Server
- **Default Port:** 1433
- **DuckDB Driver:** ADBC (`adbc_driver_mssql`)
- **Connection:** Via DuckDB ADBC secret
- **Installation Required:** `pip install dbc && dbc add mssql`
- **Also works for:** Azure SQL Database

#### MariaDB
- **Default Port:** 3306
- **Implementation:** Uses MySQL connector (MySQL protocol)
- **Connection:** `mysql://user:pass@host:3306/database`
- **Note:** Alias for MySQL connector

#### Redshift
- **Default Port:** 5439
- **Implementation:** Uses PostgreSQL connector (PostgreSQL protocol)
- **Connection:** `postgresql://user:pass@host:5439/database`
- **Note:** Alias for PostgreSQL connector

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

### Database Extraction with DuckDB
1. **Let DuckDB optimize** - DuckDB automatically optimizes query execution and memory usage
2. **Use column subsets** in queries instead of `SELECT *` to reduce transfer size
3. **Choose ZSTD compression** for best compression ratio (default)
4. **Adjust memory limit** via `DUCKDB_MEMORY_LIMIT` env var for large datasets
5. **Use WHERE clauses** to filter data at the source for faster extraction

### Compression Options
| Compression | Speed | Ratio | Best For |
|------------|-------|-------|----------|
| `zstd` (default) | Moderate | 4-8x | General use, best compression |
| `snappy` | Fastest | 2-4x | Real-time processing, speed priority |
| `none` | Fastest write | 1x | Immediate downstream processing |

### Memory Configuration
Configure DuckDB memory limits via environment variables:
```bash
DUCKDB_MEMORY_LIMIT=6GB    # Max memory for DuckDB (default: 6GB)
DUCKDB_TEMP_DIR=/tmp/duckdb  # Temp directory for spilling (default: /tmp/duckdb)
DUCKDB_THREADS=2           # Thread count (default: 2)
```

### Example: Optimized Query
```json
{
  "query": "SELECT customer_id, order_date, total FROM orders WHERE order_date > '2024-01-01'",
  "compression": "zstd",
  "row_group_size": 100000
}
```
DuckDB automatically optimizes the query execution plan and manages memory efficiently.

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
| DuckDB memory limit | 6 GB | Yes (`DUCKDB_MEMORY_LIMIT`) |
| Container memory | 8 GB | Yes (Cloud Run) |
| Max rows processed | Unlimited | Limited by memory |
| DuckDB threads | 2 | Yes (`DUCKDB_THREADS`) |
| Rate limit: health | None | No |
| Rate limit: conversions | 10/min | Yes (in code) |
| Rate limit: tests | 20/min | Yes (in code) |

---

## What's New in v4.0.0

**Major Changes:**
- ✨ **DuckDB Engine** - Replaced Polars + ConnectorX with DuckDB for unified query processing
- 🚀 **Direct R2 Write** - Can now write directly to R2/S3 without local temp files (when configured)
- 💾 **Better Memory Management** - Configurable memory limits and automatic spilling to disk
- 🗜️ **ZSTD Default** - Changed default compression from Snappy to ZSTD for better compression
- 📊 **Larger Row Groups** - Increased from 50K to 100K rows for better query performance
- 🔧 **MS SQL Server** - Now uses DuckDB ADBC driver (requires `dbc add mssql`)

**Removed:**
- ❌ **Oracle Support** - Removed (no DuckDB ADBC driver available)
- ❌ **Parallel Partitioning** - Removed manual partitioning (DuckDB handles optimization automatically)
- ❌ **Legacy Connectors** - Removed backward compatibility layer

**Breaking Changes:**
- Oracle connector is no longer available
- `partition_column` and `partition_num` parameters are no longer supported
- Default compression changed from `snappy` to `zstd`
- Default row group size changed from 50,000 to 100,000
- MS SQL Server now requires ADBC driver installation

**Migration Guide:**
See `DUCKDB_MIGRATION.md` for detailed migration instructions and configuration options.

---

## Support

For issues or questions, contact the Halatio development team.

**License:** Proprietary - Halatio Analytics Platform
