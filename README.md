# Halatio Tundra

**Version 3.0.0** - Production-grade data integration platform for converting files and database tables to optimized Parquet format.

## Overview

Halatio Tundra is a high-performance data conversion service designed for Google Cloud Run. It transforms data from multiple sources (files, databases, APIs) into optimized Parquet format using Polars and ConnectorX for maximum throughput.

## Features

### Database Connectors (v3.0)
- **Native database support**: PostgreSQL, MySQL via ConnectorX (10-100x faster than row-by-row)
- **Parallel extraction**: Partition large tables across multiple threads
- **Secure credentials**: Google Secret Manager integration with caching
- **Connection testing**: Validate credentials before saving
- **Compression options**: Snappy, Zstd, or uncompressed output
- **Async I/O**: CPU-bound operations run in thread pools to avoid blocking

### File Processing
- **Multi-format support**: CSV, TSV, Excel, JSON, Parquet
- **Schema inference**: Automatic type detection with format recognition
- **Transformations**: Column mapping, type overrides, row skipping
- **Streaming processing**: Memory-efficient handling of large files

### Production Features
- **Rate limiting**: Configurable per-endpoint limits
- **Error classification**: Proper HTTP status codes (400/403/404/502/500)
- **URL validation**: Signed URL security checks
- **Deep health checks**: Secret Manager connectivity verification
- **Comprehensive logging**: Structured logs for monitoring

## Architecture

```
app/
├── main.py                      # FastAPI routes and error handling
├── config.py                    # Environment configuration
├── utils.py                     # URL validation and error classification
├── models/
│   └── conversionRequest.py     # Pydantic models
└── services/
    ├── file_converter.py        # File to Parquet conversion
    ├── sql_converter.py         # SQL query execution
    ├── schema_inference.py      # Schema detection
    ├── secret_manager.py        # Google Secret Manager client
    └── connectors/              # Database connectors
        ├── base.py              # BaseConnector interface
        ├── postgresql.py        # PostgreSQL connector
        ├── mysql.py             # MySQL connector
        └── factory.py           # Connector factory
```

## API Endpoints

### Health Checks

#### `GET /health`
Basic health check for load balancer

**Response:**
```json
{
  "status": "healthy",
  "service": "halatio-tundra",
  "version": "3.0.0"
}
```

#### `GET /health/deep`
Deep health check including Secret Manager connectivity

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

### Database Operations

#### `POST /test/database-connection`
Test database connection before saving credentials

**Rate Limit:** 20/minute

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

**Response (200 OK):**
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

**Response (400 Bad Request):**
```json
{
  "success": false,
  "error": "connection_failed",
  "message": "connection refused"
}
```

#### `POST /convert/database`
Extract database table/query to Parquet

**Rate Limit:** 10/minute

**Request:**
```json
{
  "output_url": "https://account.r2.cloudflarestorage.com/bucket/file.parquet?X-Amz-...",
  "connector_type": "postgresql",
  "credentials_id": "postgres_prod_db_123",
  "query": "SELECT * FROM customers WHERE created_at > '2024-01-01'",
  "partition_column": "id",
  "partition_num": 8,
  "compression": "snappy"
}
```

**Request Fields:**
- `output_url` (required): Signed PUT URL for R2/S3/GCS storage
- `connector_type` (required): `postgresql` or `mysql`
- `credentials_id` (required): Secret Manager secret ID
- `query` (optional): SQL query to execute
- `table_name` (optional): Table name (alternative to query, validated for SQL injection)
- `partition_column` (optional): Column for parallel extraction
- `partition_num` (optional): Number of partitions (1-16, default: 4)
- `compression` (optional): `snappy`, `zstd`, or `none` (default: snappy)

**Response (200 OK):**
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
      "query": "SELECT * FROM customers...",
      "partitioned": true,
      "compression": "snappy"
    }
  }
}
```

**Error Responses:**
- `400`: Invalid table name, missing required fields
- `403`: Secret Manager permission denied
- `404`: Secret not found
- `502`: Database unreachable, query timeout
- `500`: Internal server error

#### `GET /connectors`
List available database connectors

**Response:**
```json
{
  "connectors": ["postgresql", "mysql"],
  "count": 2
}
```

### File Operations

#### `POST /convert/file`
Convert file to Parquet

See previous documentation for full details. Key changes in v3.0:
- Enhanced error classification (400/502/500)
- URL validation for signed URLs
- Improved async handling

#### `POST /infer/schema`
Infer schema from file for validation

See previous documentation for full details.

## Database Connectors

### PostgreSQL

**Features:**
- Zero-copy extraction via ConnectorX
- Parallel loading with partition support
- Automatic retry with exponential backoff
- SQL injection protection for table names
- Connection string validation

**Parallel Extraction Example:**
```json
{
  "table_name": "large_table",
  "partition_column": "id",
  "partition_num": 8
}
```

This splits the table into 8 partitions based on `id` column ranges, extracting in parallel.

### MySQL

Same features as PostgreSQL with MySQL-specific connection handling.

## Compression Options

### Snappy (Default)
- Fastest compression
- 2-4x compression ratio
- Best for general use

### Zstd
- Better compression ratio
- Slightly slower
- Best for archival/cold storage

### None
- No compression
- Fastest writes
- Largest file size
- Best for immediate processing

**Usage:**
```json
{
  "compression": "zstd"
}
```

## Security

### Credential Storage

Credentials are stored in Google Secret Manager:

1. Test connection with temporary credentials
2. Store credentials in Secret Manager
3. Use `credentials_id` for subsequent conversions
4. Credentials cached for 1 hour (configurable)

### URL Validation

Signed URLs are validated against allowed domains:
- `r2.cloudflarestorage.com`
- `s3.amazonaws.com`
- `storage.googleapis.com`

This prevents proxy abuse where malicious users provide URLs to unauthorized storage.

### SQL Injection Protection

Table names are validated with regex pattern `^[a-zA-Z0-9_\.]+$` to prevent injection attacks.

## Configuration

### Environment Variables

```bash
# Required
ENV=production
GCP_PROJECT_ID=your-project-id

# Processing limits
MAX_FILE_SIZE_MB=500
MAX_PROCESSING_TIME_MINUTES=10
MAX_MEMORY_USAGE_GB=2

# Database connection pooling
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
DB_POOL_RECYCLE_SECONDS=3600

# Parquet defaults
PARQUET_COMPRESSION=snappy
PARQUET_ROW_GROUP_SIZE=50000
```

### Google Cloud Setup

**Service Account Permissions:**
```bash
roles/secretmanager.secretAccessor
```

**Authentication:**
```bash
# Local development
gcloud auth application-default login

# Cloud Run (automatic)
# Uses workload identity
```

## Development

### Local Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment
export ENV=dev
export GCP_PROJECT_ID=your-project-id

# Run service
uvicorn app.main:app --reload --host 0.0.0.0 --port 8080
```

### Testing

```bash
# Health check
curl http://localhost:8080/health

# Deep health check
curl http://localhost:8080/health/deep

# List connectors
curl http://localhost:8080/connectors

# Test database connection
curl -X POST http://localhost:8080/test/database-connection \
  -H "Content-Type: application/json" \
  -d '{
    "connector_type": "postgresql",
    "credentials": {
      "host": "localhost",
      "port": 5432,
      "database": "test",
      "username": "user",
      "password": "pass"
    }
  }'
```

## Deployment

### Cloud Run Configuration

```yaml
CPU: 1 vCPU
Memory: 2 GB
Timeout: 600 seconds
Concurrency: 5
Min instances: 0
Max instances: 10
```

### Environment Variables

Set in Cloud Run:
```bash
ENV=production
GCP_PROJECT_ID=your-project-id
```

## Performance

### Database Extraction Benchmarks

| Rows | Columns | Partitions | Time | Throughput |
|------|---------|------------|------|------------|
| 1M | 10 | 1 | 15s | 66K rows/s |
| 1M | 10 | 4 | 5s | 200K rows/s |
| 1M | 10 | 8 | 3s | 333K rows/s |
| 10M | 20 | 8 | 45s | 222K rows/s |

*Benchmarks with PostgreSQL on Cloud SQL, standard instance*

### Optimization Tips

1. **Use partitioning** for tables > 100K rows
2. **Choose snappy compression** for balanced performance
3. **Specify column subset** in queries instead of `SELECT *`
4. **Index partition columns** in database for faster range scans

## Error Handling

### HTTP Status Codes

- `400`: User input errors (invalid table name, missing fields)
- `403`: Permission denied (Secret Manager access)
- `404`: Resource not found (secret, table)
- `502`: Upstream errors (database unreachable, storage unavailable)
- `500`: Internal errors (code bugs, unexpected failures)

### Error Response Format

```json
{
  "detail": "Invalid table name: 'users; DROP TABLE users'. Table names must contain only alphanumeric characters, underscores, and dots."
}
```

## Limitations

| Resource | Limit | Configurable |
|----------|-------|--------------|
| File size | 500 MB | Yes |
| Processing time | 10 minutes | Yes (Cloud Run) |
| Memory | 2 GB | Yes (Cloud Run) |
| Concurrent requests | 5 per instance | Yes (Cloud Run) |
| Secret cache TTL | 1 hour | Yes |
| Max partitions | 16 | Yes |

## Version History

### v3.0.0 (Current)
- Native database connectors (PostgreSQL, MySQL)
- Google Secret Manager integration
- Async thread pool for CPU-bound operations
- Compression options (snappy/zstd/none)
- URL validation for signed URLs
- SQL injection protection
- Improved error classification
- Deep health checks

### v2.0.0
- Excel support
- Schema inference
- Column transformations
- SQL connection testing

### v1.0.0
- Initial release
- File conversion (CSV, JSON, TSV)

## License

Proprietary - Halatio Analytics Platform

## Support

For issues or questions, contact the Halatio development team.
