# DuckDB Migration Guide

## Overview

This migration introduces **DuckDB** as the unified query engine for Halatio Tundra, replacing the previous Polars + ConnectorX approach. DuckDB provides native database connectors, better memory management, and **direct R2 write capability**.

## What Changed

### New DuckDB Connectors

The following connectors now use DuckDB:

- **PostgreSQL** - Native postgres scanner (`postgres` extension)
- **MySQL** - Native mysql scanner (`mysql` extension)
- **SQLite** - Native support (built-in)
- **MS SQL Server** - ADBC driver (`adbc_driver_mssql`)

### Legacy Connectors

Legacy Polars-based connectors are still available with `_legacy` suffix:
- `postgresql_legacy`
- `mysql_legacy`
- `sqlite_legacy`
- `mssql_legacy`

Oracle remains on the legacy connector (no DuckDB ADBC driver available).

## Key Benefits

### 1. Direct R2 Write
DuckDB can write directly to R2/S3 without creating temporary files:

```python
# No local temp file needed!
connector.extract_to_parquet(
    output_path='r2://bucket/output.parquet',
    r2_config={
        'key_id': 'xxx',
        'secret': 'xxx',
        'account_id': 'xxx'
    }
)
```

### 2. Better Memory Management
Configurable via environment variables:
```bash
DUCKDB_MEMORY_LIMIT=6GB
DUCKDB_TEMP_DIR=/tmp/duckdb
DUCKDB_THREADS=2
```

### 3. Optimized Compression
Default to ZSTD compression with 100K row groups (vs Snappy/50K):
- Better compression ratios
- Faster queries in analytical tools

### 4. Unified Query Engine
Same SQL interface across all databases with consistent performance.

## Installation

### Base Requirements
```bash
pip install -r requirements.txt
```

This installs:
- `duckdb>=1.1.0`
- All legacy dependencies (for fallback)

### MS SQL Server (ADBC)
For MS SQL Server support, install the ADBC driver:

```bash
pip install dbc
dbc add mssql
```

This installs the `adbc_driver_mssql` driver that DuckDB uses.

## Configuration

### Environment Variables

```bash
# Memory limit (default: 6GB)
DUCKDB_MEMORY_LIMIT=6GB

# Temp directory (default: /tmp/duckdb)
DUCKDB_TEMP_DIR=/tmp/duckdb

# Thread count (default: 2)
DUCKDB_THREADS=2
```

### Cloud Run Configuration

For Cloud Run deployments, update your `cloud-run.yaml`:

```yaml
resources:
  limits:
    memory: 8Gi
  cpu: 2

env:
  - name: DUCKDB_MEMORY_LIMIT
    value: "6GB"
  - name: DUCKDB_TEMP_DIR
    value: "/tmp/duckdb"
  - name: DUCKDB_THREADS
    value: "2"
```

## Usage Examples

### Basic Usage (Same as Before)

```python
from app.services.connectors import ConnectorFactory

# Create connector
connector = ConnectorFactory.create_connector(
    connector_type="postgresql",
    credentials={
        "host": "localhost",
        "port": 5432,
        "database": "mydb",
        "username": "user",
        "password": "pass"
    }
)

# Extract to local file
result = await connector.extract_to_parquet(
    output_path="/tmp/output.parquet",
    table_name="users"
)
```

### Direct R2 Write (New!)

```python
# Extract directly to R2 (no temp file!)
result = await connector.extract_to_parquet(
    output_path='r2://my-bucket/users.parquet',
    table_name="users",
    r2_config={
        'key_id': os.getenv('R2_KEY_ID'),
        'secret': os.getenv('R2_SECRET'),
        'account_id': os.getenv('R2_ACCOUNT_ID')
    }
)
```

### Custom Compression

```python
# Use ZSTD compression (default)
result = await connector.extract_to_parquet(
    output_path="/tmp/output.parquet",
    table_name="users",
    compression="zstd",
    row_group_size=100000
)

# Use Snappy for faster writes
result = await connector.extract_to_parquet(
    output_path="/tmp/output.parquet",
    table_name="users",
    compression="snappy"
)
```

### Using Legacy Connectors

```python
# If you need to use legacy Polars connectors
connector = ConnectorFactory.create_connector(
    connector_type="postgresql_legacy",
    credentials={...}
)
```

## Migration Checklist

- [x] Install `duckdb>=1.1.0` via requirements.txt
- [ ] For MS SQL Server: Install ADBC driver (`pip install dbc && dbc add mssql`)
- [ ] Update environment variables if needed (memory limit, temp dir)
- [ ] Update Cloud Run config with DuckDB settings
- [ ] Test connectors in staging environment
- [ ] Monitor memory usage and adjust `DUCKDB_MEMORY_LIMIT` if needed

## Troubleshooting

### MS SQL Server Connection Fails

**Error:** `Failed to create ADBC secret`

**Solution:** Install the ADBC driver:
```bash
pip install dbc
dbc add mssql
```

### Out of Memory Errors

**Error:** `Out of Memory Error`

**Solution:** Reduce memory limit or increase Cloud Run memory:
```bash
export DUCKDB_MEMORY_LIMIT=4GB
```

### Extension Not Found

**Error:** `Extension 'postgres' not found`

**Solution:** DuckDB auto-installs extensions on first use. Ensure internet connectivity or pre-install:
```python
conn.execute("INSTALL postgres")
```

## Performance Notes

### Memory Usage
- DuckDB uses ~1.5-2x the data size in memory during processing
- Set `DUCKDB_MEMORY_LIMIT` to 75% of available container memory
- Example: 8GB container → `DUCKDB_MEMORY_LIMIT=6GB`

### Compression Comparison
- **ZSTD:** Better compression (~30% smaller), slightly slower writes
- **Snappy:** Faster writes (~2x), larger files
- **None:** Fastest writes, largest files (not recommended)

### Thread Count
- Use 2 threads for Cloud Run (CPU limits)
- Can increase to 4-8 for dedicated VMs with more CPU

## Architecture

### DuckDB Base Connector (`duckdb_base.py`)
- Memory management and configuration
- R2 secret setup
- Base extraction logic with retry
- Abstract methods for database-specific attachment

### Database-Specific Connectors
Each connector implements:
- `_attach_database()` - Connect to external database
- `_get_db_alias()` - Return database alias for queries
- `test_connection()` - Verify connectivity

### Example: PostgreSQL Flow
```
1. Create DuckDB connection
2. Configure memory/temp settings
3. Install postgres extension
4. Attach PostgreSQL database as 'pg'
5. Execute: COPY (SELECT * FROM pg.table) TO 'output.parquet'
6. Close connection
```

## Future Enhancements

Potential future additions (not in current scope):
- Snowflake connector (ADBC available)
- BigQuery connector (ADBC available)
- Redshift connector (via postgres scanner)
- Databricks connector (ADBC available)
- Trino connector (ADBC available)

## Support

For issues or questions:
1. Check the [DuckDB documentation](https://duckdb.org/docs/)
2. Review ADBC driver docs at https://docs.adbc-drivers.org/
3. File an issue in the repo

---

**Migration Date:** 2026-02-04
**DuckDB Version:** 1.1.0+
**Status:** ✅ Complete
