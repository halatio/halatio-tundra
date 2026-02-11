# Halatio Tundra

**Version 4.0.0** — Data conversion service for files and databases to Parquet format.

## Overview

Halatio Tundra converts data from files and databases into optimised Parquet format. Built on **DuckDB**, it reads source files and writes Parquet output **directly to Cloudflare R2** — no temp files, no presigned URL management. Source metadata (org ID, file format, connector type) is looked up from Supabase, and the source status is updated after each successful conversion.

**Base URL:** `https://your-service.run.app`

---

## Architecture

```
Client                  Tundra                      Infrastructure
──────                  ──────                      ──────────────
POST /convert/file  ──► lookup source_id       ──► Supabase (sources table)
  { source_id }         build R2 paths
                        DuckDB COPY r2://… → r2://… ──► Cloudflare R2
                        update status         ──► Supabase
```

### Key design decisions

| Concern | Approach |
|---------|----------|
| Storage | DuckDB reads/writes R2 directly via a persistent secret (no httpx download/upload) |
| Memory  | Config passed at `duckdb.connect(config=…)` time to respect Cloud Run cgroup limits |
| Extensions | Pre-installed to `/opt/duckdb_extensions` at Docker build time; loaded with `autoload_known_extensions` at runtime |
| Secrets | DuckDB R2 persistent secret written to `/tmp/.duckdb/stored_secrets/` at service startup |
| Database credentials | Google Secret Manager (referenced by `credentials_id`) |
| Source metadata | Supabase PostgREST API |

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `ENV` | ✓ | `dev`, `staging`, `production` |
| `GCP_PROJECT_ID` | ✓ | Google Cloud project ID (Secret Manager) |
| `R2_ACCESS_KEY_ID` | ✓ | Cloudflare R2 access key |
| `R2_SECRET_ACCESS_KEY` | ✓ | Cloudflare R2 secret key |
| `R2_ACCOUNT_ID` | ✓ | Cloudflare account ID (32-char hex) |
| `R2_BUCKET_PREFIX` | | Prefix for per-org R2 buckets (default: `halatio-org`) |
| `SUPABASE_URL` | ✓ | Supabase project URL |
| `SUPABASE_SECRET_KEY` | ✓* | Supabase secret key (`sb_secret_...`, preferred) |
| `SUPABASE_SERVICE_ROLE_KEY` | ✓* | Legacy Supabase service role key (fallback) |
| `DUCKDB_MEMORY_LIMIT` | | DuckDB memory cap (default: `6GB`) |
| `DUCKDB_THREADS` | | DuckDB thread count (default: `2`) |
| `DUCKDB_TEMP_DIR` | | Spill directory (default: `/tmp/duckdb_swap`) |
| `DUCKDB_MAX_TEMP_DIR_SIZE` | | Max spill size (default: `1GB`) |

---

## R2 Path Convention

```
Source file:   r2://halatio-org-{org_id}/uploads/{source_id}
Output:        r2://halatio-org-{org_id}/processed/{source_id}.parquet
```

The file format (csv, json, …) and `org_id` are read from the Supabase `sources` table via `source_id`.

\* Provide at least one of `SUPABASE_SECRET_KEY` (preferred) or `SUPABASE_SERVICE_ROLE_KEY` (legacy fallback).

---

## API Endpoints

### Health

#### `GET /health`
```json
{ "status": "healthy", "service": "halatio-tundra", "version": "4.0.0" }
```

#### `GET /health/deep`
Checks Secret Manager and Supabase connectivity. Returns per-check status.

#### `GET /info`
Lists supported formats, connectors, limits, and feature flags.

---

### File Conversion

#### `POST /convert/file`

Converts a file already uploaded to R2 into Parquet.

**Request:**
```json
{
  "source_id": "uuid",
  "format": "csv",
  "options": {
    "column_mapping": { "old_name": "new_name" },
    "type_overrides":  { "amount": "DOUBLE" },
    "skip_rows":       [0, 1],
    "delimiter":       ",",
    "sheet_name":      "Sheet1"
  }
}
```

- `source_id` — UUID from the Supabase `sources` table
- `format` — optional override; if omitted, uses `connector_type` from the sources table
- `options` — all fields optional

**Response:**
```json
{
  "success": true,
  "metadata": {
    "rows": 125000,
    "columns": 8,
    "column_schema": { "fields": [...] },
    "file_size_mb": 2.4,
    "processing_time_seconds": 1.8,
    "source_type": "file"
  }
}
```

Rate limit: 10 requests/minute.

---

### Schema Inference

#### `POST /infer/schema`

Reads a sample from the source file and returns column types and statistics.

**Request:**
```json
{
  "source_id": "uuid",
  "format": "csv",
  "sample_size": 1000
}
```

**Response:**
```json
{
  "success": true,
  "schema": {
    "columns": [
      {
        "name": "amount",
        "inferred_type": "DOUBLE",
        "nullable": false,
        "null_count": 0,
        "unique_count": 1200,
        "sample_values": [1.5, 2.0, 3.7],
        "min_value": 0.01,
        "max_value": 9999.99
      }
    ],
    "total_rows": 1000,
    "total_columns": 8
  },
  "sample_data": [...],
  "warnings": []
}
```

Rate limit: 20 requests/minute.

---

### Database Conversion

#### `POST /test/database-connection`

Tests a database connection using temporary credentials (not stored).

**Request:**
```json
{
  "connector_type": "postgresql",
  "credentials": {
    "host": "db.example.com",
    "port": 5432,
    "database": "mydb",
    "username": "user",
    "password": "pass"
  }
}
```

Rate limit: 20 requests/minute.

---

#### `POST /convert/database`

Extracts a database table/query and writes Parquet to R2.

**Request:**
```json
{
  "source_id": "uuid",
  "credentials_id": "secret-manager-id",
  "query": "SELECT * FROM orders WHERE created_at > '2024-01-01'",
  "compression": "zstd"
}
```

- `source_id` — UUID from Supabase (connector type is read from there)
- `credentials_id` — Google Secret Manager ID containing connection credentials
- `query` or `table_name` — one must be provided
- `compression` — `zstd` (default), `snappy`, `none`

Rate limit: 10 requests/minute.

---

### Connectors

#### `GET /connectors`
```json
{
  "connectors": ["postgresql", "mysql", "sqlite", "mariadb", "redshift"],
  "count": 5
}
```

---

## Supported File Formats

| Format | DuckDB function |
|--------|-----------------|
| CSV | `read_csv()` |
| TSV | `read_csv(sep='\t')` |
| JSON / GeoJSON | `read_json()` |
| Excel (xlsx) | `read_xlsx()` (excel extension) |
| Parquet | `read_parquet()` |

## Supported Database Connectors

| Connector | DuckDB mechanism |
|-----------|-----------------|
| PostgreSQL | `postgres` extension (`ATTACH … TYPE postgres`) |
| MySQL / MariaDB | `mysql` extension (`ATTACH … TYPE mysql`) |
| SQLite | Built-in (`ATTACH … TYPE sqlite`) |
| Redshift | PostgreSQL-compatible connector |

---

## Cloud Run Configuration

Recommended Cloud Run settings:

```yaml
resources:
  limits:
    memory: 8Gi
    cpu: "2"
env:
  DUCKDB_MEMORY_LIMIT: "6GB"
  DUCKDB_THREADS: "2"
  DUCKDB_TEMP_DIR: "/tmp/duckdb_swap"
  DUCKDB_MAX_TEMP_DIR_SIZE: "1GB"
```

DuckDB memory is configured via `duckdb.connect(config=…)` at connection time, which correctly reads cgroup limits in containerised environments.
