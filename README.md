# Halatio Tundra

**Version 4.0.0** — Data conversion service for files and databases to Parquet format.

## Overview

Halatio Tundra converts data from files and databases into optimised Parquet format. Built on **DuckDB**, it reads source files and writes Parquet output **directly to Cloudflare R2** — no temp files, no presigned URL management. Source metadata (org ID, file format, connector type) is looked up from Supabase, and each conversion creates a versioned record in the `source_versions` table.

**Base URL:** `https://your-service.run.app`

---

## Architecture

```
Client                  Tundra                      Infrastructure
──────                  ──────                      ──────────────
POST /convert/file  ──► lookup source_id       ──► Supabase (sources table)
  { source_id }         create source_version (pending)
                        DuckDB COPY r2://… → r2://… ──► Cloudflare R2
                        update source_version (active)
                        increment current_version  ──► Supabase
```

### Key design decisions

| Concern | Approach |
|---------|----------|
| Storage | DuckDB reads/writes R2 directly via a persistent secret (no httpx download/upload) |
| Versioning | Each conversion creates a `source_versions` record; `sources.current_version` tracks the latest |
| Bucket naming | Always `org-{org_id}` (created by Supabase Edge Function on org creation) |
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
| `CLOUDFLARE_ACCOUNT_ID` | ✓ | Cloudflare account ID (32-char hex) |
| `SUPABASE_URL` | ✓ | Supabase project URL |
| `SUPABASE_SECRET_KEY` | ✓ | Supabase secret key (`sb_secret_...`) |
| `DUCKDB_MEMORY_LIMIT` | | DuckDB memory cap (default: `6GB`) |
| `DUCKDB_THREADS` | | DuckDB thread count (default: `2`) |
| `DUCKDB_TEMP_DIR` | | Spill directory (default: `/tmp/duckdb_swap`) |
| `DUCKDB_MAX_TEMP_DIR_SIZE` | | Max spill size (default: `1GB`) |
| `USE_ADBC_DRIVER` | | Set to `true` to use ADBC for PostgreSQL/Redshift (see below) |
| `MAX_PROCESSING_TIME_MINUTES` | | Async timeout for conversions (default: `10`) |

---

## R2 Path Convention (Medallion Architecture)

```
org-{org_id}/
├── uploads/{source_id}/               # User uploads land here via presigned URL
│   └── {original_filename}            # Original uploaded file
│
├── raw/{source_id}/v{version}/        # Permanent archive after validation
│   ├── data.{ext}                     # Original file moved from uploads/
│   └── _manifest.json                 # Metadata (optional)
│
└── processed/{source_id}/v{version}/  # Query-ready Parquet
    └── data.parquet                   # Converted file
```

| Function | Path |
|----------|------|
| `_source_path(org_id, source_id)` | `r2://org-{org_id}/uploads/{source_id}` |
| `_output_path(org_id, source_id, version)` | `r2://org-{org_id}/processed/{source_id}/v{version}/data.parquet` |
| `_raw_path(org_id, source_id, version, ext)` | `r2://org-{org_id}/raw/{source_id}/v{version}/data.{ext}` |

The file format (csv, json, …) and `org_id` are read from the Supabase `sources` table via `source_id`.

---

## Versioning Workflow

Each conversion follows this lifecycle:

1. **Start** — Create a `source_versions` record with `status='pending'`
2. **Process** — Convert the source to Parquet at `processed/{source_id}/v{version}/data.parquet`
3. **Success** — Update the `source_versions` record to `status='active'` with metrics (`row_count`, `column_count`, `file_size_bytes`, `processing_time_seconds`), then increment `sources.current_version`
4. **Failure** — Update the `source_versions` record to `status='error'` with `error_message`

Version numbers are calculated as `sources.current_version + 1`.

### Database Schema

**sources** table fields used by Tundra:
- `source_type` — enum: `file`, `database`, `api`
- `connector_type` — string (e.g. `csv`, `postgresql`, `mysql`)
- `current_version` — integer (default `0`)
- `extraction_query` — text (nullable, stored for database sources)

**source_versions** table:
- `id` — UUID primary key
- `source_id` — UUID foreign key to `sources.id`
- `version` — integer (scoped to source_id)
- `status` — enum: `pending`, `active`, `error`
- `row_count`, `column_count`, `file_size_bytes`, `processing_time_seconds` — nullable metrics
- `error_message` — text (nullable)
- `created_at` — timestamptz

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

Converts a file already uploaded to R2 into Parquet. Creates a new version.

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
    "version": 1,
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

Extracts a database table/query and writes Parquet to R2. Creates a new version and stores the extraction query.

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

**Response:**
```json
{
  "success": true,
  "metadata": {
    "version": 1,
    "rows": 50000,
    "columns": 6,
    "column_schema": { "fields": [] },
    "file_size_mb": 3.2,
    "processing_time_seconds": 4.5,
    "source_type": "database",
    "connection_info": {
      "connector_type": "postgresql",
      "query": "SELECT * FROM orders WHERE created_at > '2024-01-01'",
      "compression": "zstd",
      "engine": "duckdb"
    }
  }
}
```

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

| Connector | Default engine | ADBC alternative |
|-----------|---------------|-----------------|
| PostgreSQL | `postgres` extension (`ATTACH … TYPE postgres`) | ✓ `adbc-driver-postgresql` |
| MySQL / MariaDB | `mysql` extension (`ATTACH … TYPE mysql`) | — |
| SQLite | Built-in (`ATTACH … TYPE sqlite`) | — |
| Redshift | PostgreSQL-compatible connector | ✓ `adbc-driver-postgresql` |

---

## ADBC Support (Optional)

For PostgreSQL and Redshift extractions, you can enable **ADBC** (Arrow Database Connectivity) for better performance on large result sets:

```bash
export USE_ADBC_DRIVER=true
```

Requires `adbc-driver-postgresql>=1.1.0` (included in `requirements.txt`).

**How it works:** ADBC uses PostgreSQL's native COPY protocol to stream data directly as Arrow columnar format, bypassing row-by-row serialisation. Tundra then hands the in-memory Arrow table to DuckDB for Parquet/R2 writing, so direct R2 writes are fully preserved.

**Performance benefit:** Typically 5–10× faster for >100 k-row extractions compared to the DuckDB postgres scanner, with lower peak memory usage due to zero-copy Arrow transfer.

**Fallback behaviour:** If `adbc_driver_postgresql` is not installed, Tundra logs a warning and falls back to the DuckDB postgres scanner automatically — no downtime.

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
