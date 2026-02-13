from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import duckdb
import logging
import os
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import anyio

from .config import settings
from .models.conversionRequest import (
    FileConversionRequest,
    ConversionResponse,
    HealthResponse,
    SchemaInferRequest,
    SchemaInferResponse,
    ConnectionTestRequest,
    ConnectionTestResponse,
    DatabaseConversionRequest,
    ConversionMetadata,
)
from .services.file_converter import FileConverter
from .services.schema_inference import SchemaInferenceService
from .services.secret_manager import get_secret_manager
from .services.supabase_client import (
    get_source,
    create_source_version,
    update_source_version,
    update_source_current_version,
    update_source_extraction_query,
)
from .services.connectors.factory import ConnectorFactory
from .services.connectors.duckdb_base import _make_duckdb_config
from .utils import raise_http_exception, ValidationError, DatabaseError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)


def _r2_bucket(org_id: str) -> str:
    """Return the R2 bucket name for an organisation."""
    return f"org-{org_id}"


def _source_path(org_id: str, source_id: str) -> str:
    """R2 path for the uploaded source file directory (no filename)."""
    return f"r2://{_r2_bucket(org_id)}/uploads/{source_id}"


def _output_path(org_id: str, source_id: str, version: int) -> str:
    """R2 path for the processed Parquet file."""
    return f"r2://{_r2_bucket(org_id)}/processed/{source_id}/v{version}/data.parquet"


def _raw_path(org_id: str, source_id: str, version: int, ext: str) -> str:
    """R2 path for the archived raw file."""
    return f"r2://{_r2_bucket(org_id)}/raw/{source_id}/v{version}/data.{ext}"


def _setup_r2_persistent_secret() -> None:
    """
    Create a persistent DuckDB R2 secret written to /tmp/.duckdb/stored_secrets/.

    This runs once at startup.  All subsequent DuckDB connections created with
    home_directory='/tmp' will automatically load this secret, enabling direct
    R2 reads and writes without per-connection configuration.
    """
    key_id = settings.R2_ACCESS_KEY_ID
    secret_key = settings.R2_SECRET_ACCESS_KEY
    if not key_id or not secret_key:
        raise ValueError(
            "R2 credentials are not configured. Set mounted secrets, Secret Manager "
            "references, or local env fallback values."
        )

    safe_key_id = key_id.replace("'", "''")
    safe_secret_key = secret_key.replace("'", "''")

    os.makedirs("/tmp/.duckdb/stored_secrets", exist_ok=True)
    conn = duckdb.connect(":memory:", config=_make_duckdb_config())
    try:
        conn.execute("SET autoinstall_known_extensions = false")
        conn.execute("SET autoload_known_extensions = true")
        conn.execute(f"""
            CREATE OR REPLACE PERSISTENT SECRET r2_tundra (
                TYPE r2,
                KEY_ID '{safe_key_id}',
                SECRET '{safe_secret_key}',
                ACCOUNT_ID '{settings.CLOUDFLARE_ACCOUNT_ID}'
            )
        """)
        logger.info("DuckDB R2 persistent secret created")
    finally:
        conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Halatio Tundra Data Conversion Service")
    await anyio.to_thread.run_sync(_setup_r2_persistent_secret)
    yield
    logger.info("Halatio Tundra shutdown complete")


app = FastAPI(
    title="Halatio Tundra",
    description="Data conversion service — Files and Databases to Parquet via DuckDB",
    version="4.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Request-ID"],
)


# ---------------------------------------------------------------------------
# Health & info
# ---------------------------------------------------------------------------

@app.get("/", response_model=HealthResponse)
async def root():
    return HealthResponse(status="running", service="halatio-tundra", version="4.0.0")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    return HealthResponse(status="healthy", service="halatio-tundra", version="4.0.0")


@app.get("/health/deep")
async def deep_health_check():
    health_status = {
        "status": "healthy",
        "service": "halatio-tundra",
        "version": "4.0.0",
        "checks": {},
    }

    try:
        secret_manager = get_secret_manager()
        secret_manager.client.list_secrets(
            request={"parent": f"projects/{secret_manager.project_id}"}
        )
        health_status["checks"]["secret_manager"] = "healthy"
    except Exception as e:
        health_status["checks"]["secret_manager"] = f"unhealthy: {e}"
        health_status["status"] = "degraded"

    try:
        from .services.supabase_client import get_supabase_client
        sb = get_supabase_client()
        # Lightweight probe: list 1 row from sources table
        sb.table("sources").select("id").limit(1).execute()
        health_status["checks"]["supabase"] = "healthy"
    except Exception as e:
        health_status["checks"]["supabase"] = f"unhealthy: {e}"
        health_status["status"] = "degraded"

    return health_status


@app.get("/info")
async def service_info():
    return {
        "service": "halatio-tundra",
        "version": "4.0.0",
        "capabilities": {
            "file_formats": ["csv", "tsv", "json", "geojson", "excel", "parquet"],
            "output_format": "parquet",
            "max_file_size_mb": 500,
            "supported_sources": ["file", "database"],
            "database_connectors": ConnectorFactory.list_connectors(),
            "query_engine": "duckdb",
            "direct_r2_write": True,
        },
        "limits": {
            "max_processing_time_minutes": 10,
            "max_memory_usage_gb": 6,
            "max_rows_processed": "unlimited (memory-bound)",
        },
        "features": {
            "duckdb_engine": True,
            "direct_r2_read_write": True,
            "automatic_schema_inference": True,
            "optimized_parquet_output": True,
            "excel_support": True,
            "column_transformations": True,
            "database_connectors": True,
            "secret_manager_integration": True,
            "supabase_integration": True,
            "configurable_memory_limits": True,
        },
    }


# ---------------------------------------------------------------------------
# Conversion endpoints
# ---------------------------------------------------------------------------

@app.post("/convert/file", response_model=ConversionResponse)
@limiter.limit("10/minute")
async def convert_file(request: Request, body: FileConversionRequest):
    """Convert a file source to Parquet directly in R2."""
    logger.info(f"File conversion requested: source_id={body.source_id}")

    source_version = None
    try:
        source = await get_source(body.source_id)

        org_id = source["organization_id"]
        file_format = (body.format.value if body.format else source["connector_type"])
        next_version = source["current_version"] + 1

        # Create pending source_version record
        source_version = await create_source_version(
            source_id=body.source_id,
            version=next_version,
            status="pending",
        )

        src = _source_path(org_id, body.source_id)
        out = _output_path(org_id, body.source_id, next_version)

        logger.info(f"Converting {file_format} -> {out}")

        options = body.options.dict() if body.options else {}
        result = await FileConverter.convert(
            source_path=src,
            output_path=out,
            file_format=file_format,
            options=options,
            version=next_version,
        )

        if not result["success"]:
            await update_source_version(
                version_id=source_version["id"],
                status="error",
                error_message=result.get("error"),
            )
            raise HTTPException(status_code=500, detail=result["error"])

        meta = result["metadata"]

        # Update source_version with success metrics
        await update_source_version(
            version_id=source_version["id"],
            status="active",
            row_count=meta["rows"],
            column_count=meta["columns"],
            file_size_bytes=int(meta["file_size_mb"] * 1024 * 1024) or None,
            processing_time_seconds=meta["processing_time_seconds"],
        )

        # Increment sources.current_version
        await update_source_current_version(body.source_id, next_version)

        logger.info(f"File conversion complete: {meta['rows']} rows, v{next_version}")
        return ConversionResponse(**result)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"File conversion failed: {e}")
        if source_version:
            await update_source_version(
                version_id=source_version["id"],
                status="error",
                error_message=str(e),
            )
        raise HTTPException(status_code=500, detail=f"Conversion failed: {e}")


@app.post("/infer/schema", response_model=SchemaInferResponse)
@limiter.limit("20/minute")
async def infer_schema(request: Request, body: SchemaInferRequest):
    """Infer schema from a file source directly in R2."""
    logger.info(f"Schema inference requested: source_id={body.source_id}")

    try:
        source = await get_source(body.source_id)

        org_id = source["organization_id"]
        file_format = (body.format.value if body.format else source["connector_type"])

        src = _source_path(org_id, body.source_id)

        result = await anyio.to_thread.run_sync(
            SchemaInferenceService.infer_schema,
            src,
            file_format,
            body.sample_size,
        )

        logger.info(f"Schema inference complete: {result['schema_info']['total_columns']} columns")
        return SchemaInferResponse(**result)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Schema inference failed: {e}")
        raise HTTPException(status_code=500, detail=f"Schema inference failed: {e}")


@app.post("/test/database-connection", response_model=ConnectionTestResponse)
@limiter.limit("20/minute")
async def test_database_connection(request: Request, body: ConnectionTestRequest):
    """Test a database connection before saving credentials."""
    logger.info(f"Testing {body.connector_type} connection")

    try:
        connector = ConnectorFactory.create_connector(
            connector_type=body.connector_type,
            credentials=body.credentials.dict(),
        )
        result = await connector.test_connection()

        if result["success"]:
            logger.info(f"Connection test succeeded: {body.connector_type}")
        else:
            logger.warning(f"Connection test failed: {result.get('error')}")

        return ConnectionTestResponse(**result)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Connection test error: {e}")
        raise_http_exception(e)


@app.post("/convert/database", response_model=ConversionResponse)
@limiter.limit("10/minute")
async def convert_database_data(request: Request, body: DatabaseConversionRequest):
    """Extract database data and write Parquet directly to R2."""
    logger.info(f"Database conversion requested: source_id={body.source_id}")

    source_version = None
    try:
        source = await get_source(body.source_id)

        org_id = source["organization_id"]
        connector_type = source["connector_type"]
        next_version = source["current_version"] + 1

        # Create pending source_version record
        source_version = await create_source_version(
            source_id=body.source_id,
            version=next_version,
            status="pending",
        )

        out = _output_path(org_id, body.source_id, next_version)

        # Retrieve credentials from Secret Manager
        secret_manager = get_secret_manager()
        credentials = secret_manager.get_credentials(body.credentials_id)

        connector = ConnectorFactory.create_connector(
            connector_type=connector_type,
            credentials=credentials,
        )

        metadata = await connector.extract_to_parquet(
            output_path=out,
            query=body.query,
            table_name=body.table_name,
            compression=body.compression.value,
        )

        # Store extraction query if provided
        query_used = body.query or metadata.get("query", "")
        if query_used:
            await update_source_extraction_query(body.source_id, query_used)

        # Update source_version with success metrics
        await update_source_version(
            version_id=source_version["id"],
            status="active",
            row_count=metadata["rows"],
            column_count=metadata["columns"],
            file_size_bytes=int(metadata["file_size_mb"] * 1024 * 1024) or None,
            processing_time_seconds=metadata["processing_time_seconds"],
        )

        # Increment sources.current_version
        await update_source_current_version(body.source_id, next_version)

        logger.info(
            f"Database conversion complete: {metadata['rows']} rows -> {out}"
        )

        conversion_metadata = ConversionMetadata(
            version=next_version,
            rows=metadata["rows"],
            columns=metadata["columns"],
            column_schema={"fields": []},
            file_size_mb=metadata["file_size_mb"],
            processing_time_seconds=metadata["processing_time_seconds"],
            source_type="database",
            connection_info={
                "connector_type": connector_type,
                "query": metadata.get("query", ""),
                "compression": body.compression.value,
                "engine": "duckdb",
            },
        )

        return ConversionResponse(success=True, metadata=conversion_metadata)

    except (ValidationError, ValueError) as e:
        if source_version:
            await update_source_version(
                version_id=source_version["id"],
                status="error",
                error_message=str(e),
            )
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Database conversion failed: {e}")
        if source_version:
            await update_source_version(
                version_id=source_version["id"],
                status="error",
                error_message=str(e),
            )
        raise_http_exception(e)


# ---------------------------------------------------------------------------
# Connector listing
# ---------------------------------------------------------------------------

@app.get("/connectors")
async def list_available_connectors():
    connectors = ConnectorFactory.list_connectors()
    return {"connectors": connectors, "count": len(connectors)}
