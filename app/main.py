from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from .config import settings
from .models.conversionRequest import (
    FileConversionRequest,
    ConversionResponse, HealthResponse,
    SchemaInferRequest, SchemaInferResponse,
    ConnectionTestRequest, ConnectionTestResponse,
    DatabaseConversionRequest, ConversionMetadata
)
from .services.file_converter import FileConverter
from .services.schema_inference import SchemaInferenceService
from .services.secret_manager import get_secret_manager
from .services.connectors.factory import ConnectorFactory
from .utils import validate_signed_url, raise_http_exception, ValidationError, DatabaseError, StorageError
import tempfile
import os
import httpx

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setup rate limiter
limiter = Limiter(key_func=get_remote_address)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("🚀 Starting Halatio Tundra Data Conversion Service")
    yield
    # Shutdown
    logger.info("🛑 Halatio Tundra shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="Halatio Tundra",
    description="Production-grade data integration platform - Files, Databases, and APIs to Parquet (v3.0 with native database connectors)",
    version="3.0.0",
    lifespan=lifespan
)

# Add rate limiter state and exception handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=False,  # No credentials needed for conversion service
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/", response_model=HealthResponse)
async def root():
    """Root endpoint"""
    return HealthResponse(
        status="running",
        service="halatio-tundra",
        version="3.0.0"
    )

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Basic health check endpoint"""
    return HealthResponse(
        status="healthy",
        service="halatio-tundra",
        version="3.0.0"
    )

@app.get("/health/deep")
async def deep_health_check():
    """Deep health check including Secret Manager connectivity"""
    health_status = {
        "status": "healthy",
        "service": "halatio-tundra",
        "version": "3.0.0",
        "checks": {}
    }

    # Check Secret Manager connectivity
    try:
        secret_manager = get_secret_manager()
        # Try to list secrets to verify permissions
        secret_manager.client.list_secrets(
            request={"parent": f"projects/{secret_manager.project_id}"}
        )
        health_status["checks"]["secret_manager"] = "healthy"
    except Exception as e:
        health_status["checks"]["secret_manager"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"

    return health_status

@app.post("/convert/file", response_model=ConversionResponse)
@limiter.limit("10/minute")
async def convert_file(request: Request, body: FileConversionRequest):
    """Convert file from R2 source to parquet format"""
    if body.format is None:
        raise HTTPException(status_code=400, detail="`format` must be specified for file conversions.")
    logger.info(f"📄 Converting file: {body.format} → parquet")

    try:
        # Convert options to dict if present
        options = body.options.dict() if body.options else {}

        result = await FileConverter.convert(
            source_url=str(body.source_url),
            output_url=str(body.output_url),
            file_format=body.format,
            options=options
        )

        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])

        logger.info(f"✅ File conversion complete: {result['metadata']['rows']} rows")
        return ConversionResponse(**result)

    except Exception as e:
        logger.error(f"❌ File conversion failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Conversion failed: {str(e)}")

@app.post("/infer/schema", response_model=SchemaInferResponse)
@limiter.limit("20/minute")
async def infer_schema(request: Request, body: SchemaInferRequest):
    """Infer schema from file for backend verification"""
    logger.info(f"🔍 Inferring schema: {body.format}")

    try:
        result = await SchemaInferenceService.infer_schema(
            source_url=str(body.source_url),
            file_format=body.format,
            sample_size=body.sample_size
        )

        logger.info(f"✅ Schema inference complete: {result['schema_info']['total_columns']} columns")
        return SchemaInferResponse(**result)

    except Exception as e:
        logger.error(f"❌ Schema inference failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Schema inference failed: {str(e)}")

@app.post("/test/database-connection", response_model=ConnectionTestResponse)
@limiter.limit("20/minute")
async def test_database_connection(request: Request, body: ConnectionTestRequest):
    """Test database connection before saving credentials"""
    logger.info(f"Testing {body.connector_type} connection")

    try:
        # Create connector instance
        connector = ConnectorFactory.create_connector(
            connector_type=body.connector_type,
            credentials=body.credentials.dict()
        )

        # Test connection
        result = await connector.test_connection()

        if result["success"]:
            logger.info(f"Connection test successful: {body.connector_type}")
        else:
            logger.warning(f"Connection test failed: {result.get('error')}")

        return ConnectionTestResponse(**result)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        raise_http_exception(e)

@app.post("/convert/database", response_model=ConversionResponse)
@limiter.limit("10/minute")
async def convert_database_data(request: Request, body: DatabaseConversionRequest):
    """Extract database data and convert to Parquet"""
    logger.info(f"Converting {body.connector_type} data")

    temp_path = None

    try:
        # Validate signed URL
        validate_signed_url(str(body.output_url))

        # Get credentials from Secret Manager
        secret_manager = get_secret_manager()
        credentials = secret_manager.get_credentials(body.credentials_id)

        # Create connector
        connector = ConnectorFactory.create_connector(
            connector_type=body.connector_type,
            credentials=credentials
        )

        # Create temporary file for parquet output
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            temp_path = tmp_file.name

        # Extract to parquet
        metadata = await connector.extract_to_parquet(
            output_path=temp_path,
            query=body.query,
            table_name=body.table_name,
            partition_column=body.partition_column,
            partition_num=body.partition_num,
            compression=body.compression
        )

        # Upload to R2
        try:
            async with httpx.AsyncClient(timeout=300) as client:
                with open(temp_path, "rb") as f:
                    parquet_data = f.read()

                response = await client.put(
                    str(body.output_url),
                    content=parquet_data,
                    headers={"Content-Type": "application/x-parquet"}
                )
                response.raise_for_status()
        except httpx.HTTPError as e:
            raise StorageError(f"Failed to upload to storage: {str(e)}")

        logger.info(f"Database conversion complete: {metadata['rows']} rows")

        # Build response
        conversion_metadata = ConversionMetadata(
            rows=metadata["rows"],
            columns=metadata["columns"],
            column_schema={"fields": []},
            file_size_mb=metadata["file_size_mb"],
            processing_time_seconds=metadata["processing_time_seconds"],
            source_type="database",
            connection_info={
                "connector_type": body.connector_type,
                "query": metadata.get("query", ""),
                "partitioned": metadata.get("partitioned", False),
                "compression": body.compression
            }
        )

        return ConversionResponse(
            success=True,
            metadata=conversion_metadata
        )

    except (ValidationError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Database conversion failed: {str(e)}")
        raise_http_exception(e)
    finally:
        # Clean up temp file
        if temp_path and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except Exception as cleanup_error:
                logger.warning(f"Failed to clean up temp file: {cleanup_error}")

@app.get("/connectors")
async def list_available_connectors():
    """List all available connector types"""
    return {
        "connectors": ConnectorFactory.list_connectors(),
        "count": len(ConnectorFactory.list_connectors())
    }

@app.get("/info")
async def service_info():
    """Get service capabilities and limits"""
    return {
        "service": "halatio-tundra",
        "version": "3.0.0",
        "capabilities": {
            "file_formats": ["csv", "tsv", "excel", "json", "parquet"],
            "output_format": "parquet",
            "max_file_size_mb": 500,
            "supported_sources": ["file", "database"],
            "database_connectors": ConnectorFactory.list_connectors()
        },
        "limits": {
            "max_processing_time_minutes": 10,
            "max_memory_usage_gb": 2,
            "max_rows_processed": 10_000_000
        },
        "features": {
            "polars_native": True,
            "streaming_processing": True,
            "automatic_schema_inference": True,
            "optimized_parquet_output": True,
            "excel_support": True,
            "column_transformations": True,
            "schema_inference": True,
            "sql_connection_testing": True,
            "database_connectors": True,
            "secret_manager_integration": True,
            "parallel_extraction": True
        }
    }