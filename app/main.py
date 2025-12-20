from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from .config import settings
from .models.conversionRequest import (
    FileConversionRequest, SqlConversionRequest,
    ConversionResponse, HealthResponse,
    SchemaInferRequest, SchemaInferResponse,
    SqlConnectionTestRequest, SqlConnectionTestResponse
)
from .services.file_converter import FileConverter
from .services.sql_converter import SqlConverter
from .services.schema_inference import SchemaInferenceService
from .services.sql_connection_test import SqlConnectionTestService

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setup rate limiter
limiter = Limiter(key_func=get_remote_address)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("🚀 Starting Illutix Tundra Data Conversion Service")
    yield
    # Shutdown
    logger.info("🛑 Illutix Tundra shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="Illutix Tundra",
    description="High-performance data conversion service - Files to Parquet (v2.0 with Excel, schema inference, and enhanced options)",
    version="2.0.0",
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
        service="illutix-tundra",
        version="2.0.0"
    )

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        service="illutix-tundra",
        version="2.0.0"
    )

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


@app.post("/convert/sql", response_model=ConversionResponse)
@limiter.limit("10/minute")
async def convert_sql_data(request: Request, body: SqlConversionRequest):
    """Execute SQL query and convert results to parquet format"""

    logger.info(f"💾 Converting SQL data: {body.sql_database}")

    try:
        # Convert options to dict if present
        options = body.options.dict() if body.options else {}

        result = await SqlConverter.convert(
            endpoint=str(body.sql_endpoint),
            database=body.sql_database,
            query=body.sql_query,
            output_url=str(body.output_url),
            credentials_id=body.credentials_id,
            options=options
        )

        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])

        logger.info(f"✅ SQL conversion complete: {result['metadata']['rows']} rows")
        return ConversionResponse(**result)

    except Exception as e:
        logger.error(f"❌ SQL conversion failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"SQL conversion failed: {str(e)}")

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

@app.post("/test/sql-connection", response_model=SqlConnectionTestResponse)
@limiter.limit("20/minute")
async def test_sql_connection(request: Request, body: SqlConnectionTestRequest):
    """Test SQL database connection"""
    logger.info(f"🔌 Testing SQL connection: {body.sql_database}")

    try:
        result = await SqlConnectionTestService.test_connection(
            endpoint=str(body.sql_endpoint),
            database=body.sql_database,
            credentials=body.credentials,
            database_type=body.database_type,
            port=body.port,
            ssl_mode=body.ssl_mode
        )

        if result["success"]:
            logger.info(f"✅ SQL connection test successful")
        else:
            logger.warning(f"⚠️ SQL connection test failed: {result.get('error')}")

        return SqlConnectionTestResponse(**result)

    except Exception as e:
        logger.error(f"❌ SQL connection test failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Connection test failed: {str(e)}")

@app.get("/info")
async def service_info():
    """Get service capabilities and limits"""
    return {
        "service": "illutix-tundra",
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
            "sql_connection_testing": True
        }
    }