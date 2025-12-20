import httpx
import logging
from typing import Dict, Any, Optional
from ..models.conversionRequest import SqlCredentials

logger = logging.getLogger(__name__)

class SqlConnectionTestService:
    """Service for testing SQL database connections"""

    TIMEOUT_SECONDS = 30  # 30 seconds for connection test

    @staticmethod
    async def test_connection(
        endpoint: str,
        database: str,
        credentials: SqlCredentials,
        database_type: Optional[str] = None,
        port: Optional[int] = None,
        ssl_mode: Optional[str] = None
    ) -> Dict[str, Any]:
        """Test SQL database connection"""

        try:
            logger.info(f"🔌 Testing SQL connection: {database}")

            # Build auth headers
            headers = SqlConnectionTestService._build_auth_headers(credentials)

            # Build test query (simple SELECT to verify connection)
            test_query = "SELECT 1 as connection_test"

            # Prepare request
            request_body: Dict[str, Any] = {
                "query": test_query,
                "database": database
            }

            # Add optional parameters if provided
            if port:
                request_body["port"] = port
            if ssl_mode:
                request_body["ssl_mode"] = ssl_mode

            # Execute test query
            async with httpx.AsyncClient(timeout=SqlConnectionTestService.TIMEOUT_SECONDS) as client:
                try:
                    response = await client.post(
                        endpoint,
                        headers=headers,
                        json=request_body
                    )
                    response.raise_for_status()

                    # Extract metadata
                    metadata = SqlConnectionTestService._extract_metadata(
                        response, database_type
                    )

                    logger.info(f"✅ SQL connection successful: {database}")

                    return {
                        "success": True,
                        "message": "Connection successful",
                        "metadata": metadata
                    }

                except httpx.ConnectError as e:
                    logger.warning(f"⚠️ Connection refused: {str(e)}")
                    return SqlConnectionTestService._build_error_response(
                        "connection_refused",
                        "Couldn't reach the database server. Check that the server is running and the host/port are correct.",
                        {
                            "technical_error": f"Connection refused ({str(e)})",
                            "endpoint_tested": endpoint,
                            "ssl_attempted": ssl_mode == "require"
                        },
                        [
                            "Verify the hostname and port are correct",
                            "Check your database server is running",
                            "Ensure your firewall allows connections from Illutix IPs"
                        ]
                    )

                except httpx.TimeoutException:
                    logger.warning(f"⚠️ Connection timeout")
                    return SqlConnectionTestService._build_error_response(
                        "connection_timeout",
                        "The server took too long to respond. This might mean the server is busy or unreachable.",
                        {
                            "technical_error": "Connection timeout",
                            "endpoint_tested": endpoint,
                            "timeout_seconds": SqlConnectionTestService.TIMEOUT_SECONDS
                        },
                        [
                            "Check your server is responding",
                            "Try increasing the timeout",
                            "Verify your firewall settings"
                        ]
                    )

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 401 or e.response.status_code == 403:
                        logger.warning(f"⚠️ Authentication failed")
                        return SqlConnectionTestService._build_error_response(
                            "authentication_failed",
                            "The username or password wasn't accepted. Double-check your credentials and try again.",
                            {
                                "technical_error": f"HTTP {e.response.status_code}",
                                "endpoint_tested": endpoint
                            },
                            [
                                "Verify your username and password are correct",
                                "Check if the database user exists",
                                "Ensure the user has necessary permissions"
                            ]
                        )
                    else:
                        raise

        except Exception as e:
            logger.error(f"❌ SQL connection test failed: {str(e)}")
            return SqlConnectionTestService._build_error_response(
                "unknown_error",
                f"An unexpected error occurred: {str(e)}",
                {
                    "technical_error": str(e),
                    "endpoint_tested": endpoint
                },
                [
                    "Check the error message above",
                    "Verify your connection parameters",
                    "Contact support if the issue persists"
                ]
            )

    @staticmethod
    def _build_auth_headers(credentials: SqlCredentials) -> Dict[str, str]:
        """Build authentication headers from credentials"""

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        if credentials.type == "basic":
            import base64
            if credentials.username and credentials.password:
                encoded = base64.b64encode(
                    f"{credentials.username}:{credentials.password}".encode()
                ).decode()
                headers["Authorization"] = f"Basic {encoded}"

        elif credentials.type == "bearer":
            if credentials.token:
                headers["Authorization"] = f"Bearer {credentials.token}"

        elif credentials.type == "api-key":
            if credentials.token:
                headers["X-API-Key"] = credentials.token

        return headers

    @staticmethod
    def _extract_metadata(
        response: httpx.Response,
        database_type: Optional[str]
    ) -> Dict[str, Any]:
        """Extract metadata from successful connection"""

        metadata = {
            "database_type": database_type or "Unknown",
            "estimated_latency_ms": int(response.elapsed.total_seconds() * 1000)
        }

        # Try to extract additional info from response
        try:
            data = response.json()
            # Different SQL APIs return different metadata
            # This is a best-effort extraction

            if isinstance(data, dict):
                # Check for common metadata fields
                if "version" in data:
                    metadata["database_version"] = data["version"]
                if "server_timezone" in data:
                    metadata["server_timezone"] = data["server_timezone"]

        except:
            pass

        return metadata

    @staticmethod
    def _build_error_response(
        error_code: str,
        message: str,
        details: Dict[str, Any],
        suggestions: list
    ) -> Dict[str, Any]:
        """Build standardized error response"""

        return {
            "success": False,
            "error": error_code,
            "message": message,
            "details": details,
            "suggestions": suggestions
        }
