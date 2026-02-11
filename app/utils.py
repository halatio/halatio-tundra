"""Utility functions for error classification and HTTP exception handling"""

from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)


class TundraException(Exception):
    """Base exception for Tundra errors with HTTP status codes"""

    def __init__(self, status_code: int, detail: str, error_code: str = "internal_error"):
        self.status_code = status_code
        self.detail = detail
        self.error_code = error_code
        super().__init__(detail)


class ValidationError(TundraException):
    """User input validation error (400)"""
    def __init__(self, detail: str):
        super().__init__(status_code=400, detail=detail, error_code="validation_error")


class UpstreamError(TundraException):
    """External service error (502)"""
    def __init__(self, detail: str, service: str):
        super().__init__(
            status_code=502,
            detail=f"{service} error: {detail}",
            error_code="upstream_error",
        )


class DatabaseError(UpstreamError):
    """Database connection/query error"""
    def __init__(self, detail: str):
        super().__init__(detail=detail, service="Database")


class StorageError(UpstreamError):
    """Storage service error"""
    def __init__(self, detail: str):
        super().__init__(detail=detail, service="Storage")


def classify_error(exception: Exception) -> tuple:
    """
    Classify an exception and return an appropriate HTTP status code.

    Returns:
        Tuple of (status_code, error_code, detail)
    """
    error_str = str(exception).lower()

    if isinstance(exception, ValueError):
        return 400, "validation_error", str(exception)

    if any(k in error_str for k in ["connection", "unreachable", "timeout", "refused"]):
        return 502, "upstream_error", f"Database connection failed: {exception}"

    if any(k in error_str for k in ["permission", "denied", "unauthorized", "forbidden"]):
        return 403, "permission_denied", str(exception)

    if any(k in error_str for k in ["not found", "does not exist"]):
        return 404, "not_found", str(exception)

    return 500, "internal_error", f"Internal server error: {exception}"


def raise_http_exception(exception: Exception) -> None:
    """Convert an exception to an HTTPException with an appropriate status code."""
    if isinstance(exception, TundraException):
        raise HTTPException(
            status_code=exception.status_code,
            detail=exception.detail,
        )

    status_code, error_code, detail = classify_error(exception)
    logger.error(f"Error [{error_code}]: {detail}")
    raise HTTPException(status_code=status_code, detail=detail)
