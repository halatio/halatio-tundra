"""Utility functions for validation and error handling"""

from urllib.parse import urlparse
from fastapi import HTTPException
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

# Allowed R2/S3 domains for signed URL validation
ALLOWED_STORAGE_DOMAINS = [
    "r2.cloudflarestorage.com",
    "s3.amazonaws.com",
    "storage.googleapis.com"
]

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
            error_code="upstream_error"
        )

class DatabaseError(UpstreamError):
    """Database connection/query error"""
    def __init__(self, detail: str):
        super().__init__(detail=detail, service="Database")

class StorageError(UpstreamError):
    """Storage service error"""
    def __init__(self, detail: str):
        super().__init__(detail=detail, service="Storage")

def validate_signed_url(url: str, allowed_domains: Optional[List[str]] = None) -> None:
    """
    Validate that signed URL points to an allowed storage domain

    Args:
        url: URL to validate
        allowed_domains: List of allowed domains (default: R2/S3/GCS)

    Raises:
        ValidationError: If URL domain is not allowed
    """
    if allowed_domains is None:
        allowed_domains = ALLOWED_STORAGE_DOMAINS

    parsed = urlparse(url)

    # Check if domain is in allowed list or is a subdomain
    is_allowed = any(
        parsed.netloc.endswith(domain) or parsed.netloc == domain
        for domain in allowed_domains
    )

    if not is_allowed:
        logger.warning(f"Rejected URL with untrusted domain: {parsed.netloc}")
        raise ValidationError(
            f"Invalid storage URL. Domain '{parsed.netloc}' is not in allowed list. "
            f"Allowed domains: {', '.join(allowed_domains)}"
        )

def classify_error(exception: Exception) -> tuple[int, str, str]:
    """
    Classify exception and return appropriate HTTP status code

    Args:
        exception: Exception to classify

    Returns:
        Tuple of (status_code, error_code, detail)
    """
    error_str = str(exception).lower()

    # User errors (400)
    if isinstance(exception, ValueError):
        return 400, "validation_error", str(exception)

    # Connection/network errors (502/503)
    if any(keyword in error_str for keyword in ["connection", "unreachable", "timeout", "refused"]):
        return 502, "upstream_error", f"Database connection failed: {exception}"

    # Permission errors (403)
    if any(keyword in error_str for keyword in ["permission", "denied", "unauthorized", "forbidden"]):
        return 403, "permission_denied", str(exception)

    # File/resource not found (404)
    if any(keyword in error_str for keyword in ["not found", "does not exist"]):
        return 404, "not_found", str(exception)

    # Default to 500
    return 500, "internal_error", f"Internal server error: {exception}"

def raise_http_exception(exception: Exception) -> None:
    """
    Convert exception to HTTPException with appropriate status code

    Args:
        exception: Exception to convert

    Raises:
        HTTPException: With appropriate status code
    """
    if isinstance(exception, TundraException):
        raise HTTPException(
            status_code=exception.status_code,
            detail=exception.detail
        )

    status_code, error_code, detail = classify_error(exception)

    logger.error(f"Error [{error_code}]: {detail}")

    raise HTTPException(status_code=status_code, detail=detail)
