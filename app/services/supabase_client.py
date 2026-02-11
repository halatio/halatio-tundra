"""Supabase client for querying source metadata and updating status"""

import logging
from typing import Any, Dict, Optional
from supabase import create_client, Client
from ..config import settings

logger = logging.getLogger(__name__)

_client: Optional[Client] = None


def get_supabase_client() -> Client:
    """Return a module-level singleton Supabase client."""
    global _client
    if _client is None:
        _client = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)
    return _client


async def get_source(source_id: str) -> Dict[str, Any]:
    """
    Fetch a source record from the sources table.

    Returns a dict with: id, organization_id, connector_type, source_type

    Raises:
        ValueError: if the source is not found
    """
    client = get_supabase_client()
    response = (
        client.table("sources")
        .select("id, organization_id, connector_type, source_type")
        .eq("id", source_id)
        .single()
        .execute()
    )

    if not response.data:
        raise ValueError(f"Source not found: {source_id}")

    return response.data


async def update_source(
    source_id: str,
    status: str,
    row_count: Optional[int] = None,
    file_size_bytes: Optional[int] = None,
) -> None:
    """
    Update a source record's status and optional metrics.

    Args:
        source_id: UUID of the source record
        status: New status value ("active", "error", etc.)
        row_count: Number of rows in the processed file
        file_size_bytes: Size of the output parquet file in bytes
    """
    client = get_supabase_client()
    payload: Dict[str, Any] = {"status": status}
    if row_count is not None:
        payload["row_count"] = row_count
    if file_size_bytes is not None:
        payload["file_size_bytes"] = file_size_bytes

    client.table("sources").update(payload).eq("id", source_id).execute()
    logger.info(f"Updated source {source_id}: status={status}")
