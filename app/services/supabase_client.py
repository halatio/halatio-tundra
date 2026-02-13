"""Supabase client for querying source metadata and managing source versions"""

import logging
from typing import Any, Dict, Optional
from supabase import create_client, Client, ClientOptions
from ..config import settings

logger = logging.getLogger(__name__)

_client: Optional[Client] = None


def get_supabase_client() -> Client:
    """Return a module-level singleton Supabase client."""
    global _client
    if _client is None:
        _client = create_client(
            settings.SUPABASE_URL,
            settings.SUPABASE_SECRET_KEY,
            options=ClientOptions(auto_refresh_token=False, persist_session=False),
        )
    return _client


async def get_source(source_id: str) -> Dict[str, Any]:
    """
    Fetch a source record from the sources table.

    Returns a dict with: id, organization_id, connector_type, source_type, current_version

    Raises:
        ValueError: if the source is not found
    """
    client = get_supabase_client()
    response = (
        client.table("sources")
        .select("id, organization_id, connector_type, source_type, current_version")
        .eq("id", source_id)
        .single()
        .execute()
    )

    if not response.data:
        raise ValueError(f"Source not found: {source_id}")

    return response.data


async def create_source_version(
    source_id: str,
    version: int,
    status: str = "pending",
) -> Dict[str, Any]:
    """
    Create a new source_versions record.

    Returns the created record with id.
    """
    client = get_supabase_client()
    response = (
        client.table("source_versions")
        .insert({
            "source_id": source_id,
            "version": version,
            "status": status,
        })
        .execute()
    )

    record = response.data[0]
    logger.info(
        f"Created source_version {record['id']} for source {source_id} v{version}"
    )
    return record


async def update_source_version(
    version_id: str,
    status: str,
    row_count: Optional[int] = None,
    column_count: Optional[int] = None,
    file_size_bytes: Optional[int] = None,
    processing_time_seconds: Optional[float] = None,
    error_message: Optional[str] = None,
) -> None:
    """Update a source_versions record with status and metrics."""
    client = get_supabase_client()
    payload: Dict[str, Any] = {"status": status}
    if row_count is not None:
        payload["row_count"] = row_count
    if column_count is not None:
        payload["column_count"] = column_count
    if file_size_bytes is not None:
        payload["file_size_bytes"] = file_size_bytes
    if processing_time_seconds is not None:
        payload["processing_time_seconds"] = processing_time_seconds
    if error_message is not None:
        payload["error_message"] = error_message

    client.table("source_versions").update(payload).eq("id", version_id).execute()
    logger.info(f"Updated source_version {version_id}: status={status}")


async def update_source_current_version(
    source_id: str,
    version: int,
) -> None:
    """Update the sources.current_version field."""
    client = get_supabase_client()
    client.table("sources").update({"current_version": version}).eq(
        "id", source_id
    ).execute()
    logger.info(f"Updated source {source_id} current_version={version}")


async def update_source_extraction_query(
    source_id: str,
    query: str,
) -> None:
    """Store SQL query in sources.extraction_query field."""
    client = get_supabase_client()
    client.table("sources").update({"extraction_query": query}).eq(
        "id", source_id
    ).execute()
    logger.info(f"Updated source {source_id} extraction_query")
