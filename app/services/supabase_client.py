"""Supabase client for querying source metadata and updating status"""

import httpx
import logging
from typing import Any, Dict, Optional
from ..config import settings

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Minimal httpx-based PostgREST client for the Supabase sources table"""

    def __init__(self) -> None:
        self.base_url = settings.SUPABASE_URL.rstrip("/")
        self._headers = {
            "apikey": settings.SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {settings.SUPABASE_SERVICE_ROLE_KEY}",
            "Content-Type": "application/json",
        }

    async def get_source(self, source_id: str) -> Dict[str, Any]:
        """
        Fetch a source record from Supabase.

        Returns a dict with: id, organization_id, connector_type, source_type

        Raises:
            ValueError: if the source is not found
            httpx.HTTPStatusError: on HTTP errors
        """
        url = f"{self.base_url}/rest/v1/sources"
        params = {
            "id": f"eq.{source_id}",
            "select": "id,organization_id,connector_type,source_type",
        }
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url, headers=self._headers, params=params)
            response.raise_for_status()
            rows = response.json()

        if not rows:
            raise ValueError(f"Source not found: {source_id}")

        return rows[0]

    async def update_source(
        self,
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
        url = f"{self.base_url}/rest/v1/sources"
        params = {"id": f"eq.{source_id}"}
        payload: Dict[str, Any] = {"status": status}
        if row_count is not None:
            payload["row_count"] = row_count
        if file_size_bytes is not None:
            payload["file_size_bytes"] = file_size_bytes

        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.patch(
                url,
                headers={**self._headers, "Prefer": "return=minimal"},
                params=params,
                json=payload,
            )
            response.raise_for_status()

        logger.info(f"Updated source {source_id}: status={status}")


_client: Optional[SupabaseClient] = None


def get_supabase_client() -> SupabaseClient:
    """Return a module-level singleton SupabaseClient."""
    global _client
    if _client is None:
        _client = SupabaseClient()
    return _client
