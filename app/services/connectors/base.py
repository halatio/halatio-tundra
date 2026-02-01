"""Base connector interface"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class BaseConnector(ABC):
    """Base class for all connectors"""

    def __init__(self, credentials: Dict[str, Any]):
        self.credentials = credentials

    @abstractmethod
    async def extract_to_parquet(
        self,
        output_path: str,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Extract data and write to parquet file

        Args:
            output_path: Local path to write parquet file
            **kwargs: Connector-specific extraction parameters

        Returns:
            Metadata dictionary with rows, columns, file_size_mb, etc.
        """
        pass

    @abstractmethod
    async def test_connection(self) -> Dict[str, Any]:
        """
        Test connection with current credentials

        Returns:
            Dictionary with success status and metadata
        """
        pass
