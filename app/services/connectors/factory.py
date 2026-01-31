"""Connector factory for creating connector instances"""

from typing import Dict, Any
from .base import BaseConnector
from .postgresql import PostgreSQLConnector
from .mysql import MySQLConnector

class ConnectorFactory:
    """Factory for creating connector instances"""

    # Map of connector types to classes
    CONNECTOR_TYPES = {
        "postgresql": PostgreSQLConnector,
        "mysql": MySQLConnector,
        # Will add more: bigquery, snowflake, google_sheets, stripe, etc.
    }

    @classmethod
    def create_connector(
        cls,
        connector_type: str,
        credentials: Dict[str, Any]
    ) -> BaseConnector:
        """
        Create connector instance

        Args:
            connector_type: Type of connector (postgresql, mysql, etc.)
            credentials: Credential dictionary

        Returns:
            Connector instance
        """
        connector_class = cls.CONNECTOR_TYPES.get(connector_type)

        if not connector_class:
            raise ValueError(
                f"Unknown connector type: {connector_type}. "
                f"Available: {list(cls.CONNECTOR_TYPES.keys())}"
            )

        return connector_class(credentials)

    @classmethod
    def list_connectors(cls) -> list:
        """List available connector types"""
        return list(cls.CONNECTOR_TYPES.keys())
