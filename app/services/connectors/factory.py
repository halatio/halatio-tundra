"""Connector factory for creating connector instances"""

from typing import Dict, Any
from .base import BaseConnector

# DuckDB-based connectors
from .postgres_duckdb import PostgresDuckDBConnector
from .mysql_duckdb import MySQLDuckDBConnector
from .sqlite_duckdb import SQLiteDuckDBConnector
from .mssql_duckdb import MSSQLDuckDBConnector

class ConnectorFactory:
    """Factory for creating connector instances"""

    # Map of connector types to classes
    CONNECTOR_TYPES = {
        # DuckDB-based connectors
        "postgresql": PostgresDuckDBConnector,
        "mysql": MySQLDuckDBConnector,
        "sqlite": SQLiteDuckDBConnector,
        "mssql": MSSQLDuckDBConnector,

        # Protocol aliases
        "mariadb": MySQLDuckDBConnector,  # MariaDB uses MySQL protocol
        "redshift": PostgresDuckDBConnector,  # Redshift uses PostgreSQL protocol
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
            connector_type: Type of connector (postgresql, mysql, sqlite, mssql, oracle, mariadb, redshift)
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
