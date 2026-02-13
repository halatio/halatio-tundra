"""Connector factory for creating connector instances"""

import logging
from typing import Dict, Any
from .base import BaseConnector

# DuckDB-based connectors
from .postgres_duckdb import PostgresDuckDBConnector
from .mysql_duckdb import MySQLDuckDBConnector
from .sqlite_duckdb import SQLiteDuckDBConnector

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ADBC connector preference
#
# Always prefer the ADBC PostgreSQL connector for PostgreSQL/Redshift.
# Requires adbc-driver-postgresql >= 1.1.0.
# Falls back to the DuckDB connector if the package is not installed.
# ---------------------------------------------------------------------------

def _resolve_postgres_connector():
    """Return the PostgreSQL connector class, preferring ADBC with safe fallback."""
    try:
        import adbc_driver_postgresql  # noqa: F401 — availability check only
        from .postgres_adbc import PostgresADBCConnector
        logger.info(
            "Using ADBC PostgreSQL connector "
            "(adbc_driver_postgresql detected)"
        )
        return PostgresADBCConnector
    except ImportError:
        logger.warning(
            "adbc_driver_postgresql is not installed. "
            "Falling back to DuckDB postgres scanner. "
            "Install with: pip install adbc-driver-postgresql>=1.1.0"
        )
        return PostgresDuckDBConnector


class ConnectorFactory:
    """Factory for creating connector instances"""

    # Map of connector types to classes
    CONNECTOR_TYPES = {
        # DuckDB-based connectors
        "postgresql": None,  # resolved at runtime via _resolve_postgres_connector()
        "mysql": MySQLDuckDBConnector,
        "sqlite": SQLiteDuckDBConnector,

        # Protocol aliases
        "mariadb": MySQLDuckDBConnector,  # MariaDB uses MySQL protocol
        "redshift": None,  # resolved at runtime — same flag as postgresql
    }

    @classmethod
    def create_connector(
        cls,
        connector_type: str,
        credentials: Dict[str, Any]
    ) -> BaseConnector:
        """
        Create connector instance.

        For PostgreSQL and Redshift, the actual class is chosen at call time,
        preferring ADBC and falling back to DuckDB if unavailable.

        Args:
            connector_type: Type of connector (postgresql, mysql, sqlite, mariadb, redshift)
            credentials: Credential dictionary

        Returns:
            Connector instance
        """
        if connector_type not in cls.CONNECTOR_TYPES:
            raise ValueError(
                f"Unknown connector type: {connector_type}. "
                f"Available: {list(cls.CONNECTOR_TYPES.keys())}"
            )

        if connector_type in ("postgresql", "redshift"):
            connector_class = _resolve_postgres_connector()
        else:
            connector_class = cls.CONNECTOR_TYPES[connector_type]

        return connector_class(credentials)

    @classmethod
    def list_connectors(cls) -> list:
        """List available connector types"""
        return list(cls.CONNECTOR_TYPES.keys())
