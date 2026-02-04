"""Database and API connectors"""

from .factory import ConnectorFactory
from .base import BaseConnector

# DuckDB connectors
from .postgres_duckdb import PostgresDuckDBConnector
from .mysql_duckdb import MySQLDuckDBConnector
from .sqlite_duckdb import SQLiteDuckDBConnector
from .mssql_duckdb import MSSQLDuckDBConnector

# Legacy connectors
from .postgresql import PostgreSQLConnector
from .mysql import MySQLConnector
from .sqlite import SQLiteConnector
from .mssql import MSSQLConnector
from .oracle import OracleConnector

__all__ = [
    "ConnectorFactory",
    "BaseConnector",
    # DuckDB connectors
    "PostgresDuckDBConnector",
    "MySQLDuckDBConnector",
    "SQLiteDuckDBConnector",
    "MSSQLDuckDBConnector",
    # Legacy connectors
    "PostgreSQLConnector",
    "MySQLConnector",
    "SQLiteConnector",
    "MSSQLConnector",
    "OracleConnector",
]
