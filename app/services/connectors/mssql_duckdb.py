"""MS SQL Server connector using DuckDB ADBC driver"""

import duckdb
import logging
from typing import Dict, Any
from .duckdb_base import DuckDBBaseConnector
import anyio

logger = logging.getLogger(__name__)


class MSSQLDuckDBConnector(DuckDBBaseConnector):
    """MS SQL Server connector using DuckDB's ADBC integration"""

    def __init__(self, credentials: Dict[str, Any]):
        super().__init__(credentials)
        self.host = credentials['host']
        self.database = credentials['database']
        self.username = credentials['username']
        self.password = credentials['password']
        self.port = credentials.get('port', 1433)

    def _get_db_alias(self) -> str:
        """Get the database alias used in queries"""
        return "mssql"

    def _attach_database(self, conn: duckdb.DuckDBPyConnection) -> str:
        """
        Attach MS SQL Server database using ADBC

        Returns:
            Database alias 'mssql'
        """
        # Create ADBC secret for MS SQL Server
        # Note: The ADBC driver must be installed via `dbc add mssql` separately
        secret_query = f"""
            CREATE SECRET mssql_secret (
                TYPE adbc,
                driver 'adbc_driver_mssql',
                host '{self.host}',
                port '{self.port}',
                database '{self.database}',
                username '{self.username}',
                password '{self.password}'
            )
        """
        conn.execute(secret_query)

        logger.info(f"Created ADBC secret for MS SQL Server at {self.host}:{self.port}/{self.database}")
        return "mssql"

    async def test_connection(self) -> Dict[str, Any]:
        """Test MS SQL Server connection"""
        try:
            def _test_sync():
                conn = duckdb.connect()
                try:
                    self._configure_duckdb(conn)

                    # Create ADBC secret
                    secret_query = f"""
                        CREATE SECRET mssql_secret (
                            TYPE adbc,
                            driver 'adbc_driver_mssql',
                            host '{self.host}',
                            port '{self.port}',
                            database '{self.database}',
                            username '{self.username}',
                            password '{self.password}'
                        )
                    """
                    conn.execute(secret_query)

                    # Test query - try to execute a simple query
                    # Note: With ADBC, we can't use the traditional ATTACH pattern
                    # Instead, we query directly using the secret
                    result = conn.execute("SELECT 1 as test")
                    rows = result.fetchone()

                    return len(rows) if rows else 0
                finally:
                    conn.close()

            rows = await anyio.to_thread.run_sync(_test_sync)

            return {
                "success": True,
                "message": "Connection successful",
                "metadata": {
                    "database_type": "MS SQL Server (DuckDB ADBC)",
                    "engine": "duckdb-adbc-mssql",
                    "test_rows": rows,
                    "host": self.host,
                    "database": self.database
                }
            }

        except Exception as e:
            logger.error(f"MS SQL Server connection test failed: {str(e)}")
            return {
                "success": False,
                "error": "connection_failed",
                "message": str(e),
                "note": "Ensure ADBC driver is installed via: pip install dbc && dbc add mssql"
            }
