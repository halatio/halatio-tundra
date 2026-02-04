"""MySQL connector using DuckDB native mysql scanner"""

import duckdb
import logging
from typing import Dict, Any
from .duckdb_base import DuckDBBaseConnector
import anyio

logger = logging.getLogger(__name__)


class MySQLDuckDBConnector(DuckDBBaseConnector):
    """MySQL connector using DuckDB's native mysql extension"""

    def __init__(self, credentials: Dict[str, Any]):
        super().__init__(credentials)
        self.connection_string = self._build_connection_string()

    def _build_connection_string(self) -> str:
        """Build MySQL connection string"""
        return (
            f"mysql://{self.credentials['username']}:{self.credentials['password']}"
            f"@{self.credentials['host']}:{self.credentials.get('port', 3306)}"
            f"/{self.credentials['database']}"
        )

    def _get_db_alias(self) -> str:
        """Get the database alias used in queries"""
        return "mysql"

    def _attach_database(self, conn: duckdb.DuckDBPyConnection) -> str:
        """
        Attach MySQL database using native scanner

        Returns:
            Database alias 'mysql'
        """
        # Install and load mysql extension
        conn.execute("INSTALL mysql")
        conn.execute("LOAD mysql")

        # Attach MySQL database
        attach_query = f"""
            ATTACH '{self.connection_string}' AS mysql (TYPE mysql)
        """
        conn.execute(attach_query)

        logger.info(f"Attached MySQL database as 'mysql'")
        return "mysql"

    async def test_connection(self) -> Dict[str, Any]:
        """Test MySQL connection"""
        try:
            def _test_sync():
                conn = duckdb.connect()
                try:
                    self._configure_duckdb(conn)
                    conn.execute("INSTALL mysql")
                    conn.execute("LOAD mysql")
                    conn.execute(f"ATTACH '{self.connection_string}' AS mysql (TYPE mysql)")

                    # Test query
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
                    "database_type": "MySQL (DuckDB)",
                    "engine": "duckdb-mysql-scanner",
                    "test_rows": rows
                }
            }

        except Exception as e:
            logger.error(f"MySQL connection test failed: {str(e)}")
            return {
                "success": False,
                "error": "connection_failed",
                "message": str(e)
            }
