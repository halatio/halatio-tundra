"""PostgreSQL connector using DuckDB native postgres scanner"""

import duckdb
import logging
from typing import Dict, Any
from .duckdb_base import DuckDBBaseConnector
import anyio

logger = logging.getLogger(__name__)


class PostgresDuckDBConnector(DuckDBBaseConnector):
    """PostgreSQL connector using DuckDB's native postgres extension"""

    def __init__(self, credentials: Dict[str, Any]):
        super().__init__(credentials)
        self.connection_string = self._build_connection_string()

    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string"""
        return (
            f"postgresql://{self.credentials['username']}:{self.credentials['password']}"
            f"@{self.credentials['host']}:{self.credentials.get('port', 5432)}"
            f"/{self.credentials['database']}"
        )

    def _get_db_alias(self) -> str:
        """Get the database alias used in queries"""
        return "pg"

    def _attach_database(self, conn: duckdb.DuckDBPyConnection) -> str:
        """
        Attach PostgreSQL database using native scanner

        Returns:
            Database alias 'pg'
        """
        # Install and load postgres extension
        conn.execute("INSTALL postgres")
        conn.execute("LOAD postgres")

        # Attach PostgreSQL database
        attach_query = f"""
            ATTACH '{self.connection_string}' AS pg (TYPE postgres)
        """
        conn.execute(attach_query)

        logger.info(f"Attached PostgreSQL database as 'pg'")
        return "pg"

    async def test_connection(self) -> Dict[str, Any]:
        """Test PostgreSQL connection"""
        try:
            def _test_sync():
                conn = duckdb.connect()
                try:
                    self._configure_duckdb(conn)
                    conn.execute("INSTALL postgres")
                    conn.execute("LOAD postgres")
                    conn.execute(f"ATTACH '{self.connection_string}' AS pg (TYPE postgres)")

                    # Test query
                    result = conn.execute("SELECT 1 as test FROM pg.information_schema.tables LIMIT 1")
                    rows = result.fetchone()

                    return len(rows) if rows else 0
                finally:
                    conn.close()

            rows = await anyio.to_thread.run_sync(_test_sync)

            return {
                "success": True,
                "message": "Connection successful",
                "metadata": {
                    "database_type": "PostgreSQL (DuckDB)",
                    "engine": "duckdb-postgres-scanner",
                    "test_rows": rows
                }
            }

        except Exception as e:
            logger.error(f"PostgreSQL connection test failed: {str(e)}")
            return {
                "success": False,
                "error": "connection_failed",
                "message": str(e)
            }
