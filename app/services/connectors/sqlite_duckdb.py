"""SQLite connector using DuckDB native support"""

import duckdb
import logging
from typing import Dict, Any
from .duckdb_base import DuckDBBaseConnector
import anyio

logger = logging.getLogger(__name__)


class SQLiteDuckDBConnector(DuckDBBaseConnector):
    """SQLite connector using DuckDB's native SQLite support"""

    def __init__(self, credentials: Dict[str, Any]):
        super().__init__(credentials)
        self.database_path = credentials.get('database') or credentials.get('file_path')
        if not self.database_path:
            raise ValueError("SQLite requires 'database' or 'file_path' in credentials")

    def _get_db_alias(self) -> str:
        """Get the database alias used in queries"""
        return "sqlite"

    def _attach_database(self, conn: duckdb.DuckDBPyConnection) -> str:
        """
        Attach SQLite database using native support

        Returns:
            Database alias 'sqlite'
        """
        # SQLite is natively supported, just attach the database file
        attach_query = f"""
            ATTACH '{self.database_path}' AS sqlite (TYPE sqlite)
        """
        conn.execute(attach_query)

        logger.info(f"Attached SQLite database from '{self.database_path}' as 'sqlite'")
        return "sqlite"

    async def test_connection(self) -> Dict[str, Any]:
        """Test SQLite connection"""
        try:
            def _test_sync():
                conn = duckdb.connect()
                try:
                    self._configure_duckdb(conn)
                    conn.execute(f"ATTACH '{self.database_path}' AS sqlite (TYPE sqlite)")

                    # Test query - get list of tables
                    result = conn.execute("SELECT COUNT(*) as table_count FROM sqlite.sqlite_master WHERE type='table'")
                    table_count = result.fetchone()[0]

                    return table_count
                finally:
                    conn.close()

            table_count = await anyio.to_thread.run_sync(_test_sync)

            return {
                "success": True,
                "message": "Connection successful",
                "metadata": {
                    "database_type": "SQLite (DuckDB)",
                    "engine": "duckdb-native",
                    "table_count": table_count,
                    "database_path": self.database_path
                }
            }

        except Exception as e:
            logger.error(f"SQLite connection test failed: {str(e)}")
            return {
                "success": False,
                "error": "connection_failed",
                "message": str(e)
            }
