"""MySQL connector using DuckDB native mysql scanner"""

import duckdb
import logging
from typing import Any, Dict
from urllib.parse import quote
from .duckdb_base import DuckDBBaseConnector, _make_duckdb_config
import anyio

logger = logging.getLogger(__name__)


class MySQLDuckDBConnector(DuckDBBaseConnector):
    """MySQL/MariaDB connector via DuckDB mysql extension"""

    def __init__(self, credentials: Dict[str, Any]) -> None:
        super().__init__(credentials)
        encoded_username = quote(credentials["username"], safe="")
        encoded_password = quote(credentials["password"], safe="")
        self._conn_str = (
            f"mysql://{encoded_username}:{encoded_password}"
            f"@{credentials['host']}:{credentials.get('port', 3306)}"
            f"/{credentials['database']}"
        )

    def _get_db_alias(self) -> str:
        return "mysql"

    def _attach_database(self, conn: duckdb.DuckDBPyConnection) -> str:
        conn.execute("LOAD mysql")
        conn.execute(f"ATTACH '{self._conn_str}' AS mysql (TYPE mysql)")
        logger.info("Attached MySQL database as 'mysql'")
        return "mysql"

    async def test_connection(self) -> Dict[str, Any]:
        try:
            def _test() -> bool:
                conn = duckdb.connect(":memory:", config=_make_duckdb_config())
                try:
                    conn.execute("SET autoinstall_known_extensions = false")
                    conn.execute("SET autoload_known_extensions = true")
                    conn.execute("LOAD mysql")
                    conn.execute(f"ATTACH '{self._conn_str}' AS mysql (TYPE mysql)")
                    conn.execute("SELECT 1")
                    return True
                finally:
                    conn.close()

            await anyio.to_thread.run_sync(_test)
            return {
                "success": True,
                "message": "Connection successful",
                "metadata": {
                    "database_type": "MySQL",
                    "engine": "duckdb-mysql-scanner",
                },
            }
        except Exception as e:
            logger.error(f"MySQL connection test failed: {e}")
            return {"success": False, "error": "connection_failed", "message": str(e)}
