import sys
import types
import unittest
from urllib.parse import unquote, urlsplit

if "duckdb" not in sys.modules:
    sys.modules["duckdb"] = types.SimpleNamespace(
        DuckDBPyConnection=object,
        connect=lambda *args, **kwargs: None,
    )

if "google" not in sys.modules:
    sys.modules["google"] = types.ModuleType("google")
if "google.cloud" not in sys.modules:
    sys.modules["google.cloud"] = types.ModuleType("google.cloud")
if "google.cloud.secretmanager" not in sys.modules:
    mock_sm = types.ModuleType("google.cloud.secretmanager")
    mock_sm.SecretManagerServiceClient = object
    sys.modules["google.cloud.secretmanager"] = mock_sm

from app.services.connectors.mysql_duckdb import MySQLDuckDBConnector
from app.services.connectors.postgres_duckdb import PostgresDuckDBConnector
from app.services.secret_manager import SecretManagerService


class ConnectionStringEncodingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.credentials = {
            "username": "user:name+space",
            "password": "p@ss/w:rd?&=# +",
            "host": "db.example.com",
            "port": 5432,
            "database": "analytics",
        }

    def _assert_encoded_userinfo(self, conn_str: str, expected_username: str, expected_password: str) -> None:
        parsed = urlsplit(conn_str)
        self.assertIsNotNone(parsed.username)
        self.assertIsNotNone(parsed.password)
        self.assertEqual(unquote(parsed.username or ""), expected_username)
        self.assertEqual(unquote(parsed.password or ""), expected_password)

    def test_postgres_duckdb_connector_encodes_credentials(self) -> None:
        connector = PostgresDuckDBConnector(self.credentials)

        self.assertIn("user%3Aname%2Bspace", connector._conn_str)
        self.assertIn("p%40ss%2Fw%3Ard%3F%26%3D%23%20%2B", connector._conn_str)
        self._assert_encoded_userinfo(
            connector._conn_str,
            self.credentials["username"],
            self.credentials["password"],
        )

    def test_mysql_duckdb_connector_encodes_credentials(self) -> None:
        mysql_credentials = {**self.credentials, "port": 3306}
        connector = MySQLDuckDBConnector(mysql_credentials)

        self.assertIn("user%3Aname%2Bspace", connector._conn_str)
        self.assertIn("p%40ss%2Fw%3Ard%3F%26%3D%23%20%2B", connector._conn_str)
        self._assert_encoded_userinfo(
            connector._conn_str,
            mysql_credentials["username"],
            mysql_credentials["password"],
        )

    def test_secret_manager_connection_string_encodes_credentials(self) -> None:
        manager = SecretManagerService.__new__(SecretManagerService)

        postgres_conn_str = manager.build_connection_string("postgresql", self.credentials)
        mysql_conn_str = manager.build_connection_string(
            "mysql", {**self.credentials, "port": 3306}
        )

        self._assert_encoded_userinfo(
            postgres_conn_str,
            self.credentials["username"],
            self.credentials["password"],
        )
        self._assert_encoded_userinfo(
            mysql_conn_str,
            self.credentials["username"],
            self.credentials["password"],
        )


if __name__ == "__main__":
    unittest.main()
