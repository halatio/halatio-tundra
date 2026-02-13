import os
import sys
import types

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from app.models.conversionRequest import DatabaseConversionRequest
from app.utils import ValidationError


os.environ.setdefault("ENV", "test")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("R2_ACCESS_KEY_ID", "test-key")
os.environ.setdefault("R2_SECRET_ACCESS_KEY", "test-secret")
os.environ.setdefault("CLOUDFLARE_ACCOUNT_ID", "test-account")
os.environ.setdefault("SUPABASE_URL", "https://example.supabase.co")
os.environ.setdefault("SUPABASE_SECRET_KEY", "test-supabase-key")


if "duckdb" not in sys.modules:
    class _DummyConn:
        def execute(self, *args, **kwargs):
            return None

        def close(self):
            return None

    sys.modules["duckdb"] = types.SimpleNamespace(
        connect=lambda *args, **kwargs: _DummyConn(),
        DuckDBPyConnection=object,
    )



if "supabase" not in sys.modules:
    class _DummyClientOptions:
        def __init__(self, *args, **kwargs):
            pass

    sys.modules["supabase"] = types.SimpleNamespace(
        create_client=lambda *args, **kwargs: object(),
        Client=object,
        ClientOptions=_DummyClientOptions,
    )

# Stub google.cloud.secretmanager if not installed
if "google" not in sys.modules:
    _google = types.ModuleType("google")
    _google_cloud = types.ModuleType("google.cloud")
    _google_cloud_sm = types.ModuleType("google.cloud.secretmanager")
    _google_cloud_sm.SecretManagerServiceClient = type("SecretManagerServiceClient", (), {"__init__": lambda self: None})
    _google.cloud = _google_cloud
    _google_cloud.secretmanager = _google_cloud_sm
    sys.modules["google"] = _google
    sys.modules["google.cloud"] = _google_cloud
    sys.modules["google.cloud.secretmanager"] = _google_cloud_sm


from app import main as main_module


class _SecretManager:
    def get_credentials(self, credentials_id: str):
        return {"credentials_id": credentials_id}


class _Connector:
    def __init__(self, behavior):
        self._behavior = behavior

    async def extract_to_parquet(self, **kwargs):
        return await self._behavior(**kwargs)


@pytest.mark.anyio
async def test_convert_database_data_sets_active_status_on_success(monkeypatch):
    version_calls = []
    current_version_calls = []
    extraction_query_calls = []

    async def fake_get_source(source_id):
        return {
            "id": source_id,
            "organization_id": "org-1",
            "connector_type": "postgresql",
            "source_type": "database",
            "current_version": 0,
        }

    async def fake_create_source_version(source_id, version, status="pending"):
        return {"id": "sv-1", "source_id": source_id, "version": version, "status": status}

    async def fake_update_source_version(version_id, status, **kwargs):
        version_calls.append((version_id, status, kwargs))

    async def fake_update_source_current_version(source_id, version):
        current_version_calls.append((source_id, version))

    async def fake_update_source_extraction_query(source_id, query):
        extraction_query_calls.append((source_id, query))

    async def behavior(**kwargs):
        return {
            "rows": 12,
            "columns": 3,
            "file_size_mb": 1.5,
            "processing_time_seconds": 0.2,
            "query": "select 1",
        }

    monkeypatch.setattr(main_module, "get_source", fake_get_source)
    monkeypatch.setattr(main_module, "create_source_version", fake_create_source_version)
    monkeypatch.setattr(main_module, "update_source_version", fake_update_source_version)
    monkeypatch.setattr(main_module, "update_source_current_version", fake_update_source_current_version)
    monkeypatch.setattr(main_module, "update_source_extraction_query", fake_update_source_extraction_query)
    monkeypatch.setattr(main_module, "get_secret_manager", lambda: _SecretManager())
    monkeypatch.setattr(
        main_module.ConnectorFactory,
        "create_connector",
        lambda **kwargs: _Connector(behavior),
    )

    body = DatabaseConversionRequest(
        source_id="src-1",
        credentials_id="cred-1",
        query="select 1",
    )

    request = Request({"type": "http", "method": "POST", "path": "/convert/database", "headers": []})
    response = await main_module.convert_database_data(request=request, body=body)

    assert response.success is True
    assert response.metadata.version == 1

    # source_version updated to active with metrics
    assert version_calls == [
        ("sv-1", "active", {
            "row_count": 12,
            "column_count": 3,
            "file_size_bytes": 1572864,
            "processing_time_seconds": 0.2,
        })
    ]

    # sources.current_version incremented
    assert current_version_calls == [("src-1", 1)]

    # extraction_query stored
    assert extraction_query_calls == [("src-1", "select 1")]


@pytest.mark.anyio
async def test_convert_database_data_sets_error_status_on_validation_error(monkeypatch):
    version_calls = []

    async def fake_get_source(source_id):
        return {
            "id": source_id,
            "organization_id": "org-1",
            "connector_type": "postgresql",
            "source_type": "database",
            "current_version": 0,
        }

    async def fake_create_source_version(source_id, version, status="pending"):
        return {"id": "sv-2", "source_id": source_id, "version": version, "status": status}

    async def fake_update_source_version(version_id, status, **kwargs):
        version_calls.append((version_id, status, kwargs))

    async def fake_update_source_current_version(source_id, version):
        pass

    async def fake_update_source_extraction_query(source_id, query):
        pass

    async def behavior(**kwargs):
        raise ValidationError("Invalid query")

    monkeypatch.setattr(main_module, "get_source", fake_get_source)
    monkeypatch.setattr(main_module, "create_source_version", fake_create_source_version)
    monkeypatch.setattr(main_module, "update_source_version", fake_update_source_version)
    monkeypatch.setattr(main_module, "update_source_current_version", fake_update_source_current_version)
    monkeypatch.setattr(main_module, "update_source_extraction_query", fake_update_source_extraction_query)
    monkeypatch.setattr(main_module, "get_secret_manager", lambda: _SecretManager())
    monkeypatch.setattr(
        main_module.ConnectorFactory,
        "create_connector",
        lambda **kwargs: _Connector(behavior),
    )

    body = DatabaseConversionRequest(
        source_id="src-2",
        credentials_id="cred-1",
        query="select broken",
    )

    with pytest.raises(HTTPException) as excinfo:
        request = Request({"type": "http", "method": "POST", "path": "/convert/database", "headers": []})
        await main_module.convert_database_data(request=request, body=body)

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "Invalid query"
    assert version_calls == [("sv-2", "error", {"error_message": "Invalid query"})]


@pytest.mark.anyio
async def test_convert_database_data_sets_error_status_on_unexpected_exception(monkeypatch):
    version_calls = []

    async def fake_get_source(source_id):
        return {
            "id": source_id,
            "organization_id": "org-1",
            "connector_type": "postgresql",
            "source_type": "database",
            "current_version": 0,
        }

    async def fake_create_source_version(source_id, version, status="pending"):
        return {"id": "sv-3", "source_id": source_id, "version": version, "status": status}

    async def fake_update_source_version(version_id, status, **kwargs):
        version_calls.append((version_id, status, kwargs))

    async def fake_update_source_current_version(source_id, version):
        pass

    async def fake_update_source_extraction_query(source_id, query):
        pass

    async def behavior(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(main_module, "get_source", fake_get_source)
    monkeypatch.setattr(main_module, "create_source_version", fake_create_source_version)
    monkeypatch.setattr(main_module, "update_source_version", fake_update_source_version)
    monkeypatch.setattr(main_module, "update_source_current_version", fake_update_source_current_version)
    monkeypatch.setattr(main_module, "update_source_extraction_query", fake_update_source_extraction_query)
    monkeypatch.setattr(main_module, "get_secret_manager", lambda: _SecretManager())
    monkeypatch.setattr(
        main_module.ConnectorFactory,
        "create_connector",
        lambda **kwargs: _Connector(behavior),
    )

    body = DatabaseConversionRequest(
        source_id="src-3",
        credentials_id="cred-1",
        query="select 1",
    )

    with pytest.raises(HTTPException) as excinfo:
        request = Request({"type": "http", "method": "POST", "path": "/convert/database", "headers": []})
        await main_module.convert_database_data(request=request, body=body)

    assert excinfo.value.status_code == 500
    assert "Internal server error: boom" in excinfo.value.detail
    assert version_calls == [("sv-3", "error", {"error_message": "boom"})]
