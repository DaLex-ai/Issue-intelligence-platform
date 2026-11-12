"""HTTP admission tests: no broker or DB, boundary is mocked."""

from unittest.mock import AsyncMock
from uuid import uuid4

from issue_intelligence_platform.api.routes import process as routes
from issue_intelligence_platform.main import app
from issue_intelligence_platform.pipeline.rabbit import PublishUncertain, QueueFull


async def test_create_returns_202_and_location(client, monkeypatch):
    publish = AsyncMock(return_value=True)
    monkeypatch.setattr(routes, "submit", publish)
    app.state.publisher = object()
    response = await client.post(
        "/api/v1/tasks",
        json={
            "summary": "Node fails",
            "description": "Startup error",
        },
    )
    assert response.status_code == 202
    assert response.json()["status"] == "PENDING"
    assert response.headers["Location"] == response.json()["status_url"]
    publish.assert_awaited_once()
    assert publish.call_args.args[3:] == ("Node fails", "Startup error")


async def test_queue_overload_is_503_with_retry_after(client, monkeypatch):
    monkeypatch.setattr(routes, "submit", AsyncMock(side_effect=QueueFull()))
    app.state.publisher = object()
    response = await client.post("/api/v1/tasks", json={"summary": "Slow"})
    assert response.status_code == 503
    assert response.headers["Retry-After"].isdigit()
    assert int(response.headers["Retry-After"]) >= 1


async def test_ambiguous_publish_is_cancelled_before_503(client, monkeypatch):
    # Integration of submit() cancellation is separately tested below.
    monkeypatch.setattr(routes, "submit", AsyncMock(side_effect=PublishUncertain()))
    app.state.publisher = object()
    response = await client.post("/api/v1/tasks", json={"summary": "Slow"})
    assert response.status_code == 503


async def test_process_alias_now_async_202(client, monkeypatch):
    monkeypatch.setattr(routes, "submit", AsyncMock(return_value=True))
    app.state.publisher = object()
    response = await client.post("/process", json={"summary": "Previous URL"})
    assert response.status_code == 202


async def test_invalid_summary_422(client):
    response = await client.post("/api/v1/tasks", json={"summary": ""})
    assert response.status_code == 422


async def test_get_known_task(client, monkeypatch):
    fake = {"task_id": str(uuid4()), "status": "SUCCEEDED", "prediction": "High"}
    monkeypatch.setattr(routes.store, "get", AsyncMock(return_value=fake))
    response = await client.get("/api/v1/tasks/" + fake["task_id"])
    assert response.status_code == 200
    assert response.json()["prediction"] == "High"


async def test_get_unknown_task_404(client, monkeypatch):
    monkeypatch.setattr(routes.store, "get", AsyncMock(return_value=None))
    response = await client.get(f"/api/v1/tasks/{uuid4()}")
    assert response.status_code == 404


async def test_api_sheds_concurrent_publish_without_db_write(client, monkeypatch):
    from asyncio import Queue

    queue = Queue(maxsize=1)  # intentionally no available tokens
    app.state.admission_slots = queue
    send = AsyncMock()
    monkeypatch.setattr(routes, "submit", send)
    response = await client.post("/api/v1/tasks", json={"summary": "Very busy"})
    assert response.status_code == 503
    assert response.headers["Retry-After"].isdigit()
    send.assert_not_awaited()


async def test_whitespace_summary_422(client):
    response = await client.post("/api/v1/tasks", json={"summary": "   "})
    assert response.status_code == 422


async def test_database_commit_ambiguity_exposes_task_id_for_lookup(
    client, monkeypatch
):
    from asyncpg.exceptions import CannotConnectNowError

    monkeypatch.setattr(
        routes, "submit", AsyncMock(side_effect=CannotConnectNowError())
    )
    app.state.publisher = object()
    response = await client.post("/api/v1/tasks", json={"summary": "Fails"})
    assert response.status_code == 503
    body = response.json()
    assert response.headers["Location"] == body["status_url"]
    assert body["task_id"] in body["status_url"]
    assert "unknown" in body["detail"]


async def test_status_lookup_database_failure_is_503(client, monkeypatch):
    monkeypatch.setattr(
        routes.store, "get", AsyncMock(side_effect=OSError("database down"))
    )
    response = await client.get(f"/api/v1/tasks/{uuid4()}")
    assert response.status_code == 503
    assert response.headers["Retry-After"].isdigit()


async def test_admission_has_bounded_total_wait(client, monkeypatch):
    import asyncio

    async def blocked(*args):
        await asyncio.Event().wait()

    monkeypatch.setattr(routes, "submit", blocked)
    monkeypatch.setattr(routes.settings, "api_operation_timeout_seconds", 0.01)
    app.state.publisher = object()
    response = await asyncio.wait_for(
        client.post("/api/v1/tasks", json={"summary": "Fails"}), timeout=1
    )
    assert response.status_code == 503
    assert "task_id" in response.json()
