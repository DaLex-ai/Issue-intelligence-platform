import issue_intelligence_platform.api.routes.health as health_module


class FakeResult:
    def scalar_one(self) -> str:
        return "16.4"


class FakeConnection:
    async def execute(self, _query):
        return FakeResult()


class FakeHealthyConnectionContext:
    async def __aenter__(self):
        return FakeConnection()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeHealthyEngine:
    def connect(self):
        return FakeHealthyConnectionContext()


class FakeFailingConnectionContext:
    async def __aenter__(self):
        raise OSError("database unavailable")

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakeFailingEngine:
    def connect(self):
        return FakeFailingConnectionContext()


async def test_healthz_returns_ok(client) -> None:
    response = await client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_health_returns_ok_when_database_is_available(
    client,
    monkeypatch,
) -> None:
    monkeypatch.setattr(health_module, "engine", FakeHealthyEngine())

    response = await client.get("/api/v1/health")
    data = response.json()

    assert response.status_code == 200
    assert data["status"] == "ok"
    assert data["components"]["postgresql"]["status"] == "ok"
    assert data["components"]["postgresql"]["version"] == "16.4"
    assert isinstance(data["components"]["postgresql"]["response_time_ms"], float)


async def test_health_returns_503_when_database_is_unavailable(
    client,
    monkeypatch,
) -> None:
    monkeypatch.setattr(health_module, "engine", FakeFailingEngine())

    response = await client.get("/api/v1/health")
    data = response.json()

    assert response.status_code == 503
    assert data["status"] == "unhealthy"
    assert data["components"]["postgresql"]["status"] == "unavailable"
    assert data["components"]["postgresql"]["version"] is None
    assert isinstance(data["components"]["postgresql"]["response_time_ms"], float)


async def test_readiness_requires_broker_and_database(client, monkeypatch):
    from types import SimpleNamespace

    from issue_intelligence_platform.main import app

    monkeypatch.setattr(health_module, "engine", FakeHealthyEngine())
    monkeypatch.setattr(
        app.state, "publisher", SimpleNamespace(ready=lambda: True), raising=False
    )
    assert (await client.get("/readyz")).status_code == 200
    monkeypatch.setattr(app.state, "publisher", SimpleNamespace(ready=lambda: False))
    assert (await client.get("/readyz")).status_code == 503
    monkeypatch.setattr(app.state, "publisher", SimpleNamespace(ready=lambda: True))
    monkeypatch.setattr(health_module, "engine", FakeFailingEngine())
    assert (await client.get("/readyz")).status_code == 503
