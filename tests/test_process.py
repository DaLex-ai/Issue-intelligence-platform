import pandas as pd
import pytest

from issue_intelligence_platform.main import app, lifespan
from issue_intelligence_platform.ml.model_service import model_service


async def test_process_returns_prediction(client, monkeypatch) -> None:
    def fake_predict(*, summary: str, description: str) -> str:
        assert summary == "Node fails"
        assert description == "Startup error"
        return "High"

    monkeypatch.setattr(model_service, "predict", fake_predict)
    monkeypatch.setattr(model_service, "model_version", "7")

    response = await client.post(
        "/process",
        json={
            "summary": "Node fails",
            "description": "Startup error",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "prediction": "High",
        "model_name": "issue-priority-classifier",
        "model_version": "7",
        "model_alias": "champion",
    }


async def test_process_rejects_empty_summary(client) -> None:
    response = await client.post(
        "/process",
        json={
            "summary": "",
            "description": "Startup error",
        },
    )

    assert response.status_code == 422


async def test_process_does_not_reload_model_per_request(
    client,
    monkeypatch,
) -> None:
    predict_calls = 0

    def fake_predict(*, summary: str, description: str) -> str:
        nonlocal predict_calls
        predict_calls += 1
        return "Normal"

    def forbidden_load() -> None:
        raise AssertionError("Model must not be loaded during /process")

    monkeypatch.setattr(model_service, "predict", fake_predict)
    monkeypatch.setattr(model_service, "load", forbidden_load)
    monkeypatch.setattr(model_service, "model_version", "1")

    payload = {
        "summary": "Test issue",
        "description": "Test description",
    }

    first = await client.post("/process", json=payload)
    second = await client.post("/process", json=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    assert predict_calls == 2


async def test_lifespan_loads_model_once(monkeypatch) -> None:
    load_calls = 0

    def fake_load() -> None:
        nonlocal load_calls
        load_calls += 1

    monkeypatch.setattr(model_service, "load", fake_load)

    async with lifespan(app):
        assert load_calls == 1

    assert load_calls == 1


async def test_startup_fails_when_model_cannot_be_loaded(
    monkeypatch,
) -> None:
    def failing_load() -> None:
        raise RuntimeError("MLflow unavailable")

    monkeypatch.setattr(model_service, "load", failing_load)

    with pytest.raises(RuntimeError, match="MLflow unavailable"):
        async with lifespan(app):
            pass


def test_model_service_predict_uses_loaded_model() -> None:
    class FakeModel:
        def predict(self, data: pd.DataFrame):
            assert list(data.columns) == ["text"]
            assert data.iloc[0]["text"] == "Node fails\nStartup error"
            return ["Urgent"]

    previous_model = model_service.model

    try:
        model_service.model = FakeModel()

        prediction = model_service.predict(
            summary="Node fails",
            description="Startup error",
        )

        assert prediction == "Urgent"
    finally:
        model_service.model = previous_model


def test_model_service_predict_fails_when_model_is_not_loaded() -> None:
    previous_model = model_service.model

    try:
        model_service.model = None

        with pytest.raises(RuntimeError, match="Model is not loaded"):
            model_service.predict(
                summary="Node fails",
                description="Startup error",
            )
    finally:
        model_service.model = previous_model
