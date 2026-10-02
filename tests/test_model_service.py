from types import SimpleNamespace

import issue_intelligence_platform.ml.model_service as model_service_module


def test_load_resolves_alias_and_loads_exact_version(monkeypatch) -> None:
    service = model_service_module.ModelService()

    class FakeClient:
        def get_model_version_by_alias(self, model_name: str, alias: str):
            assert model_name == model_service_module.settings.mlflow_model_name
            assert alias == model_service_module.settings.mlflow_model_alias

            return SimpleNamespace(version="7")

    fake_model = object()
    loaded = {}

    def fake_load_model(model_uri: str):
        loaded["model_uri"] = model_uri
        return fake_model

    monkeypatch.setattr(
        model_service_module,
        "MlflowClient",
        FakeClient,
    )
    monkeypatch.setattr(
        model_service_module.mlflow,
        "set_tracking_uri",
        lambda _uri: None,
    )
    monkeypatch.setattr(
        model_service_module.mlflow.pyfunc,
        "load_model",
        fake_load_model,
    )

    service.load()

    assert loaded["model_uri"] == "models:/issue-priority-classifier/7"
    assert service.model is fake_model
    assert service.model_version == "7"
