from types import SimpleNamespace

import pytest

from issue_intelligence_platform.ml import registry


@pytest.mark.parametrize(
    "scores, expected", [((0.3, 0.4), "svc"), ((0.4, 0.3), "lr"), ((0.3, 0.3), "lr")]
)
def test_champion_is_selected_by_validated_macro_f1(monkeypatch, scores, expected):
    import sys

    calls = []

    class Client:
        def get_run(self, run_id):
            index = 0 if run_id == "lr" else 1
            return SimpleNamespace(
                info=SimpleNamespace(status="FINISHED"),
                data=SimpleNamespace(
                    metrics={"macro_f1": scores[index]},
                    params={"model": ("LogisticRegression", "LinearSVC")[index]},
                ),
            )

        def set_registered_model_alias(self, name, alias, version):
            calls.append((name, alias, version))

    monkeypatch.setattr(registry, "MlflowClient", Client)
    monkeypatch.setattr(registry.mlflow, "set_tracking_uri", lambda _: None)
    monkeypatch.setattr(
        registry, "register_run", lambda run_id, _: SimpleNamespace(version=run_id)
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "registry",
            "--logistic-run-id",
            "lr",
            "--svc-run-id",
            "svc",
        ],
    )
    registry.main()
    assert calls == [(registry.MODEL_NAME, "champion", expected)]


def test_registry_rejects_unfinished_or_wrong_run_before_registration(monkeypatch):
    import sys

    class Client:
        def get_run(self, _):
            return SimpleNamespace(
                info=SimpleNamespace(status="FAILED"),
                data=SimpleNamespace(
                    metrics={"macro_f1": 0.99}, params={"model": "LinearSVC"}
                ),
            )

    monkeypatch.setattr(registry, "MlflowClient", Client)
    monkeypatch.setattr(registry.mlflow, "set_tracking_uri", lambda _: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "registry",
            "--logistic-run-id",
            "lr",
            "--svc-run-id",
            "svc",
        ],
    )
    with pytest.raises(ValueError, match="Invalid completed"):
        registry.main()
