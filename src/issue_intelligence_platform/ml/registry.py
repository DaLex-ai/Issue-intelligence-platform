from __future__ import annotations

import argparse

import mlflow
from mlflow import MlflowClient

TRACKING_URI = "http://localhost:5000"
MODEL_NAME = "issue-priority-classifier"


def register_run(run_id: str, candidate: str):
    model_version = mlflow.register_model(
        model_uri=f"runs:/{run_id}/model",
        name=MODEL_NAME,
        tags={"candidate": candidate},
    )

    print(f"Registered {candidate}: {MODEL_NAME} version {model_version.version}")

    return model_version


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--logistic-run-id",
        required=True,
    )
    parser.add_argument(
        "--svc-run-id",
        required=True,
    )

    args = parser.parse_args()

    mlflow.set_tracking_uri(TRACKING_URI)

    logistic_version = register_run(
        args.logistic_run_id,
        "tfidf-logistic-regression",
    )

    svc_version = register_run(
        args.svc_run_id,
        "tfidf-linear-svc",
    )

    client = MlflowClient()

    client.set_registered_model_alias(
        MODEL_NAME,
        "champion",
        logistic_version.version,
    )

    print(f"Alias champion -> version {logistic_version.version}")
    print(f"SVC candidate -> version {svc_version.version}")


if __name__ == "__main__":
    main()
