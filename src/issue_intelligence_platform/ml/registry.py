from __future__ import annotations

import argparse
import math

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

    client = MlflowClient()
    candidates = [
        (args.logistic_run_id, "LogisticRegression"),
        (args.svc_run_id, "LinearSVC"),
    ]
    scores = []
    for run_id, expected_model in candidates:
        run = client.get_run(run_id)
        score = run.data.metrics.get("macro_f1")
        if (
            run.info.status != "FINISHED"
            or run.data.params.get("model") != expected_model
            or score is None
            or not math.isfinite(score)
            or not 0 <= score <= 1
        ):
            raise ValueError(f"Invalid completed {expected_model} run: {run_id}")
        scores.append(score)

    logistic_version = register_run(
        args.logistic_run_id,
        "tfidf-logistic-regression",
    )

    svc_version = register_run(
        args.svc_run_id,
        "tfidf-linear-svc",
    )

    champion = logistic_version if scores[0] >= scores[1] else svc_version
    client.set_registered_model_alias(
        MODEL_NAME,
        "champion",
        champion.version,
    )

    print(f"Alias champion -> version {champion.version}")
    print(f"SVC candidate -> version {svc_version.version}")


if __name__ == "__main__":
    main()
