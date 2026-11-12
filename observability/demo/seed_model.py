"""Seed and check a real MLflow run using a small offline synthetic CSV."""

import hashlib
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from mlflow import MlflowClient

from issue_intelligence_platform.ml import data, train
from issue_intelligence_platform.ml.registry import MODEL_NAME, register_run

if os.environ.get("IIP_QA_SEED") != "true":
    raise SystemExit("Use only in the disposable iip-observe-qa project")

with TemporaryDirectory() as folder:
    path = Path(folder) / "demo.csv"
    rows = []
    for i in range(40):
        words = "server crash timeout" if i % 2 else "documentation spelling typo"
        rows.append(
            {
                "Summary": f"{words} {i}",
                "Issue id": f"demo-{i}",
                "Status": "Closed",
                "Priority": "High" if i % 2 else "Low",
                "Resolution": "Fixed",
                "Created": "2026-01-01",
                "Resolved": "2026-01-02",
                "Description": words,
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)
    os.environ["IIP_DATASET_PATH"] = str(path)
    train.main()
    client = MlflowClient()
    experiment = client.get_experiment_by_name(train.EXPERIMENT_NAME)
    run = client.search_runs(
        [experiment.experiment_id],
        "tags.mlflow.runName = 'tfidf-logistic-regression'",
        max_results=1,
    )[0]
    manifest = json.loads(
        Path(
            client.download_artifacts(
                run.info.run_id,
                "data/split-manifest.json",
                folder,
            )
        ).read_text(encoding="utf-8")
    )
    prepared = data.prepare_dataset(data.load_raw_dataset())
    digest = hashlib.sha256(
        prepared[["text", "Priority"]]
        .to_json(orient="records", force_ascii=False)
        .encode()
    ).hexdigest()
    train_ids = manifest["train_indices"]
    test_ids = manifest["test_indices"]
    assert digest == manifest["prepared_sha256"]
    assert len(train_ids) == 32 and len(test_ids) == 8
    assert len(set(train_ids + test_ids)) == len(prepared)
    assert len(run.inputs.dataset_inputs) == 2
    version = register_run(run.info.run_id, "offline-qa-fixture")
    client.set_registered_model_alias(MODEL_NAME, "champion", version.version)
    print(
        json.dumps(
            {
                "run_id": run.info.run_id,
                "train": len(train_ids),
                "test": len(test_ids),
                "model_version": version.version,
            }
        )
    )
