from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import matplotlib.pyplot as plt
import mlflow
import mlflow.data

from issue_intelligence_platform.ml.data import (
    DATASET_URL,
    load_raw_dataset,
    prepare_dataset,
)

TRACKING_URI = "http://localhost:5000"
EXPERIMENT_NAME = "issue-priority-classification"


def main() -> None:
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    raw_df = load_raw_dataset()
    df = prepare_dataset(raw_df)

    dataset = mlflow.data.from_pandas(
        df,
        source=DATASET_URL,
        name="gitbugs-cassandra-priority",
        targets="Priority",
    )

    with mlflow.start_run(run_name="eda") as run:
        mlflow.log_input(dataset, context="eda")

        mlflow.log_params(
            {
                "raw_rows": len(raw_df),
                "prepared_rows": len(df),
                "target": "Priority",
                "features": "Summary + Description",
            }
        )

        priority_counts = df["Priority"].value_counts()

        fig, ax = plt.subplots(figsize=(8, 5))
        priority_counts.plot(kind="bar", ax=ax)
        ax.set_title("Priority distribution")
        ax.set_xlabel("Priority")
        ax.set_ylabel("Count")
        fig.tight_layout()

        mlflow.log_figure(
            fig,
            "eda/priority_distribution.png",
        )
        plt.close(fig)

        text_lengths = df["text"].str.len()

        fig, ax = plt.subplots(figsize=(8, 5))
        ax.hist(text_lengths, bins=50)
        ax.set_title("Text length distribution")
        ax.set_xlabel("Characters")
        ax.set_ylabel("Count")
        fig.tight_layout()

        mlflow.log_figure(
            fig,
            "eda/text_length_distribution.png",
        )
        plt.close(fig)

        missing_values = raw_df.isna().sum().sort_values(ascending=False)

        fig, ax = plt.subplots(figsize=(10, 5))
        missing_values.plot(kind="bar", ax=ax)
        ax.set_title("Missing values")
        ax.set_xlabel("Column")
        ax.set_ylabel("Missing count")
        fig.tight_layout()

        mlflow.log_figure(
            fig,
            "eda/missing_values.png",
        )
        plt.close(fig)

        summary = {
            "raw_rows": int(len(raw_df)),
            "prepared_rows": int(len(df)),
            "priority_distribution": {
                str(key): int(value) for key, value in priority_counts.items()
            },
            "missing_values": {
                str(key): int(value) for key, value in missing_values.items()
            },
            "mean_text_length": float(text_lengths.mean()),
            "median_text_length": float(text_lengths.median()),
            "max_text_length": int(text_lengths.max()),
        }

        with TemporaryDirectory() as tmp_dir:
            summary_path = Path(tmp_dir) / "eda_summary.json"
            summary_path.write_text(
                json.dumps(summary, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )

            mlflow.log_artifact(
                str(summary_path),
                artifact_path="eda",
            )

        print(f"Run ID: {run.info.run_id}")
        print(f"Dataset digest: {dataset.digest}")
        print(f"Dataset source: {dataset.source}")
        print(summary)


if __name__ == "__main__":
    main()
