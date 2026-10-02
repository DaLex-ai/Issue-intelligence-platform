from __future__ import annotations

import matplotlib.pyplot as plt
import mlflow
import mlflow.data
import mlflow.sklearn
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    accuracy_score,
    classification_report,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.svm import LinearSVC

from issue_intelligence_platform.ml.data import (
    DATASET_URL,
    load_raw_dataset,
    prepare_dataset,
)

TRACKING_URI = "http://localhost:5000"
EXPERIMENT_NAME = "issue-priority-classification"
RANDOM_STATE = 42


def make_text_pipeline(
    classifier,
    *,
    ngram_range: tuple[int, int],
    max_features: int,
) -> Pipeline:
    features = ColumnTransformer(
        transformers=[
            (
                "text",
                TfidfVectorizer(
                    lowercase=True,
                    min_df=2,
                    max_features=max_features,
                    ngram_range=ngram_range,
                    sublinear_tf=True,
                ),
                "text",
            )
        ]
    )

    return Pipeline(
        [
            ("features", features),
            ("classifier", classifier),
        ]
    )


def log_evaluation(model, x_test, y_test) -> dict[str, float]:
    predictions = model.predict(x_test)

    metrics = {
        "macro_f1": f1_score(
            y_test,
            predictions,
            average="macro",
            zero_division=0,
        ),
        "weighted_f1": f1_score(
            y_test,
            predictions,
            average="weighted",
            zero_division=0,
        ),
        "accuracy": accuracy_score(y_test, predictions),
        "precision_macro": precision_score(
            y_test,
            predictions,
            average="macro",
            zero_division=0,
        ),
        "recall_macro": recall_score(
            y_test,
            predictions,
            average="macro",
            zero_division=0,
        ),
    }

    mlflow.log_metrics(metrics)

    report = classification_report(
        y_test,
        predictions,
        output_dict=True,
        zero_division=0,
    )
    mlflow.log_dict(
        report,
        "diagnostics/classification_report.json",
    )

    fig, ax = plt.subplots(figsize=(8, 6))
    ConfusionMatrixDisplay.from_predictions(
        y_test,
        predictions,
        ax=ax,
        colorbar=False,
    )
    ax.set_title("Confusion matrix")
    fig.tight_layout()

    mlflow.log_figure(
        fig,
        "diagnostics/confusion_matrix.png",
    )
    plt.close(fig)

    input_example = x_test.head(5)
    signature = infer_signature(
        input_example,
        model.predict(input_example),
    )

    mlflow.sklearn.log_model(
        model,
        name="model",
        signature=signature,
        input_example=input_example,
    )

    return metrics


def run_model(
    *,
    run_name: str,
    model,
    dataset,
    x_train,
    x_test,
    y_train,
    y_test,
    params: dict,
) -> None:
    with mlflow.start_run(run_name=run_name):
        mlflow.log_input(dataset, context="training")
        mlflow.log_params(params)

        model.fit(x_train, y_train)

        metrics = log_evaluation(
            model,
            x_test,
            y_test,
        )

        print(f"\n{run_name}")
        for name, value in metrics.items():
            print(f"  {name}: {value:.4f}")


def main() -> None:
    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    raw_df = load_raw_dataset()
    df = prepare_dataset(raw_df)

    x = df[["text"]]
    y = df["Priority"]

    x_train, x_test, y_train, y_test = train_test_split(
        x,
        y,
        test_size=0.2,
        random_state=RANDOM_STATE,
        stratify=y,
    )

    dataset = mlflow.data.from_pandas(
        df[["text", "Priority"]],
        source=DATASET_URL,
        name="gitbugs-cassandra-priority",
        targets="Priority",
    )

    run_model(
        run_name="baseline-dummy",
        model=DummyClassifier(strategy="most_frequent"),
        dataset=dataset,
        x_train=x_train,
        x_test=x_test,
        y_train=y_train,
        y_test=y_test,
        params={
            "model": "DummyClassifier",
            "strategy": "most_frequent",
            "test_size": 0.2,
            "random_state": RANDOM_STATE,
        },
    )

    logistic_model = make_text_pipeline(
        LogisticRegression(
            max_iter=1000,
            class_weight="balanced",
            random_state=RANDOM_STATE,
        ),
        ngram_range=(1, 1),
        max_features=30000,
    )

    run_model(
        run_name="tfidf-logistic-regression",
        model=logistic_model,
        dataset=dataset,
        x_train=x_train,
        x_test=x_test,
        y_train=y_train,
        y_test=y_test,
        params={
            "model": "LogisticRegression",
            "class_weight": "balanced",
            "ngram_range": "1,1",
            "max_features": 30000,
            "test_size": 0.2,
            "random_state": RANDOM_STATE,
        },
    )

    svc_model = make_text_pipeline(
        LinearSVC(
            C=1.0,
            class_weight="balanced",
            random_state=RANDOM_STATE,
        ),
        ngram_range=(1, 2),
        max_features=50000,
    )

    run_model(
        run_name="tfidf-linear-svc",
        model=svc_model,
        dataset=dataset,
        x_train=x_train,
        x_test=x_test,
        y_train=y_train,
        y_test=y_test,
        params={
            "model": "LinearSVC",
            "C": 1.0,
            "class_weight": "balanced",
            "ngram_range": "1,2",
            "max_features": 50000,
            "test_size": 0.2,
            "random_state": RANDOM_STATE,
        },
    )


if __name__ == "__main__":
    main()
