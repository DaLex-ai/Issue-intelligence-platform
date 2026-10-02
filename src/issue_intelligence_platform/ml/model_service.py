from __future__ import annotations

import mlflow
import pandas as pd
from mlflow import MlflowClient
from mlflow.pyfunc import PyFuncModel

from issue_intelligence_platform.core.config import settings


class ModelService:
    def __init__(self) -> None:
        self.model: PyFuncModel | None = None
        self.model_version: str | None = None

    def load(self) -> None:
        mlflow.set_tracking_uri(settings.mlflow_tracking_uri)

        client = MlflowClient()

        model_version = client.get_model_version_by_alias(
            settings.mlflow_model_name,
            settings.mlflow_model_alias,
        )

        model_uri = f"models:/{settings.mlflow_model_name}/{model_version.version}"

        self.model = mlflow.pyfunc.load_model(model_uri)
        self.model_version = str(model_version.version)

    def predict(
        self,
        *,
        summary: str,
        description: str,
    ) -> str:
        if self.model is None:
            raise RuntimeError("Model is not loaded")

        text = f"{summary.strip()}\n{description.strip()}"

        data = pd.DataFrame(
            {
                "text": [text],
            }
        )

        prediction = self.model.predict(data)

        return str(prediction[0])


model_service = ModelService()
