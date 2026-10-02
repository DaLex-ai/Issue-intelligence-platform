from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from issue_intelligence_platform.core.config import settings
from issue_intelligence_platform.ml.model_service import model_service

router = APIRouter(tags=["inference"])


class ProcessRequest(BaseModel):
    summary: str = Field(min_length=1)
    description: str = ""


class ProcessResponse(BaseModel):
    prediction: str
    model_name: str
    model_version: str
    model_alias: str


@router.post("/process", response_model=ProcessResponse)
async def process(request: ProcessRequest) -> ProcessResponse:
    prediction = await run_in_threadpool(
        model_service.predict,
        summary=request.summary,
        description=request.description,
    )

    return ProcessResponse(
        prediction=prediction,
        model_name=settings.mlflow_model_name,
        model_version=model_service.model_version or "unknown",
        model_alias=settings.mlflow_model_alias,
    )
