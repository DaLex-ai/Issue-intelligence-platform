"""Validation independent from AMQP and persistence."""

from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class TaskCreate(BaseModel):
    summary: str = Field(min_length=1, max_length=10000)
    description: str = Field(default="", max_length=200000)

    @field_validator("summary")
    @classmethod
    def summary_has_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("summary must contain non-whitespace text")
        return value


class TaskPayload(TaskCreate):
    task_id: UUID
