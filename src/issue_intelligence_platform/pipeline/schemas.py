"""Validation independent from AMQP and persistence."""

from uuid import UUID

from pydantic import BaseModel, Field


class TaskPayload(BaseModel):
    task_id: UUID
    summary: str = Field(min_length=1, max_length=10000)
    description: str = Field(default="", max_length=200000)


class TaskCreate(BaseModel):
    summary: str = Field(min_length=1, max_length=10000)
    description: str = Field(default="", max_length=200000)
