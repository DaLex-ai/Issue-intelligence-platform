"""Atomic per-task state transitions, also providing the idempotency boundary.

The message is acknowledged only after a committed SUCCEEDED/FAILED state.
The task UUID is the unique storage key; row locks serialize duplicate deliveries.
"""

from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID

from sqlalchemy import DateTime, Float, Integer, String, Text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TaskState(StrEnum):
    PUBLISHING = "PUBLISHING"
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    REJECTED = "REJECTED"


class Task(Base):
    __tablename__ = "inference_tasks"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    state: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    prediction: Mapped[str | None] = mapped_column(Text, nullable=True)
    model_name: Mapped[str | None] = mapped_column(String(255))
    model_alias: Mapped[str | None] = mapped_column(String(255))
    model_version: Mapped[str | None] = mapped_column(String(80))
    inference_seconds: Mapped[float | None] = mapped_column(Float)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )


def as_dict(task: Task) -> dict:
    return {
        "task_id": task.id,
        "status": task.state,
        "prediction": task.prediction,
        "model_name": task.model_name,
        "model_alias": task.model_alias,
        "model_version": task.model_version,
        "attempts": task.attempts,
        "inference_seconds": task.inference_seconds,
        "error": task.last_error,
    }


class TaskStore:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]):
        self.sessions = sessions

    async def get(self, task_id: UUID | str) -> dict | None:
        async with self.sessions() as session:
            obj = await session.get(Task, str(task_id))
            return as_dict(obj) if obj is not None else None

    async def stage(self, task_id: UUID, summary: str, description: str) -> bool:
        """Reserve a stable ID; allow recovery of abandoned batch attempts.

        Returns False for an already accepted/completed task. API uses fresh UUIDs.
        """
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if obj is not None:
                if obj.summary != summary or obj.description != description:
                    raise ValueError("task ID reused with different payload")
                if obj.state in {
                    TaskState.PENDING,
                    TaskState.PROCESSING,
                    TaskState.SUCCEEDED,
                    TaskState.FAILED,
                }:
                    return False
                obj.state = TaskState.PUBLISHING
                obj.last_error = None
            else:
                session.add(
                    Task(
                        id=str(task_id),
                        summary=summary,
                        description=description,
                        state=TaskState.PUBLISHING,
                    )
                )
        return True

    async def published(self, task_id: UUID) -> None:
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if obj is not None and obj.state == TaskState.PUBLISHING:
                obj.state = TaskState.PENDING

    async def rejected(self, task_id: UUID, error: str) -> bool:
        """Cancellation wins over an in-flight result unless already terminal.

        Worker checks state again before committing a result.
        """
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if obj is None or obj.state in {TaskState.SUCCEEDED, TaskState.FAILED}:
                return False
            obj.state = TaskState.REJECTED
            obj.last_error = error[:2000]
            return True

    async def claim(self, task_id: UUID, max_attempts: int) -> str:
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if obj is None:
                return "unknown"
            if obj.state in {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.REJECTED}:
                return "terminal"
            if obj.state == TaskState.PROCESSING:
                # A duplicate delivery is not permitted to steal an active claim.
                return "busy"
            if obj.attempts >= max_attempts:
                obj.state = TaskState.FAILED
                obj.last_error = "max attempts exhausted before redelivery"
                return "exhausted"
            obj.state = TaskState.PROCESSING
            obj.attempts += 1
            return "claimed"

    async def recover(self, task_id: UUID) -> None:
        """Used only on broker redelivery after a consumer was killed.

        Unique processing lives in a single task row. A redelivery cannot
        occur while the original live consumer still owns the same delivery.
        """
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if obj is not None and obj.state == TaskState.PROCESSING:
                obj.state = TaskState.PENDING

    async def succeed(
        self,
        task_id: UUID,
        prediction: str,
        model_name: str,
        model_alias: str,
        model_version: str,
        inference_seconds: float,
    ) -> bool:
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if obj is None or obj.state != TaskState.PROCESSING:
                return False
            obj.state = TaskState.SUCCEEDED
            obj.prediction = prediction
            obj.model_name = model_name
            obj.model_alias = model_alias
            obj.model_version = model_version
            obj.inference_seconds = inference_seconds
            obj.last_error = None
            return True

    async def fail(self, task_id: UUID, error: str, max_attempts: int) -> bool:
        """Returns True when all attempts have been consumed -> dead-letter."""
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if obj is None or obj.state != TaskState.PROCESSING:
                return False
            obj.last_error = error[:2000]
            exhausted = obj.attempts >= max_attempts
            obj.state = TaskState.FAILED if exhausted else TaskState.PENDING
            return exhausted
