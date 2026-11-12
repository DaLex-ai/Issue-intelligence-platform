"""Transactional state transitions; the attempt number fences stale workers."""

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import NamedTuple
from uuid import UUID

from sqlalchemy import DateTime, Float, Integer, String, Text, update
from sqlalchemy.exc import IntegrityError
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


class PublicationInProgress(Exception):
    """Another publisher still owns this task's admission."""


class Claim(NamedTuple):
    decision: str
    attempt: int = 0
    summary: str = ""
    description: str = ""
    created_at: datetime | None = None


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

    async def expire_publications(self, timeout: float) -> int:
        """Cancel abandoned admissions, never republish an uncertain message."""
        cutoff = datetime.now(UTC) - timedelta(seconds=timeout)
        async with self.sessions.begin() as session:
            result = await session.execute(
                update(Task)
                .where(Task.state == TaskState.PUBLISHING, Task.updated_at < cutoff)
                .values(
                    state=TaskState.REJECTED,
                    last_error="publication expired before admission completed",
                    updated_at=datetime.now(UTC),
                )
            )
            return result.rowcount

    async def stage(
        self, task_id: UUID, summary: str, description: str
    ) -> datetime | None:
        # A missing row cannot be locked. Resolve a concurrent INSERT using the
        # primary-key constraint, then inspect the winner in a new transaction.
        try:
            return await self._stage(task_id, summary, description)
        except IntegrityError:
            async with self.sessions() as session:
                if await session.get(Task, str(task_id)) is None:
                    raise
            return await self._stage(task_id, summary, description)

    async def _stage(
        self, task_id: UUID, summary: str, description: str
    ) -> datetime | None:
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if obj is not None:
                if obj.summary != summary or obj.description != description:
                    raise ValueError("task ID reused with different payload")
                if obj.state == TaskState.PUBLISHING:
                    raise PublicationInProgress("admission is still in progress")
                if obj.state != TaskState.REJECTED:
                    return None
                obj.state = TaskState.PUBLISHING
                obj.last_error = None
            token = datetime.now(UTC)
            if obj is None:
                session.add(
                    Task(
                        id=str(task_id),
                        summary=summary,
                        description=description,
                        state=TaskState.PUBLISHING,
                        updated_at=token,
                    )
                )
            else:
                token = max(
                    token,
                    obj.updated_at.replace(tzinfo=UTC) + timedelta(microseconds=1),
                )
                obj.updated_at = token
        return token

    async def published(self, task_id: UUID, *, publication_token: datetime) -> bool:
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if (
                obj is None
                or obj.state != TaskState.PUBLISHING
                or obj.updated_at.replace(tzinfo=UTC) != publication_token
            ):
                return False
            obj.state = TaskState.PENDING
            return True

    async def rejected(
        self, task_id: UUID, error: str, *, publication_token: datetime
    ) -> bool:
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if (
                obj is None
                or obj.state != TaskState.PUBLISHING
                or obj.updated_at.replace(tzinfo=UTC) != publication_token
            ):
                return False
            obj.state = TaskState.REJECTED
            obj.last_error = error[:2000]
            return True

    async def claim(
        self,
        task_id: UUID,
        max_attempts: int,
        *,
        redelivered: bool = False,
        expected_payload: tuple[str, str] | None = None,
        lease_seconds: float = 30.0,
    ) -> Claim:
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if obj is None:
                return Claim("unknown")
            if expected_payload is not None and expected_payload != (
                obj.summary,
                obj.description,
            ):
                return Claim("invalid")
            if obj.state == TaskState.FAILED:
                return Claim("failed", obj.attempts)
            if obj.state in {TaskState.SUCCEEDED, TaskState.REJECTED}:
                return Claim("terminal")
            if obj.state == TaskState.PUBLISHING:
                return Claim("publishing")
            if obj.state == TaskState.PROCESSING:
                expired = obj.updated_at.replace(tzinfo=UTC) < (
                    datetime.now(UTC) - timedelta(seconds=lease_seconds)
                )
                if not redelivered or not expired:
                    return Claim("busy")
            if obj.attempts >= max_attempts:
                obj.state = TaskState.FAILED
                obj.last_error = "max attempts exhausted before redelivery"
                return Claim("failed", obj.attempts)
            # Recovery and acquisition share one transaction. Never reset this
            # counter: it is also the fencing token checked on every completion.
            obj.state = TaskState.PROCESSING
            obj.attempts += 1
            return Claim(
                "claimed", obj.attempts, obj.summary, obj.description, obj.created_at
            )

    async def renew(self, task_id: UUID, attempt: int) -> bool:
        async with self.sessions.begin() as session:
            result = await session.execute(
                update(Task)
                .where(
                    Task.id == str(task_id),
                    Task.state == TaskState.PROCESSING,
                    Task.attempts == attempt,
                )
                .values(updated_at=datetime.now(UTC))
            )
            return result.rowcount == 1

    async def succeed(
        self,
        task_id: UUID,
        prediction: str,
        model_name: str,
        model_alias: str,
        model_version: str,
        inference_seconds: float,
        *,
        attempt: int,
    ) -> bool:
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if (
                obj is None
                or obj.state != TaskState.PROCESSING
                or obj.attempts != attempt
            ):
                return False
            obj.state = TaskState.SUCCEEDED
            obj.prediction = prediction
            obj.model_name = model_name
            obj.model_alias = model_alias
            obj.model_version = model_version
            obj.inference_seconds = inference_seconds
            obj.last_error = None
            return True

    async def fail(
        self,
        task_id: UUID,
        error: str,
        max_attempts: int,
        *,
        attempt: int,
    ) -> bool:
        """A stale owner cannot fail or roll back a newer attempt."""
        async with self.sessions.begin() as session:
            obj = await session.get(Task, str(task_id), with_for_update=True)
            if (
                obj is None
                or obj.state != TaskState.PROCESSING
                or obj.attempts != attempt
            ):
                return False
            obj.last_error = error[:2000]
            exhausted = obj.attempts >= max_attempts
            obj.state = TaskState.FAILED if exhausted else TaskState.PENDING
            return exhausted
