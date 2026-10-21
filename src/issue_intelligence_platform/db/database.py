from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from issue_intelligence_platform.core.config import settings
from issue_intelligence_platform.pipeline.store import Base

engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
)


session_factory = async_sessionmaker(engine, expire_on_commit=False)


async def initialize_schema() -> None:
    async with engine.begin() as connection:
        if connection.dialect.name == "postgresql":
            # Multiple worker replicas and API instances may boot concurrently.
            # Transaction-scoped lock serializes the create_all check + DDL.
            await connection.execute(text("SELECT pg_advisory_xact_lock(820004)"))
        await connection.run_sync(Base.metadata.create_all)
