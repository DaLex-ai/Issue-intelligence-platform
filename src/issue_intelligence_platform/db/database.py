from sqlalchemy.ext.asyncio import create_async_engine

from issue_intelligence_platform.core.config import settings

engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
)
