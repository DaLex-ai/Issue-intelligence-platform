import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from issue_intelligence_platform.main import app


@pytest_asyncio.fixture
async def client() -> AsyncClient:
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as async_client:
        yield async_client
