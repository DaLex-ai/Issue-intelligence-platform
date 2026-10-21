import asyncio

import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from issue_intelligence_platform.main import app


@pytest_asyncio.fixture
async def client() -> AsyncClient:
    app.state.admission_slots = asyncio.Queue(maxsize=4)
    for _ in range(4):
        app.state.admission_slots.put_nowait(None)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as async_client:
        yield async_client
