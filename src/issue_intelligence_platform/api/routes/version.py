from fastapi import APIRouter

from issue_intelligence_platform import __version__

router = APIRouter(prefix="/api/v1", tags=["version"])


@router.get("/version")
async def get_version() -> dict[str, str]:
    return {"version": __version__}
