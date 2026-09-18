from issue_intelligence_platform import __version__


async def test_get_version(client) -> None:
    response = await client.get("/api/v1/version")

    assert response.status_code == 200
    assert response.json() == {"version": __version__}
