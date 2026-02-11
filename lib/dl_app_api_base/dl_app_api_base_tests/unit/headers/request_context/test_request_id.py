import aiohttp
import pytest


@pytest.mark.asyncio
async def test_default(app_client: aiohttp.ClientSession) -> None:
    response = await app_client.get("/api/v1/headers")
    assert response.status == 200
    response_json = await response.json()

    assert response_json["request_id"] is not None
    assert response_json["child_request_id"].startswith(response_json["request_id"])


@pytest.mark.asyncio
async def test_request_id_passed(app_client: aiohttp.ClientSession) -> None:
    request_id = "test_request_id"

    response = await app_client.get(
        "/api/v1/headers",
        headers={"X-Request-ID": request_id},
    )
    assert response.status == 200
    response_json = await response.json()

    assert response_json["request_id"] == request_id
