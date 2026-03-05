import aiohttp
import pytest


@pytest.mark.asyncio
async def test_default(app_client: aiohttp.ClientSession) -> None:
    response = await app_client.get("/api/v1/headers/test")
    assert response.status == 200
    response_json = await response.json()

    assert response_json["trace_id"] is not None


@pytest.mark.asyncio
async def test_trace_id_passed(app_client: aiohttp.ClientSession) -> None:
    trace_id = "test_trace_id"

    response = await app_client.get(
        "/api/v1/headers/test",
        headers={"Uber-Trace-Id": trace_id},
    )
    assert response.status == 200
    response_json = await response.json()

    assert response_json["trace_id"] == trace_id
