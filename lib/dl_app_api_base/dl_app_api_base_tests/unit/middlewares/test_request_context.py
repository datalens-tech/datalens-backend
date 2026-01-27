import aiohttp
import pytest

import dl_constants
import dl_utils


@pytest.mark.asyncio
async def test_handler_request_id(
    app_client: aiohttp.ClientSession,
) -> None:
    request_id = dl_utils.request_id_generator()

    response = await app_client.get(
        "/api/v1/health/liveness",
        headers={
            dl_constants.DLHeadersCommon.REQUEST_ID.value: request_id,
        },
    )
    assert response.headers.get(dl_constants.DLHeadersCommon.REQUEST_ID.value) == request_id


@pytest.mark.asyncio
async def test_handler_passes_request_context(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get("/api/v1/counter")
    assert response.status == 200
    assert await response.json() == {
        "counter_value": 1,
        "value": 42,
    }

    response = await app_client.get("/api/v1/counter")
    assert response.status == 200
    assert await response.json() == {
        "counter_value": 2,
        "value": 42,
    }
