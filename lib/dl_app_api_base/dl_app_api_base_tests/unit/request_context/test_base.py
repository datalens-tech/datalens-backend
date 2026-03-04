import aiohttp
import pytest

import dl_app_api_base_tests.unit.conftest as test_conftest


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
    app_settings: test_conftest.AppSettings,
) -> None:
    response = await app_client.get("/api/v1/headers/test1?query_param=test2")
    assert response.status == 200
    response_json = await response.json()

    assert response_json["method"] == "GET"
    assert response_json["path"] == "/api/v1/headers/test1?query_param=test2"
    assert response_json["path_pattern"] == "/api/v1/headers/{path_param}"
    assert response_json["host"] == f"{app_settings.HTTP_SERVER.HOST}:{app_settings.HTTP_SERVER.PORT}"
