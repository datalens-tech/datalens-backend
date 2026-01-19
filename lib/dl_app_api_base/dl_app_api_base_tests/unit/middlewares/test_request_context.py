import aiohttp
import pytest

import dl_app_api_base.app
import dl_constants
import dl_utils


@pytest.mark.asyncio
async def test_handler_generates_request_id(
    app_settings: dl_app_api_base.app.HttpServerAppSettingsMixin,
) -> None:
    async with aiohttp.ClientSession() as session:
        response = await session.get(
            f"http://{app_settings.HTTP_SERVER.HOST}:{app_settings.HTTP_SERVER.PORT}/api/v1/health/liveness"
        )
        assert response.headers.get(dl_constants.DLHeadersCommon.REQUEST_ID.value) is not None


@pytest.mark.asyncio
async def test_handler_passes_request_id(
    app_settings: dl_app_api_base.app.HttpServerAppSettingsMixin,
) -> None:
    request_id = dl_utils.request_id_generator()

    async with aiohttp.ClientSession() as session:
        response = await session.get(
            f"http://{app_settings.HTTP_SERVER.HOST}:{app_settings.HTTP_SERVER.PORT}/api/v1/health/liveness",
            headers={
                dl_constants.DLHeadersCommon.REQUEST_ID.value: request_id,
            },
        )
        assert response.headers.get(dl_constants.DLHeadersCommon.REQUEST_ID.value) == request_id


@pytest.mark.asyncio
async def test_handler_passes_request_context(
    app_settings: dl_app_api_base.app.HttpServerAppSettingsMixin,
) -> None:
    async with aiohttp.ClientSession() as session:
        response = await session.get(
            f"http://{app_settings.HTTP_SERVER.HOST}:{app_settings.HTTP_SERVER.PORT}/api/v1/counter",
        )
        assert response.status == 200
        assert await response.json() == {
            "counter_value": 1,
            "value": 42,
        }

        response = await session.get(
            f"http://{app_settings.HTTP_SERVER.HOST}:{app_settings.HTTP_SERVER.PORT}/api/v1/counter",
        )
        assert response.status == 200
        assert await response.json() == {
            "counter_value": 2,
            "value": 42,
        }
