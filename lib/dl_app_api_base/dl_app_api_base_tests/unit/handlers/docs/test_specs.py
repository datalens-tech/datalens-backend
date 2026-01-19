import os

import aiohttp
import pytest

import dl_app_api_base.app
import dl_json


SPEC_FILE_PATH = os.path.join(os.path.dirname(__file__), "expected_spec.json")


@pytest.mark.asyncio
async def test_default(
    app_settings: dl_app_api_base.app.HttpServerAppSettingsMixin,
) -> None:
    async with aiohttp.ClientSession() as session:
        response = await session.get(
            f"http://{app_settings.HTTP_SERVER.HOST}:{app_settings.HTTP_SERVER.PORT}/api/v1/docs/spec.json"
        )
        assert response.status == 200
        spec = await response.json()

        # with open(SPEC_FILE_PATH, "wb") as f:
        #     f.write(dl_json.dumps_bytes(spec))
        with open(SPEC_FILE_PATH, "rb") as f:
            raw_expected_spec = f.read()
            expected_spec = dl_json.loads_bytes(raw_expected_spec)

        assert spec == expected_spec
