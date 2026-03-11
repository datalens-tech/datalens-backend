import os

import aiohttp
import pytest

import dl_app_api_base_tests.settings as test_settings
import dl_json


SPEC_FILE_PATH = os.path.join(os.path.dirname(__file__), "expected_spec.json")


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
    test_settings: test_settings.Settings,
) -> None:
    response = await app_client.get("/api/v1/docs/spec.json")
    assert response.status == 200
    spec = await response.json()

    if test_settings.REWRITE_DOC_SPECS:
        with open(SPEC_FILE_PATH, "w") as f:
            f.write(dl_json.dumps_str_human_readable(spec))
            f.write("\n")

    with open(SPEC_FILE_PATH, "rb") as f:
        raw_expected_spec = f.read()
        expected_spec = dl_json.loads_bytes(raw_expected_spec)

    assert spec == expected_spec, "Spec mismatch. Set REWRITE_DOC_SPECS environment variable to rewrite expected spec."
