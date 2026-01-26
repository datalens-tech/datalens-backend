import aiohttp
import pytest

import dl_app_api_base_tests.unit.conftest as conftest


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
    readiness_resource: conftest.ReadinessResource,
) -> None:
    readiness_resource.set_readiness(True)
    response = await app_client.get("/api/v1/health/readiness")
    assert response.status == 200
    assert await response.json() == {
        "status": "healthy",
        "subsystems_status": {
            "readiness_resource.is_ready": True,
        },
    }


@pytest.mark.asyncio
async def test_unhealthy(
    app_client: aiohttp.ClientSession,
    readiness_resource: conftest.ReadinessResource,
) -> None:
    readiness_resource.set_readiness(False)

    response = await app_client.get("/api/v1/health/readiness")
    assert response.status == 500
    assert await response.json() == {
        "status": "unhealthy",
        "subsystems_status": {
            "readiness_resource.is_ready": False,
        },
    }
