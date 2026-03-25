import aiohttp
import pytest

import dl_app_api_base_tests.unit.conftest as conftest


@pytest.mark.asyncio
async def test_default(
    app_client: aiohttp.ClientSession,
    readiness_resource: conftest.ReadinessResource,
    non_critical_readiness_resource: conftest.ReadinessResource,
) -> None:
    readiness_resource.set_readiness(True)
    non_critical_readiness_resource.set_readiness(True)
    response = await app_client.get("/system/health/startup")
    assert response.status == 200
    assert await response.json() == {
        "status": "healthy",
        "subsystems_status": {
            "readiness_resource.is_ready": {"value": True, "critical": True},
            "non_critical_readiness_resource.is_ready": {"value": True, "critical": False},
        },
    }


@pytest.mark.asyncio
async def test_non_critical_unhealthy(
    app_client: aiohttp.ClientSession,
    readiness_resource: conftest.ReadinessResource,
    non_critical_readiness_resource: conftest.ReadinessResource,
) -> None:
    readiness_resource.set_readiness(True)
    non_critical_readiness_resource.set_readiness(False)

    response = await app_client.get("/system/health/startup")
    assert response.status == 500
    assert await response.json() == {
        "status": "unhealthy",
        "subsystems_status": {
            "readiness_resource.is_ready": {"value": True, "critical": True},
            "non_critical_readiness_resource.is_ready": {"value": False, "critical": False},
        },
    }


@pytest.mark.asyncio
async def test_critical_unhealthy(
    app_client: aiohttp.ClientSession,
    readiness_resource: conftest.ReadinessResource,
    non_critical_readiness_resource: conftest.ReadinessResource,
) -> None:
    readiness_resource.set_readiness(False)
    non_critical_readiness_resource.set_readiness(True)

    response = await app_client.get("/system/health/startup")
    assert response.status == 500
    assert await response.json() == {
        "status": "unhealthy",
        "subsystems_status": {
            "readiness_resource.is_ready": {"value": False, "critical": True},
            "non_critical_readiness_resource.is_ready": {"value": True, "critical": False},
        },
    }


@pytest.mark.asyncio
async def test_all_unhealthy(
    app_client: aiohttp.ClientSession,
    readiness_resource: conftest.ReadinessResource,
    non_critical_readiness_resource: conftest.ReadinessResource,
) -> None:
    readiness_resource.set_readiness(False)
    non_critical_readiness_resource.set_readiness(False)

    response = await app_client.get("/system/health/startup")
    assert response.status == 500
    assert await response.json() == {
        "status": "unhealthy",
        "subsystems_status": {
            "readiness_resource.is_ready": {"value": False, "critical": True},
            "non_critical_readiness_resource.is_ready": {"value": False, "critical": False},
        },
    }
