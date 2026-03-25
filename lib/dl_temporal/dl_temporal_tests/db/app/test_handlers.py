import aiohttp
import pytest


@pytest.fixture(name="temporal_task_queue")
def fixture_temporal_task_queue() -> str:
    return "tests/db/app/test_handlers"


@pytest.mark.asyncio
async def test_liveness_probe_handler(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get("/system/health/liveness")
    assert response.status == 200
    assert await response.json() == {"status": "healthy"}


@pytest.mark.asyncio
async def test_readiness_probe_handler(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get("/system/health/readiness")
    assert response.status == 200
    assert await response.json() == {
        "status": "healthy",
        "subsystems_status": {
            "temporal_client.check_health": {"value": True, "critical": True},
            "temporal_worker.is_running": {"value": True, "critical": True},
        },
    }


@pytest.mark.asyncio
async def test_startup_probe_handler(
    app_client: aiohttp.ClientSession,
) -> None:
    response = await app_client.get("/system/health/startup")
    assert response.status == 200
    assert await response.json() == {
        "status": "healthy",
        "subsystems_status": {
            "temporal_client.check_health": {"value": True, "critical": True},
            "temporal_worker.is_running": {"value": True, "critical": True},
        },
    }
