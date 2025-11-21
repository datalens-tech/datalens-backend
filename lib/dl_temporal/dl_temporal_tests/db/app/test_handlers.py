import aiohttp
import pytest

import dl_temporal
import dl_temporal.app


@pytest.fixture(name="temporal_task_queue")
def fixture_temporal_task_queue() -> str:
    return "tests/db/app/test_handlers"


@pytest.mark.asyncio
async def test_liveness_probe_handler(
    app_settings: dl_temporal.app.BaseTemporalWorkerAppSettings,
) -> None:
    async with aiohttp.ClientSession() as session:
        response = await session.get(
            f"http://{app_settings.http_server.host}:{app_settings.http_server.port}/api/v1/health/liveness"
        )
        assert response.status == 200
        assert await response.json() == {"status": "healthy"}


@pytest.mark.asyncio
async def test_readiness_probe_handler(
    app_settings: dl_temporal.app.BaseTemporalWorkerAppSettings,
) -> None:
    async with aiohttp.ClientSession() as session:
        response = await session.get(
            f"http://{app_settings.http_server.host}:{app_settings.http_server.port}/api/v1/health/readiness"
        )
        assert response.status == 200
        assert await response.json() == {
            "status": "healthy",
            "subsystems_status": {
                "temporal_client.check_health": True,
                "temporal_worker.is_running": True,
            },
        }
