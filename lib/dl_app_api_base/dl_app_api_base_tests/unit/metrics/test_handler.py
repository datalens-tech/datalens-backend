import aiohttp.test_utils
import aiohttp.web
import pytest

import dl_app_api_base
import dl_prometheus


@pytest.mark.asyncio
async def test_handler_returns_prometheus_exposition_format() -> None:
    counter = dl_prometheus.Counter(
        name="example_total",
        documentation="example",
    )
    metrics_registry = dl_prometheus.MetricsRegistry(metrics=(counter,))
    counter.inc(2.0)

    handler = dl_app_api_base.MetricsHandler(metrics_registry=metrics_registry)
    application = aiohttp.web.Application()
    application.router.add_get("/system/metrics", handler.process)

    test_server = aiohttp.test_utils.TestServer(application)
    async with test_server:
        async with aiohttp.test_utils.TestClient(test_server) as client:
            response = await client.get("/system/metrics")
            body = await response.text()

    assert response.status == 200
    assert response.headers["Content-Type"].startswith("text/plain")
    assert "example_total 2.0" in body
