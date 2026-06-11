import aiohttp.test_utils
import aiohttp.web
import pytest

import dl_app_api_base
import dl_prometheus


def _build_metrics() -> tuple[
    dl_app_api_base.HttpRequestsTotal,
    dl_app_api_base.HttpRequestDurationSeconds,
    dl_prometheus.MetricsRegistry,
]:
    http_requests_total = dl_app_api_base.HttpRequestsTotal.from_settings(dl_app_api_base.HttpRequestsTotalSettings())
    http_request_duration_seconds = dl_app_api_base.HttpRequestDurationSeconds.from_settings(
        dl_app_api_base.HttpRequestDurationSecondsSettings()
    )
    metrics_registry = dl_prometheus.MetricsRegistry(
        metrics=(
            http_requests_total,
            http_request_duration_seconds,
        ),
    )
    return http_requests_total, http_request_duration_seconds, metrics_registry


def _build_app(metrics_middleware: dl_app_api_base.MetricsMiddleware) -> aiohttp.web.Application:
    async def ok_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response(text="ok")

    async def server_error_handler(request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response(status=500, text="boom")

    application = aiohttp.web.Application(middlewares=[metrics_middleware.process])
    application.router.add_get("/items/{item_id}", ok_handler)
    application.router.add_get("/error", server_error_handler)
    return application


def _sample_value(
    metrics_registry: dl_prometheus.MetricsRegistryProtocol,
    name: str,
    labels: dict[str, str],
) -> float | None:
    for sample in metrics_registry.get_samples():
        if sample.name == name and sample.labels == labels:
            return sample.value
    return None


@pytest.mark.asyncio
async def test_records_counter_and_histogram_for_successful_request() -> None:
    http_requests_total, http_request_duration_seconds, metrics_registry = _build_metrics()
    middleware = dl_app_api_base.MetricsMiddleware(
        http_requests_total=http_requests_total,
        http_request_duration_seconds=http_request_duration_seconds,
    )
    application = _build_app(middleware)

    test_server = aiohttp.test_utils.TestServer(application)
    async with test_server, aiohttp.test_utils.TestClient(test_server) as client:
        response = await client.get("/items/42")

    assert response.status == 200

    labels = {
        "method": "GET",
        "status_code": "200",
        "path": "/items/{item_id}",
    }
    assert _sample_value(metrics_registry, "http_requests_total", labels) == 1.0
    assert _sample_value(metrics_registry, "http_request_duration_seconds_count", labels) == 1.0
    assert _sample_value(metrics_registry, "http_request_duration_seconds_sum", labels) is not None


@pytest.mark.asyncio
async def test_path_label_uses_route_pattern_not_concrete_path() -> None:
    http_requests_total, http_request_duration_seconds, metrics_registry = _build_metrics()
    middleware = dl_app_api_base.MetricsMiddleware(
        http_requests_total=http_requests_total,
        http_request_duration_seconds=http_request_duration_seconds,
    )
    application = _build_app(middleware)

    test_server = aiohttp.test_utils.TestServer(application)
    async with test_server, aiohttp.test_utils.TestClient(test_server) as client:
        await client.get("/items/1")
        await client.get("/items/2")
        await client.get("/items/3")

    labels = {
        "method": "GET",
        "status_code": "200",
        "path": "/items/{item_id}",
    }
    assert _sample_value(metrics_registry, "http_requests_total", labels) == 3.0


@pytest.mark.asyncio
async def test_records_status_code_label_for_server_errors() -> None:
    http_requests_total, http_request_duration_seconds, metrics_registry = _build_metrics()
    middleware = dl_app_api_base.MetricsMiddleware(
        http_requests_total=http_requests_total,
        http_request_duration_seconds=http_request_duration_seconds,
    )
    application = _build_app(middleware)

    test_server = aiohttp.test_utils.TestServer(application)
    async with test_server, aiohttp.test_utils.TestClient(test_server) as client:
        response = await client.get("/error")

    assert response.status == 500

    labels = {
        "method": "GET",
        "status_code": "500",
        "path": "/error",
    }
    assert _sample_value(metrics_registry, "http_requests_total", labels) == 1.0
