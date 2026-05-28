import aiohttp
import pytest

import dl_app_api_ext_testing.base as base


class MetricsExtTestSuite(base.ExtTestSuiteBase):
    @pytest.mark.asyncio
    async def test_metrics_endpoint_returns_prometheus_exposition(
        self,
        app_client: aiohttp.ClientSession,
        ext_test_suite_settings: base.ExtTestSuiteSettings,
    ) -> None:
        await app_client.get("system/health/liveness")

        response = await app_client.get(ext_test_suite_settings.METRICS_PATH.lstrip("/"))
        assert response.status == 200
        content_type = response.headers["Content-Type"].split(";", 1)[0].strip()
        assert content_type == "text/plain"

        body = await response.text()
        assert "http_requests_total" in body
        assert "http_request_duration_seconds" in body
