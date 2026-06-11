import time

import aiohttp.typedefs
import aiohttp.web
import attrs

import dl_app_api_base.metrics.http_request_duration_seconds as http_request_duration_seconds
import dl_app_api_base.metrics.http_requests_total as http_requests_total

UNKNOWN_PATH_LABEL = "unknown"


@attrs.define(frozen=True, kw_only=True)
class MetricsMiddleware:
    _http_requests_total: http_requests_total.HttpRequestsTotal
    _http_request_duration_seconds: http_request_duration_seconds.HttpRequestDurationSeconds

    @aiohttp.web.middleware
    async def process(
        self,
        request: aiohttp.web.Request,
        handler: aiohttp.typedefs.Handler,
    ) -> aiohttp.web.StreamResponse:
        started_at = time.perf_counter()
        response = await handler(request)
        elapsed_seconds = time.perf_counter() - started_at

        method = request.method
        status_code = response.status
        path = self._extract_path_label(request)
        self._http_requests_total.record(method=method, status_code=status_code, path=path)
        self._http_request_duration_seconds.record(
            method=method,
            status_code=status_code,
            path=path,
            duration=elapsed_seconds,
        )

        return response

    @staticmethod
    def _extract_path_label(request: aiohttp.web.Request) -> str:
        match_info = request.match_info
        if match_info.route is None:
            return UNKNOWN_PATH_LABEL
        resource = match_info.route.resource
        if resource is None:
            return UNKNOWN_PATH_LABEL
        return resource.canonical
