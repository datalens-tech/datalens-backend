import aiohttp.web
import attrs

import dl_app_api_base.handlers.base as base
import dl_prometheus


@attrs.define(frozen=True, kw_only=True)
class MetricsHandler(base.BaseHandler):
    OPENAPI_INCLUDE = False

    _metrics_registry: dl_prometheus.MetricsRegistryProtocol

    async def process(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        latest = self._metrics_registry.get_latest()
        return base.Response.with_bytes(
            body=latest.body,
            headers={"Content-Type": latest.content_type},
        )
