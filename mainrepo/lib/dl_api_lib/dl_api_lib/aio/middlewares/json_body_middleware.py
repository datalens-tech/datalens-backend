# TODO FIX: Move to common

from __future__ import annotations

import json
import logging

from aiohttp import web
from aiohttp.typedefs import Handler

from dl_api_commons.aio.typing import AIOHTTPMiddleware
from dl_api_commons.logging import mask_sensitive_fields_by_name_in_json_recursive
from dl_api_lib.aio.aiohttp_wrappers import DSAPIRequest
from dl_api_lib.app.data_api.resources.base import (
    BaseView,
    RequiredResourceDSAPI,
)

LOGGER = logging.getLogger(__name__)


def json_body_middleware() -> AIOHTTPMiddleware:
    @web.middleware
    @DSAPIRequest.use_dl_request
    async def actual_body_log_middleware(dl_request: DSAPIRequest, handler: Handler) -> web.StreamResponse:
        request = dl_request.request
        view_cls = request.match_info.handler

        if (
            RequiredResourceDSAPI.JSON_REQUEST in dl_request.required_resources
            # TODO FIX: ensure methods
            and request.method.lower() in ("post", "put", "patch", "delete")
        ):
            # TODO CONSIDER: Maybe check content type first
            try:
                body = await request.json()
            # TODO CONSIDER: Maybe catch all exceptions
            except json.JSONDecodeError as err:
                raise web.HTTPBadRequest(reason="Invalid JSON in body") from err

            dl_request.store_parsed_json_body(body)

            dbg_body_data = mask_sensitive_fields_by_name_in_json_recursive(body)
            dbg_body = json.dumps(dbg_body_data)
            url = dl_request.url  # url with http->https override
            extra = dict(
                request_path=url,
                request_body=dbg_body,
            )
            LOGGER.debug("Body for %s: %s...", url, dbg_body[:100], extra=extra)

        if isinstance(view_cls, type) and issubclass(view_cls, BaseView):

            async def json_is_ready():
                raise Exception("Direct usage of request.json() is prohibited. Use DLRequest.json_body")

            request.json = json_is_ready

        return await handler(request)

    return actual_body_log_middleware
