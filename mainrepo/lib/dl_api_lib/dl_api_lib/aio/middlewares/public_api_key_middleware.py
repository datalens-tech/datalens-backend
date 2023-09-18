from __future__ import annotations

import logging

from aiohttp import web
from aiohttp.typedefs import Handler

from dl_api_commons.aio.typing import AIOHTTPMiddleware
from dl_api_commons.aiohttp.aiohttp_wrappers import RequiredResourceCommon
from dl_api_commons.base_models import TenantCommon
from dl_api_lib.aio.aiohttp_wrappers import DSAPIRequest
from dl_constants.api_constants import DLHeadersCommon

LOGGER = logging.getLogger(__name__)


def public_api_key_middleware(api_key: str) -> AIOHTTPMiddleware:
    if not isinstance(api_key, str):
        raise TypeError(f"API key must be a string, not '{type(api_key)}'")

    @web.middleware
    @DSAPIRequest.use_dl_request
    async def actual_public_api_key_middleware(dl_request: DSAPIRequest, handler: Handler) -> web.StreamResponse:
        if RequiredResourceCommon.SKIP_AUTH in dl_request.required_resources:
            LOGGER.info("Auth was skipped due to SKIP_AUTH flag in target view")
            return await handler(dl_request.request)

        inbound_api_key = dl_request.get_single_header(DLHeadersCommon.PUBLIC_API_KEY, required=False)

        if inbound_api_key is None:
            raise web.HTTPForbidden(reason="public api key required")

        if inbound_api_key != api_key:
            LOGGER.info("Invalid API key, rejecting request...")
            raise web.HTTPForbidden(reason="invalid public api key")

        LOGGER.info("API key check passed")

        dl_request.replace_temp_rci(
            dl_request.temp_rci.clone(
                user_id="__ANONYMOUS_USER_OF_PUBLIC_DATALENS__",
                user_name="Anonymous (Public) User",
                tenant=TenantCommon(),
            )
        )

        return await handler(dl_request.request)

    return actual_public_api_key_middleware
