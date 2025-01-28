from __future__ import annotations

import logging

from aiohttp import web
from aiohttp.typedefs import (
    Handler,
    Middleware,
)

from dl_api_commons.aiohttp.aiohttp_wrappers import (
    DLRequestBase,
    RequiredResourceCommon,
)
from dl_constants.api_constants import (
    DLHeaders,
    DLHeadersCommon,
)


LOGGER = logging.getLogger(__name__)


def master_key_middleware(
    master_key: str,
    header: DLHeaders = DLHeadersCommon.API_KEY,
) -> Middleware:
    @web.middleware
    @DLRequestBase.use_dl_request
    async def actual_master_key_middleware(dl_request: DLRequestBase, handler: Handler) -> web.StreamResponse:
        handler_required_resources = dl_request.required_resources

        if RequiredResourceCommon.MASTER_KEY in handler_required_resources:
            LOGGER.info("Master key is required for this handler")
            request_api_key = dl_request.get_single_header(header)

            if request_api_key != master_key:
                LOGGER.warning("Attempt to access private resource without master key")
                raise web.HTTPForbidden()

            LOGGER.info("Master key is OK")

        return await handler(dl_request.request)

    return actual_master_key_middleware
