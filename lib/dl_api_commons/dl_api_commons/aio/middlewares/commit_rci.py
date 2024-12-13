from __future__ import annotations

from typing import (
    Optional,
    Sequence,
)

from aiohttp import web
from aiohttp.typedefs import (
    Handler,
    Middleware,
)
from multidict import CIMultiDict

from dl_api_commons.aiohttp import aiohttp_wrappers
from dl_api_commons.headers import (
    DEFAULT_RCI_PLAIN_HEADERS,
    DEFAULT_RCI_SECRET_HEADERS,
    append_extra_headers_and_normalize,
)


def commit_rci_middleware(
    rci_extra_plain_headers: Optional[Sequence[str]] = None,
    rci_extra_secret_headers: Optional[Sequence[str]] = None,
) -> Middleware:
    plain_headers_to_rci = append_extra_headers_and_normalize(DEFAULT_RCI_PLAIN_HEADERS, rci_extra_plain_headers or ())
    secret_headers_to_rci = append_extra_headers_and_normalize(
        DEFAULT_RCI_SECRET_HEADERS, rci_extra_secret_headers or ()
    )

    @web.middleware
    @aiohttp_wrappers.DLRequestBase.use_dl_request
    async def actual_commit_rci_middleware(
        dl_request: aiohttp_wrappers.DLRequestBase, handler: Handler
    ) -> web.StreamResponse:
        headers = dl_request.request.headers
        dl_request.replace_temp_rci(
            dl_request.temp_rci.clone(
                plain_headers=CIMultiDict(
                    ((key, val) for key, val in headers.items() if key.lower() in plain_headers_to_rci)
                ),
                secret_headers=CIMultiDict(
                    ((key, val) for key, val in headers.items() if key.lower() in secret_headers_to_rci)
                ),
            )
        )
        dl_request.commit_rci()
        return await handler(dl_request.request)

    return actual_commit_rci_middleware
