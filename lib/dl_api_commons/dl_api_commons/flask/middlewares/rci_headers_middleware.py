from __future__ import annotations

import attr
import flask
from multidict import CIMultiDict

from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from dl_api_commons.headers import (
    DEFAULT_RCI_PLAIN_HEADERS,
    DEFAULT_RCI_SECRET_HEADERS,
    append_extra_headers_and_normalize,
)


@attr.s(frozen=True)
class RCIHeadersMiddleware:
    """
    This middleware populates plain_headers and secret_headers on the temp RCI
    from the incoming request headers.
    It should be set up before auth middleware and before ReqCtxInfoMiddleware (commit_rci).
    """

    plain_headers: tuple[str, ...] = attr.ib(  # type: ignore  # 2024-01-24 # TODO: Need type annotation for "plain_headers"  [var-annotated]
        default=(),
        converter=lambda extra: append_extra_headers_and_normalize(
            default=DEFAULT_RCI_PLAIN_HEADERS,
            extra=extra,
        ),
    )
    secret_headers: tuple[str, ...] = attr.ib(  # type: ignore  # 2024-01-24 # TODO: Need type annotation for "secret_headers"  [var-annotated]
        default=(),
        converter=lambda extra: append_extra_headers_and_normalize(
            default=DEFAULT_RCI_SECRET_HEADERS,
            extra=extra,
        ),
    )

    def _populate_rci_headers(self) -> None:
        temp_rci = ReqCtxInfoMiddleware.get_temp_rci()

        updated_rci = temp_rci.clone(
            plain_headers=CIMultiDict(
                (
                    (
                        header,
                        flask.request.headers.get(header),
                    )
                    for header in self.plain_headers
                )
            ),
            secret_headers=CIMultiDict(
                (
                    (
                        header,
                        flask.request.headers.get(header),
                    )
                    for header in self.secret_headers
                )
            ),
        )
        ReqCtxInfoMiddleware.replace_temp_rci(updated_rci)

    def set_up(self, app: flask.Flask) -> None:
        app.before_request(self._populate_rci_headers)
