from __future__ import annotations

from typing import Optional

import attr
import flask
from multidict import CIMultiDict

from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.exc import FlaskRCINotSet
from dl_api_commons.headers import (
    DEFAULT_RCI_PLAIN_HEADERS,
    DEFAULT_RCI_SECRET_HEADERS,
    append_extra_headers_and_normalize,
)


@attr.s(frozen=True)
class ReqCtxInfoMiddleware:
    """
    This middleware gathers context info from products of another middleware and request headers.
    It should be setup after request id & auth middleware.
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

    _G_ATTR_NAME_COMMITTED_RCI = "_bi_request_context_info"
    _G_ATTR_NAME_TEMP_RCI = "_bi_temp_request_context_info"

    def _commit_rci(self) -> None:
        # TODO CONSIDER: raise if something is missing
        temp_rci = self.get_temp_rci()

        final_rci = temp_rci.clone(
            # TODO CONSIDER: Add default user agent if not passed in header
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
        setattr(flask.g, self._G_ATTR_NAME_COMMITTED_RCI, final_rci)

    def set_up(self, app: flask.Flask) -> None:
        app.before_request(self._commit_rci)

    @classmethod
    def get_last_resort_rci(cls) -> Optional[RequestContextInfo]:
        try:
            return cls.get_request_context_info()
        except FlaskRCINotSet:
            try:
                return cls.get_temp_rci()
            except FlaskRCINotSet:
                return None

    @classmethod
    def get_temp_rci(cls) -> RequestContextInfo:
        temp_rci = getattr(flask.g, cls._G_ATTR_NAME_TEMP_RCI, None)
        if temp_rci is None:
            raise FlaskRCINotSet("Temp RCI was not set for current request context")
        assert isinstance(temp_rci, RequestContextInfo)
        return temp_rci

    @classmethod
    def replace_temp_rci(cls, rci: RequestContextInfo) -> None:
        assert isinstance(rci, RequestContextInfo)
        setattr(flask.g, cls._G_ATTR_NAME_TEMP_RCI, rci)

    @classmethod
    def is_rci_committed(cls) -> bool:
        return hasattr(flask.g, cls._G_ATTR_NAME_COMMITTED_RCI)

    @classmethod
    def get_request_context_info(cls) -> RequestContextInfo:
        if hasattr(flask.g, cls._G_ATTR_NAME_COMMITTED_RCI):
            return getattr(flask.g, cls._G_ATTR_NAME_COMMITTED_RCI)

        raise FlaskRCINotSet("Request context info was not flushed")
