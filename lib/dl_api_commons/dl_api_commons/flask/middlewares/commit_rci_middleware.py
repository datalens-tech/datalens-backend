from typing import Optional

import attr
import flask

from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.exc import FlaskRCINotSet


@attr.s(frozen=True)
class ReqCtxInfoMiddleware:
    _G_ATTR_NAME_COMMITTED_RCI = "_bi_request_context_info"
    _G_ATTR_NAME_TEMP_RCI = "_bi_temp_request_context_info"

    def _commit_rci(self) -> None:
        # TODO CONSIDER: raise if something is missing
        temp_rci = self.get_temp_rci()

        setattr(flask.g, self._G_ATTR_NAME_COMMITTED_RCI, temp_rci)

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
