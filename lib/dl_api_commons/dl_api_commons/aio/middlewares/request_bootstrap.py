from __future__ import annotations

import asyncio
import contextlib
from http import HTTPStatus
import json
import logging
import time
from typing import (
    Any,
    Iterable,
    Optional,
)

from aiohttp import web
from aiohttp.typedefs import Handler
import attr
import sentry_sdk

from dl_api_commons.aio.middlewares.commons import (
    add_logging_ctx_controller,
    get_root_logging_context_controller,
)
from dl_api_commons.aio.middlewares.error_handling_outer import AIOHTTPErrorHandler
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aiohttp.aiohttp_wrappers import DLRequestBase
from dl_api_commons.exc import RequestTimeoutError
from dl_api_commons.logging import RequestLoggingContextController
from dl_api_commons.reporting.records import RequestResultReportingRecord
from dl_utils.aio import timeout


LOGGER = logging.getLogger(__name__)


@attr.s
class SentryRequestLoggingContextController(RequestLoggingContextController):
    _scope: sentry_sdk.Scope = attr.ib()
    _user_info: dict = attr.ib(init=False, factory=dict)

    allowed_tags = frozenset(
        (
            "request_id",
            "parent_request_id",
            "folder_id",
            "dataset_id",
            "conn_id",
            "conn_exec_cls",
            "conn_type",
        )
    )

    user_info_remap = {"user_name": "username", "user_id": "id"}

    def put_to_context(self, key: str, value: Any) -> None:
        val_to_write: Any
        try:
            json.dumps(value)
            val_to_write = value
        except Exception:  # noqa
            LOGGER.exception("Attempt to put to context non-jsonable value")
            try:
                val_to_write = str(value)
            except Exception:
                LOGGER.exception("Attempt to put to context non-stringifiable value")
                raise

        if key in self.user_info_remap:
            self._user_info[self.user_info_remap[key]] = val_to_write
            self._scope.set_user({**self._user_info})
        elif key in self.allowed_tags:
            self._scope.set_tag(key, val_to_write)
        else:
            self._scope.set_extra(key, val_to_write)


@contextlib.contextmanager  # type: ignore  # TODO: fix
def last_chance_log() -> Iterable[None]:
    try:
        yield
    except Exception:
        LOGGER.exception("Exception fired in outer_error_handling_middleware")
        raise


@attr.s
class RequestBootstrap:
    req_id_service: RequestId = attr.ib()
    error_handler: Optional[AIOHTTPErrorHandler] = attr.ib(default=None)
    timeout_sec: Optional[float] = attr.ib(default=None)

    @web.middleware
    async def middleware(self, request: web.Request, handler: Handler) -> web.StreamResponse:
        dl_request: Optional[DLRequestBase] = None
        result = None
        err_code: Optional[str] = None
        try:
            with contextlib.ExitStack() as top_level_stack:
                top_level_stack.enter_context(last_chance_log())
                if self.error_handler is not None and self.error_handler.use_sentry:
                    scope = top_level_stack.enter_context(sentry_sdk.configure_scope())
                    if self.error_handler.sentry_app_name_tag:
                        scope.set_tag("app_name", self.error_handler.sentry_app_name_tag)
                    add_logging_ctx_controller(request, SentryRequestLoggingContextController(scope))

                req_logging_ctx_ctrl = get_root_logging_context_controller(request)

                dl_request = self.req_id_service.dl_request_init(request)

                if self.timeout_sec is not None:
                    try:
                        async with timeout(self.timeout_sec):
                            result = await handler(request)
                    except asyncio.TimeoutError:
                        raise RequestTimeoutError() from None
                else:
                    result = await handler(request)
        except Exception as err:
            if self.error_handler is not None:
                result, err_data = self.error_handler.handle_error(err, request, req_logging_ctx_ctrl)
                err_code = err_data.response_body.get("code", None)
            else:
                raise
        finally:
            response_status_code = (
                result.status if isinstance(result, web.Response) else HTTPStatus.INTERNAL_SERVER_ERROR
            )
            if dl_request is not None:
                dl_request.reporting_registry.save_reporting_record(
                    RequestResultReportingRecord(
                        timestamp=time.time(),
                        response_status_code=response_status_code,
                        err_code=err_code,
                    )
                )
                self.req_id_service.dl_request_finilize(dl_request)
            self.req_id_service.on_request_end(request.method, request.path_qs, response_status_code)
        assert isinstance(result, web.StreamResponse)
        return result
