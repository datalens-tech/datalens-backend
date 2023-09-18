from __future__ import annotations

import abc
from asyncio import CancelledError
from contextlib import ExitStack
import enum
import logging
import os
import sys
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
)

from aiohttp import web
import attr
import sentry_sdk
from typing_extensions import final

from dl_api_commons.logging import RequestLoggingContextController

LOGGER = logging.getLogger(__name__)


@final
class ErrorLevel(enum.Enum):
    info = enum.auto()
    warning = enum.auto()
    error = enum.auto()


@attr.s(auto_attribs=True)
class ErrorData:
    status_code: int
    response_body: Dict[str, Any]
    level: ErrorLevel = attr.ib(validator=[attr.validators.instance_of(ErrorLevel)])
    http_reason: Optional[str] = attr.ib(default=None)


@attr.s
class AIOHTTPErrorHandler(metaclass=abc.ABCMeta):
    sentry_app_name_tag: Optional[str] = attr.ib()
    use_sentry: bool = attr.ib(default=False)

    @sentry_app_name_tag.validator
    def _validate_sentry_app_name_tag(self, attribute, value):  # type: ignore  # TODO: fix
        if value is not None and not isinstance(value, str):
            raise ValueError(f"Unexpected type of sentry_app_name_tag: '{value}'")

    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def make_response(self, err_data: ErrorData, err: Exception, request: web.Request) -> web.Response:
        return web.json_response(err_data.response_body, status=err_data.status_code, reason=err_data.http_reason)

    @abc.abstractmethod
    def _classify_error(self, err: Exception, request: web.Request) -> ErrorData:
        pass

    def handle_error(
        self, err: Exception, request: web.Request, req_logging_ctx_ctrl: RequestLoggingContextController
    ) -> Tuple[web.Response, ErrorData]:
        if isinstance(err, CancelledError):
            LOGGER.warning("Client request was cancelled", exc_info=True)
            raise  # noqa
        elif isinstance(err, web.HTTPSuccessful):
            raise  # noqa
        else:
            self.maybe_debug_catch(err)
            try:
                # TODO CONSIDER: Validate that response is serializable
                err_data = self._classify_error(err, request)
                self.log_error_http_response(err, err_data, req_logging_ctx_ctrl)
                return self.make_response(err_data, err, request), err_data

            except Exception as on_error_error:  # noqa
                req_logging_ctx_ctrl.put_to_context("is_error", True)
                LOGGER.error(
                    "Error handler raised an error during handling this exception",
                    exc_info=(type(err), err, err.__traceback__),
                )
                LOGGER.critical("Error handler raised an error during creating error response", exc_info=True)
                err_data = ErrorData(
                    500,
                    http_reason="Internal server error",
                    response_body=dict(message="Internal server error"),
                    level=ErrorLevel.error,
                )
                return (
                    web.json_response(
                        dict(message="Internal Server Error"),
                        status=500,
                    ),
                    err_data,
                )

    def maybe_debug_catch(self, err):  # type: ignore  # TODO: fix
        if os.environ.get("BI_ERR_PDB"):
            sys.last_traceback = err.__traceback__
            import traceback

            traceback.print_exc()
            import ipdb

            ipdb.pm()

    def log_error_http_response(  # type: ignore  # TODO: fix
        self,
        err: Exception,
        err_data: ErrorData,
        request_logging_context_ctrl: RequestLoggingContextController,
    ):
        with ExitStack() as err_logging_stack:
            # skip all 4xx codes
            use_sentry = self.use_sentry and not (400 <= err_data.status_code < 500)
            if use_sentry:
                reg_exc_scope = err_logging_stack.enter_context(sentry_sdk.push_scope())
                # Tagging events as captured in this middleware
                reg_exc_scope.set_tag("event_source", "outer_error_handling_middleware")
                reg_exc_scope.set_tag("http_response_code", err_data.status_code)
                reg_exc_scope.set_extra("http_response_body", err_data.response_body)
                reg_exc_scope.set_level(err_data.level.name)

            if err_data.level == ErrorLevel.info:
                # Assumed that events will bi captured for warnings and higher, so we capture manually only for info
                if use_sentry:
                    sentry_sdk.capture_exception(err)
                LOGGER.info("Regular exception fired", exc_info=True)
            elif err_data.level == ErrorLevel.warning:
                LOGGER.warning("Warning exception fired", exc_info=True)
            else:
                request_logging_context_ctrl.put_to_context("is_error", True)
                LOGGER.exception("Caught an exception in request handler")
