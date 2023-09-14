"""
Middleware to extract OpenTracing context from HTTP headers and creating root request span.
Should be placed after ContextVarMiddleware
"""
from __future__ import annotations

import contextlib
import functools
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Sequence,
    Tuple,
)

import attr
import flask
import opentracing
import opentracing.tags

from bi_api_commons.flask.middlewares.logging_context import RequestLoggingContextControllerMiddleWare
from bi_api_commons.flask.middlewares.wsgi_middleware import FlaskWSGIMiddleware
from bi_api_commons.logging_tracing import OpenTracingLoggingContextController

if TYPE_CHECKING:
    from bi_api_commons.flask.types import (
        WSGIEnviron,
        WSGIReturn,
        WSGIStartResponse,
    )

LOGGER = logging.getLogger(__name__)


@attr.s
class TracingMiddleware(FlaskWSGIMiddleware):
    _APP_FLAG_ATTR_NAME = "_bi_middleware_flag_open_tracing"

    url_prefix_exclude: Sequence[str] = attr.ib(default=())

    @staticmethod
    def normalize_headers(wsgi_environ: Dict[str, str]) -> Dict[str, str]:
        prefix = "HTTP_"
        headers = {
            key[len(prefix) :].replace("_", "-").lower(): val
            for (key, val) in wsgi_environ.items()
            if key.startswith(prefix)
        }
        return headers

    def should_create_root_span(self, http_path: str) -> bool:
        for prefix in self.url_prefix_exclude:
            if http_path.startswith(prefix):
                return False
        return True

    def wsgi_app(self, environ: WSGIEnviron, start_response: WSGIStartResponse) -> WSGIReturn:  # type: ignore
        http_path = environ["PATH_INFO"]
        http_method = environ["REQUEST_METHOD"]
        tracer = opentracing.global_tracer()

        span_context = tracer.extract(opentracing.Format.HTTP_HEADERS, self.normalize_headers(environ))
        default_operation_name = "inbound_request"

        effective_start_response: WSGIStartResponse = start_response

        with contextlib.ExitStack() as cm_stack:
            if self.should_create_root_span(http_path=http_path):
                scope = cm_stack.enter_context(tracer.start_active_span(default_operation_name, child_of=span_context))
                scope.span.set_tag(opentracing.tags.SPAN_KIND, opentracing.tags.SPAN_KIND_RPC_SERVER)
                scope.span.set_tag(opentracing.tags.HTTP_URL, http_path)
                scope.span.set_tag(opentracing.tags.HTTP_METHOD, http_method)

                @functools.wraps(start_response)
                def start_response_set_span_tags(
                    status: str, response_headers: List[Tuple[str, str]], exc_info: Any = None
                ) -> Any:
                    try:
                        status_code = int(status.split(" ")[0])
                        scope.span.set_tag(opentracing.tags.HTTP_STATUS_CODE, status_code)
                        if status_code >= 500:
                            scope.span.set_tag(opentracing.tags.ERROR, True)
                    except Exception:  # noqa
                        LOGGER.exception("Exception during extracting tags from WSGI start_response args")

                    return start_response(status, response_headers, exc_info)

                effective_start_response = start_response_set_span_tags

            return self.original_wsgi_app(environ, effective_start_response)


class TracingContextMiddleware:
    def set_up(self, app: flask.Flask) -> None:
        app.before_request(self._bind_logging_request_controller_to_request)

    @classmethod
    def _bind_logging_request_controller_to_request(cls) -> None:
        tracer = opentracing.global_tracer()

        if tracer.active_span is not None:
            composite_ctrl = RequestLoggingContextControllerMiddleWare.get_for_request()
            composite_ctrl.add_sub_controller(OpenTracingLoggingContextController(tracer.active_span))
