from __future__ import annotations

import contextlib
import logging
from typing import (
    Any,
    Generator,
    Optional,
)

from aiohttp import web
from aiohttp.typedefs import Handler
import opentracing
import opentracing.tags

from dl_api_commons.aio.middlewares.commons import (
    add_logging_ctx_controller,
    get_endpoint_code,
)
from dl_api_commons.logging_tracing import OpenTracingLoggingContextController


LOGGER = logging.getLogger(__name__)


class TracingService:
    """
    This middleware creates a root span for request to component.
    It provides interface for setting tags/other data to span via logging context controller.
    It is injected into AIOHTTP request and can be accessed via DLRequestBase.
    Assumed that it will be top-level middleware.
    """

    @contextlib.contextmanager
    def prepared_span_cm(self, request: web.Request, operation_name: str) -> Generator[opentracing.Span, None, None]:
        tracer = opentracing.global_tracer()
        span_context = tracer.extract(format=opentracing.Format.HTTP_HEADERS, carrier=request.headers)

        with tracer.start_active_span(operation_name, child_of=span_context) as scope:
            span = scope.span
            span.set_tag(opentracing.tags.SPAN_KIND, opentracing.tags.SPAN_KIND_RPC_SERVER)
            # TODO FIX: consider move to request ID middleware
            span.set_tag(opentracing.tags.HTTP_METHOD, request.method)
            span.set_tag(opentracing.tags.HTTP_URL, str(request.url))

            logging_context_controller = OpenTracingLoggingContextController(scope.span)
            add_logging_ctx_controller(request, logging_context_controller)
            yield span

    @staticmethod
    def set_span_tag(span: Optional[opentracing.Span], tag_name: str, tag_value: Any) -> None:
        if span is None:
            return
        span.set_tag(tag_name, tag_value)

    @web.middleware
    async def middleware(self, request: web.Request, handler: Handler) -> web.StreamResponse:  # type: ignore[return]
        root_span: Optional[opentracing.Span] = None
        operation_name = get_endpoint_code(request)

        with contextlib.ExitStack() as exit_stack:
            if operation_name is not None:
                if isinstance(operation_name, str):
                    root_span = exit_stack.enter_context(self.prepared_span_cm(request, operation_name))
                else:
                    LOGGER.warning("Got not-string value for root span operation name : %s", operation_name)

            try:
                resp = await handler(request)
            except Exception:  # noqa
                self.set_span_tag(root_span, opentracing.tags.ERROR, True)
                raise
            else:
                self.set_span_tag(root_span, opentracing.tags.HTTP_STATUS_CODE, resp.status)
                return resp
