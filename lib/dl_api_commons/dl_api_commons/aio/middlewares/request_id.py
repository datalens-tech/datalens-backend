from __future__ import annotations

import json
import logging
from typing import (
    Optional,
    Type,
)

from aiohttp import web
import attr

from dl_api_commons import (
    make_uuid_from_parts,
    request_id_generator,
)
from dl_api_commons.aio.middlewares.commons import get_endpoint_code
from dl_api_commons.aiohttp import aiohttp_wrappers
from dl_api_commons.aiohttp.aiohttp_wrappers import DLRequestBase
from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.headers import (
    HEADER_DEBUG_MODE_ENABLED,
    HEADER_LOGGING_CONTEXT,
    get_x_dl_context,
)
from dl_api_commons.logging import (
    NON_TRANSITIVE_LOGGING_CTX_KEYS,
    RequestLogHelper,
)
from dl_api_commons.reporting.profiler import DefaultReportingProfiler
from dl_api_commons.reporting.registry import DefaultReportingRegistry
from dl_app_tools import log
from dl_constants.api_constants import DLHeadersCommon


LOGGER = logging.getLogger(__name__)
LOG_HELPER = RequestLogHelper(logger=LOGGER)


@attr.s
class RequestId:
    header_name: str = attr.ib(default="X-Request-ID")
    dl_request_cls: Type[aiohttp_wrappers.DLRequestBase] = attr.ib(default=aiohttp_wrappers.DLRequestBase)
    append_own_req_id: bool = attr.ib(default=False)
    app_prefix: Optional[str] = attr.ib(default=None)
    accept_logging_ctx: bool = attr.ib(default=False)
    logging_ctx_header_name: Optional[str] = attr.ib(default=None)
    # Is used only for reporting registry to determine if request is anonymous
    #  Remove after adding mechanisms to detect if request in anonymous via RCI (e.g. based on user ID/AuthData)
    is_public_env: bool = attr.ib(default=False)

    def __attrs_post_init__(self) -> None:
        if self.append_own_req_id and self.accept_logging_ctx:
            raise ValueError("Flags accept_logging_ctx and append_own_req_id can not be used together")

        if self.logging_ctx_header_name is None:
            self.logging_ctx_header_name = HEADER_LOGGING_CONTEXT

    async def on_response_prepare(self, request: web.Request, response: web.StreamResponse) -> None:
        dl_req = self.dl_request_cls.get_for_request(request)
        if dl_req is not None and dl_req.last_resort_rci is not None and dl_req.last_resort_rci.request_id is not None:
            response.headers.add(self.header_name, dl_req.last_resort_rci.request_id)

    def dl_request_init(self, request: web.Request) -> DLRequestBase:
        dl_request = self.dl_request_cls.init_and_bind_to_aiohttp_request(request)

        parent_request_id = dl_request.request.headers.get(self.header_name)

        if self.append_own_req_id:
            request_id = make_uuid_from_parts(
                current=request_id_generator(self.app_prefix),
                parent=parent_request_id,
            )
        else:
            request_id = parent_request_id or request_id_generator(self.app_prefix)

        endpoint_code = get_endpoint_code(request)

        trace_id_header = DLHeadersCommon.UBER_TRACE_ID.lower()
        trace_id = request.headers[trace_id_header].split(":")[0] if trace_id_header in request.headers else None

        if dl_request.log_ctx_controller:
            dl_request.log_ctx_controller.put_to_context("trace_id", trace_id)
            dl_request.log_ctx_controller.put_to_context("request_id", request_id)
            dl_request.log_ctx_controller.put_to_context("parent_request_id", parent_request_id)
            dl_request.log_ctx_controller.put_to_context("endpoint_code", endpoint_code)

        log.context.reset_context()
        initial_log_context = log.context.get_log_context()
        assert len(initial_log_context) == 0, (
            f"Initial log context for request {request_id}" f" is not empty: {initial_log_context}"
        )

        log.context.put_to_context("trace_id", trace_id)
        log.context.put_to_context("request_id", request_id)
        log.context.put_to_context("parent_request_id", parent_request_id)
        log.context.put_to_context("endpoint_code", endpoint_code)

        if self.accept_logging_ctx and self.logging_ctx_header_name in request.headers:
            assert self.logging_ctx_header_name is not None
            try:
                logging_ctx_from_header = json.loads(request.headers.get(self.logging_ctx_header_name) or "")
                for ctx_key in NON_TRANSITIVE_LOGGING_CTX_KEYS:
                    logging_ctx_from_header.pop(ctx_key, None)
            except Exception:  # noqa
                LOGGER.exception("Can not parse logging context: %s", request.headers.get(self.logging_ctx_header_name))
            else:
                for ctx_key, ctx_val in logging_ctx_from_header.items():
                    if ctx_key in initial_log_context:
                        LOGGER.warning(
                            "Logging context key was in initial logging context: '%s' -> '%s'."
                            " Ignoring value from header",
                            ctx_key,
                            initial_log_context[ctx_key],
                        )
                    else:
                        log.context.put_to_context(ctx_key, ctx_val)

        LOG_HELPER.log_request_start(method=request.method, full_path=request.path_qs, headers=request.headers.items())

        dl_request.init_temp_rci(
            RequestContextInfo.create(
                request_id=request_id,
                x_dl_debug_mode=bool(int(request.headers.get(HEADER_DEBUG_MODE_ENABLED, "0"))),
                endpoint_code=endpoint_code,
                x_dl_context=get_x_dl_context(dl_request.request.headers.get(DLHeadersCommon.DL_CONTEXT, "{}")),
                # This props will be filled in commit_rci middleware
                plain_headers=None,
                secret_headers=None,
                # This props will be filled in auth middleware
                tenant=None,
                user_id=None,
                user_name=None,
                auth_data=None,
            )
        )

        reporting_registry = DefaultReportingRegistry(rci=dl_request.temp_rci)
        dl_request.reporting_registry = reporting_registry
        reporting_profiler = DefaultReportingProfiler(
            reporting_registry=reporting_registry,
            is_public_env=self.is_public_env,
        )
        dl_request.reporting_profiler = reporting_profiler

        return dl_request

    def dl_request_finilize(self, dl_request: DLRequestBase) -> None:
        dl_request.reporting_profiler.on_request_end()

    def on_request_end(self, method: str, full_path: str, status_code: int) -> None:
        LOG_HELPER.log_request_end(method=method, full_path=full_path, status_code=status_code)
