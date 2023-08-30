from __future__ import annotations

from typing import Optional

from aiohttp import web

from bi_api_commons.aiohttp.aiohttp_wrappers import DLRequestBase
from bi_api_commons.logging import CompositeLoggingContextController, RequestLoggingContextController


def get_root_logging_context_controller(request: web.Request) -> CompositeLoggingContextController:
    key = DLRequestBase.KEY_LOG_CTX_CONTROLLER

    if key in request:
        composite_logging_controller = request[key]
        assert isinstance(composite_logging_controller, CompositeLoggingContextController)
    else:
        composite_logging_controller = CompositeLoggingContextController()
        request[key] = composite_logging_controller

    return composite_logging_controller


def add_logging_ctx_controller(request: web.Request, ctrl: RequestLoggingContextController) -> None:
    """
    Adds to current AIOHTTP request new request logging context controller.
    If composite controller is not created for current request it will be created automatically.
    """
    composite_logging_controller = get_root_logging_context_controller(request)
    composite_logging_controller.add_sub_controller(ctrl)


_endpoint_code_attr_name = "endpoint_code"


def get_endpoint_code(request: web.Request) -> Optional[str]:
    final_handler = request.match_info.handler

    # TODO FIX: Change to base view subclass check after https://st.yandex-team.ru/BI-1506
    if hasattr(final_handler, _endpoint_code_attr_name):
        maybe_endpoint_code = getattr(final_handler, _endpoint_code_attr_name)
        if isinstance(maybe_endpoint_code, str):
            return maybe_endpoint_code

    return None
