from __future__ import annotations

import functools
from typing import Any, TypeVar, Callable

import flask
from bi_api_commons.logging import CompositeLoggingContextController, LogRequestLoggingContextController
from bi_api_commons.flask.middlewares.context_var_middleware import ContextVarMiddleware


class RequestLoggingContextControllerMiddleWare:
    _G_ATTR_NAME = "_bi_logging_ctx_controller"
    _APP_INIT_FLAG = "_bi_middleware_flag_req_log_ctx_ctrl"

    def set_up(self, app: flask.Flask) -> None:
        assert ContextVarMiddleware.is_wrapping_app(app), \
            "ContextVarMiddleware should be initialized before RequestLoggingContextControllerMiddleWare"
        app.before_request(self._bind_logging_request_controller_to_request)
        setattr(app, self._APP_INIT_FLAG, True)

    def _bind_logging_request_controller_to_request(self) -> None:
        setattr(flask.g, self._G_ATTR_NAME, CompositeLoggingContextController([
            LogRequestLoggingContextController(),
        ]))

    @classmethod
    def is_initialized_for_app(cls, app: flask.Flask) -> bool:
        return hasattr(app, cls._APP_INIT_FLAG)

    @classmethod
    def get_for_request(cls) -> CompositeLoggingContextController:
        if hasattr(flask.g, cls._G_ATTR_NAME):
            return getattr(flask.g, cls._G_ATTR_NAME)

        raise ValueError("Request logging context controller info was not created")


_RESULT_TV = TypeVar('_RESULT_TV')


def put_to_request_context(**ctx: Any) -> Callable[[Callable[..., _RESULT_TV]], Callable[..., _RESULT_TV]]:
    def real_decorator(wrapped: Callable[..., _RESULT_TV]) -> Callable[..., _RESULT_TV]:
        @functools.wraps(wrapped)
        def wrapper(*args: Any, **kwargs: Any) -> _RESULT_TV:
            for ctx_key, ctx_value in ctx.items():
                RequestLoggingContextControllerMiddleWare.get_for_request().put_to_context(ctx_key, ctx_value)
            return wrapped(*args, **kwargs)

        return wrapper

    return real_decorator
