import flask

from dl_api_commons.exc import FlaskRCINotSet
from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from dl_obfuscator import (
    OBFUSCATION_BASE_OBFUSCATORS_KEY,
    create_request_engine,
)
from dl_obfuscator.profiling import (
    clear_log_format_profiling,
    dump_log_format_profiling,
    init_log_format_profiling,
)
from dl_obfuscator.request_context import (
    clear_request_obfuscation_engine,
    set_request_obfuscation_engine,
)


class ObfuscationContextMiddleware:
    def __init__(self, app: flask.Flask, log_format_profiling_enabled: bool = False) -> None:
        self._app = app
        self._profiling_enabled = log_format_profiling_enabled

    def before_request(self) -> None:
        if self._profiling_enabled:
            init_log_format_profiling()

        base_obfuscators = self._app.config.get(OBFUSCATION_BASE_OBFUSCATORS_KEY)
        if base_obfuscators is None:
            return

        try:
            rci = ReqCtxInfoMiddleware.get_temp_rci()
        except FlaskRCINotSet:
            rci = None

        secret_keeper = rci.secret_keeper if rci is not None else None

        engine = create_request_engine(
            base_obfuscators=base_obfuscators,
            secret_keeper=secret_keeper,
        )

        set_request_obfuscation_engine(engine)

        if rci is not None:
            ReqCtxInfoMiddleware.replace_temp_rci(rci.clone(obfuscation_engine=engine))

    def teardown_request(self, exception: BaseException | None) -> None:
        # clear() is a safety net: dump() is a no-op when call_count == 0 or context is None
        if self._profiling_enabled:
            dump_log_format_profiling()
            clear_log_format_profiling()
        clear_request_obfuscation_engine()


def setup_obfuscation_context_middleware(
    app: flask.Flask,
    log_format_profiling_enabled: bool = False,
) -> None:
    middleware = ObfuscationContextMiddleware(app, log_format_profiling_enabled)
    app.before_request(middleware.before_request)
    app.teardown_request(middleware.teardown_request)
