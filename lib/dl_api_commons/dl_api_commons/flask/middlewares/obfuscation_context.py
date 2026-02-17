import flask

from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from dl_obfuscator import (
    OBFUSCATION_BASE_OBFUSCATORS_KEY,
    create_request_engine,
)
from dl_obfuscator.request_context import (
    clear_request_obfuscation_engine,
    set_request_obfuscation_engine,
)


class ObfuscationContextMiddleware:
    def __init__(self, app: flask.Flask) -> None:
        self._app = app

    def before_request(self) -> None:
        base_obfuscators = self._app.config.get(OBFUSCATION_BASE_OBFUSCATORS_KEY)
        if base_obfuscators is None:
            return

        rci = ReqCtxInfoMiddleware.get_last_resort_rci()
        secret_keeper = rci.secret_keeper if rci is not None else None

        engine = create_request_engine(
            base_obfuscators=base_obfuscators,
            secret_keeper=secret_keeper,
        )

        set_request_obfuscation_engine(engine)

        if rci is not None:
            ReqCtxInfoMiddleware.replace_temp_rci(rci.clone(obfuscation_engine=engine))

    def teardown_request(self, exception: BaseException | None) -> None:
        clear_request_obfuscation_engine()


def setup_obfuscation_context_middleware(app: flask.Flask) -> None:
    middleware = ObfuscationContextMiddleware(app)
    app.before_request(middleware.before_request)
    app.teardown_request(middleware.teardown_request)
