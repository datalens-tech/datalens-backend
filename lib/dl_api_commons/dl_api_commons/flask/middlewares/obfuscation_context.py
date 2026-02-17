import flask

from dl_api_commons.flask.middlewares.commit_rci_middleware import ReqCtxInfoMiddleware
from dl_obfuscator import (
    OBFUSCATION_ENGINE_KEY,
    setup_request_obfuscation,
    teardown_request_obfuscation,
)


class ObfuscationContextMiddleware:
    def __init__(self, app: flask.Flask) -> None:
        self._app = app

    def before_request(self) -> None:
        engine = self._app.config.get(OBFUSCATION_ENGINE_KEY)
        rci = ReqCtxInfoMiddleware.get_last_resort_rci()
        secret_keeper = rci.secret_keeper if rci is not None else None

        setup_request_obfuscation(
            engine=engine,
            secret_keeper=secret_keeper,
        )

    def teardown_request(self, exception: BaseException | None) -> None:
        engine = self._app.config.get(OBFUSCATION_ENGINE_KEY)
        teardown_request_obfuscation(engine)


def setup_obfuscation_context_middleware(app: flask.Flask) -> None:
    middleware = ObfuscationContextMiddleware(app)
    app.before_request(middleware.before_request)
    app.teardown_request(middleware.teardown_request)