import abc
from typing import (
    Generic,
    TypeVar,
)

from aiohttp import web
from aiohttp.typedefs import Middleware
import attr

from dl_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aio.middlewares.tracing import TracingService
from dl_api_commons.sentry_config import (
    SentryConfig,
    configure_sentry_for_aiohttp,
)
from dl_auth_api_lib.error_handler import OAuthApiErrorHandler
from dl_auth_api_lib.oauth.google import GoogleOAuthClient
from dl_auth_api_lib.oauth.yandex import YandexOAuthClient
from dl_auth_api_lib.settings import (
    AuthAPISettings,
    register_auth_client,
)
from dl_auth_api_lib.views import google as google_views
from dl_auth_api_lib.views import snowflake as snowflake_views
from dl_auth_api_lib.views import yandex as yandex_views
from dl_core.aio.ping_view import PingView


_TSettings = TypeVar("_TSettings", bound=AuthAPISettings)


@attr.s(kw_only=True)
class OAuthApiAppFactory(Generic[_TSettings], abc.ABC):
    _settings: _TSettings = attr.ib()

    @abc.abstractmethod
    def get_auth_middlewares(self) -> list[Middleware]:
        raise NotImplementedError()

    def set_up_sentry(self, secret_sentry_dsn: str, release: str | None) -> None:
        configure_sentry_for_aiohttp(
            SentryConfig(
                dsn=secret_sentry_dsn,
                release=release,
            )
        )

    def create_app(self, app_version: str | None = None) -> web.Application:
        if (secret_sentry_dsn := self._settings.sentry_dsn) is not None:
            self.set_up_sentry(secret_sentry_dsn, app_version)

        req_id_service = RequestId()

        error_handler = OAuthApiErrorHandler(
            use_sentry=(secret_sentry_dsn is not None),
            sentry_app_name_tag="auth-api",
        )

        middleware_list = [
            TracingService().middleware,
            RequestBootstrap(
                req_id_service=req_id_service,
                error_handler=error_handler,
            ).middleware,
            *self.get_auth_middlewares(),
            commit_rci_middleware(),
        ]

        app = web.Application(
            middlewares=middleware_list,
        )

        app.router.add_route("get", "/oauth/ping", PingView)

        app.router.add_route("get", "/oauth/uri/yandex", yandex_views.YandexURIView)
        app.router.add_route("post", "/oauth/token/yandex", yandex_views.YandexTokenView)

        app.router.add_route("get", "/oauth/uri/google", google_views.GoogleURIView)
        app.router.add_route("post", "/oauth/token/google", google_views.GoogleTokenView)

        app.router.add_route("get", "/oauth/uri/snowflake", snowflake_views.SnowflakeURIView)
        app.router.add_route("post", "/oauth/token/snowflake", snowflake_views.SnowflakeTokenView)

        app["settings"] = self._settings
        app["clients"] = self._settings.auth_clients

        return app


register_auth_client("yandex", YandexOAuthClient)
register_auth_client("google", GoogleOAuthClient)
