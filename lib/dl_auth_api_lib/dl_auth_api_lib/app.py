import abc
from typing import (
    Generic,
    TypeVar,
)

from aiohttp import web
import attr

from dl_api_commons.aio.middlewares.commit_rci import commit_rci_middleware
from dl_api_commons.aio.middlewares.request_bootstrap import RequestBootstrap
from dl_api_commons.aio.middlewares.request_id import RequestId
from dl_api_commons.aio.middlewares.tracing import TracingService
from dl_api_commons.aio.typing import AIOHTTPMiddleware
from dl_api_commons.sentry_config import (
    SentryConfig,
    configure_sentry_for_aiohttp,
)
from dl_auth_api_lib.oauth.yandex import YandexOAuthClient
from dl_auth_api_lib.settings import (
    AuthAPISettings,
    register_auth_client,
)
from dl_auth_api_lib.views import yandex as yandex_views
from dl_core.aio.ping_view import PingView


_TSettings = TypeVar("_TSettings", bound=AuthAPISettings)


@attr.s(kw_only=True)
class OAuthApiAppFactory(Generic[_TSettings], abc.ABC):
    _settings: _TSettings = attr.ib()

    @abc.abstractmethod
    def get_auth_middlewares(self) -> list[AIOHTTPMiddleware]:
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

        middleware_list = [
            TracingService().middleware,
            RequestBootstrap(
                req_id_service=req_id_service,
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

        app["settings"] = self._settings
        app["clients"] = self._settings.auth_clients

        return app


register_auth_client("yandex", YandexOAuthClient)
