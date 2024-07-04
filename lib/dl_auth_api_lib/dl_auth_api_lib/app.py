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
from dl_auth_api_lib.settings import AuthAPISettings
from dl_auth_api_lib.views import yandex as yandex_views
from dl_core.aio.ping_view import PingView


_TSettings = TypeVar("_TSettings", bound=AuthAPISettings)


@attr.s(kw_only=True)
class OAuthApiAppFactory(Generic[_TSettings], abc.ABC):
    _settings: _TSettings = attr.ib()

    @abc.abstractmethod
    def get_auth_middlewares(self) -> list[AIOHTTPMiddleware]:
        raise NotImplementedError()

    def create_app(self) -> web.Application:
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
