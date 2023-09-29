import attr

from bi_api_commons_ya_team.aio.middlewares.blackbox_auth import blackbox_auth_middleware
from dl_api_commons.aio.middlewares.csrf import CSRFMiddleware
from dl_api_commons.aio.typing import AIOHTTPMiddleware
from dl_file_uploader_api_lib.app import FileUploaderApiAppFactory
from dl_file_uploader_api_lib.settings import FileUploaderAPISettings


class CSRFMiddlewareYT(CSRFMiddleware):
    USER_ID_COOKIES = ("yandexuid",)


@attr.s(kw_only=True)
class FileUploaderApiAppFactoryYT(FileUploaderApiAppFactory[FileUploaderAPISettings]):
    CSRF_MIDDLEWARE_CLS = CSRFMiddlewareYT

    def get_auth_middlewares(self) -> list[AIOHTTPMiddleware]:
        auth_mw_list: list[AIOHTTPMiddleware] = [
            blackbox_auth_middleware(),
        ]
        return auth_mw_list
