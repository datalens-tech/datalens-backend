import attr

from dl_api_commons.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from dl_api_commons.aio.middlewares.csrf import CSRFMiddleware
from dl_api_commons.aio.typing import AIOHTTPMiddleware
from dl_file_uploader_api_lib.app import FileUploaderApiAppFactory
from dl_file_uploader_api_lib.settings import FileUploaderAPISettings


class CSRFMiddlewareOS(CSRFMiddleware):
    USER_ID_COOKIES = ("yandexuid",)


@attr.s(kw_only=True)
class StandaloneFileUploaderApiAppFactory(FileUploaderApiAppFactory[FileUploaderAPISettings]):
    CSRF_MIDDLEWARE_CLS = CSRFMiddlewareOS

    def get_auth_middlewares(self) -> list[AIOHTTPMiddleware]:
        auth_mw_list: list[AIOHTTPMiddleware] = [
            auth_trust_middleware(
                fake_user_id="_user_id_",
                fake_user_name="_user_name_",
            )
        ]
        return auth_mw_list
