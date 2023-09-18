import attr

from bi_api_commons_ya_cloud.constants import YcTokenHeaderMode
from dl_configs.enums import AppType

from dl_core.aio.middlewares.auth_trust_middleware import auth_trust_middleware
from dl_core.aio.middlewares.csrf import CSRFMiddleware

from dl_file_uploader_api_lib.app import FileUploaderApiAppFactory

from dl_api_commons.aio.typing import AIOHTTPMiddleware

from bi_api_commons_ya_cloud.aio.middlewares.yc_auth import YCAuthService
from bi_api_commons_ya_cloud.yc_access_control_model import AuthorizationModeYandexCloud, AuthorizationModeDataCloud
from bi_api_commons_ya_cloud.yc_auth import make_default_yc_auth_service_config
from bi_api_commons_ya_team.aio.middlewares.blackbox_auth import blackbox_auth_middleware

from bi_file_uploader.app_settings import DefaultFileUploaderAPISettings


class DefaultCSRFMiddleware(CSRFMiddleware):
    USER_ID_COOKIES = ('yandexuid',)


@attr.s(kw_only=True)
class LegacyFileUploaderApiAppFactory(FileUploaderApiAppFactory[DefaultFileUploaderAPISettings]):
    CSRF_MIDDLEWARE_CLS = DefaultCSRFMiddleware

    def get_auth_middlewares(self) -> list[AIOHTTPMiddleware]:
        app_type = self._settings.APP_TYPE

        auth_mw_list: list[AIOHTTPMiddleware]
        if app_type == AppType.INTRANET:
            auth_mw_list = [
                blackbox_auth_middleware(),
            ]
        elif app_type in (AppType.CLOUD, AppType.NEBIUS):
            yc_auth_settings = self._settings.YC_AUTH_SETTINGS
            assert yc_auth_settings
            yc_auth_service = YCAuthService(
                allowed_folder_ids=None,
                yc_token_header_mode=YcTokenHeaderMode.INTERNAL,
                authorization_mode=AuthorizationModeYandexCloud(
                    folder_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                    organization_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
                enable_cookie_auth=True,
                access_service_cfg=make_default_yc_auth_service_config(endpoint=yc_auth_settings.YC_AS_ENDPOINT),
                session_service_cfg=make_default_yc_auth_service_config(endpoint=yc_auth_settings.YC_SS_ENDPOINT),
                ss_sa_key_data=self._settings.SA_KEY_DATA,
                yc_ts_endpoint=yc_auth_settings.YC_TS_ENDPOINT,
            )
            auth_mw_list = [yc_auth_service.middleware]
        elif app_type == AppType.DATA_CLOUD:
            yc_auth_settings = self._settings.YC_AUTH_SETTINGS
            assert yc_auth_settings
            dc_yc_auth_service = YCAuthService(
                allowed_folder_ids=None,
                yc_token_header_mode=YcTokenHeaderMode.INTERNAL,
                authorization_mode=AuthorizationModeDataCloud(
                    project_permission_to_check=yc_auth_settings.YC_AUTHORIZE_PERMISSION,
                ),
                enable_cookie_auth=True,
                access_service_cfg=make_default_yc_auth_service_config(endpoint=yc_auth_settings.YC_AS_ENDPOINT),
                session_service_cfg=make_default_yc_auth_service_config(endpoint=yc_auth_settings.YC_SS_ENDPOINT),
                ss_sa_key_data=self._settings.SA_KEY_DATA,
                yc_ts_endpoint=yc_auth_settings.YC_TS_ENDPOINT,
            )
            auth_mw_list = [dc_yc_auth_service.middleware]
        elif app_type == AppType.TESTS:
            auth_mw_list = [
                auth_trust_middleware(
                    fake_user_id='_the_tests_asyncapp_user_id_',
                    fake_user_name='_the_tests_asyncapp_user_name_',
                )
            ]
        else:
            raise ValueError(f"Can not determine auth_mw_list due to unknown app type: {app_type}")

        return auth_mw_list
