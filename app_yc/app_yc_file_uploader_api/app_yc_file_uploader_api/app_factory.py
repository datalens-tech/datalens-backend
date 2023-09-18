from typing import Sequence, Any

import attr

from bi_api_commons_ya_cloud.constants import YcTokenHeaderMode

from bi_api_commons_ya_cloud.aio.middlewares.yc_auth import YCAuthService
from bi_api_commons_ya_cloud.yc_access_control_model import AuthorizationModeYandexCloud
from bi_api_commons_ya_cloud.yc_auth import make_default_yc_auth_service_config

from dl_file_uploader_api_lib.app import FileUploaderApiAppFactory

from app_yc_file_uploader_api.app_settings import FileUploaderAPISettingsYC


@attr.s(kw_only=True)
class FileUploaderApiAppFactoryYC(FileUploaderApiAppFactory[FileUploaderAPISettingsYC]):
    def get_auth_middlewares(self) -> Sequence[Any]:
        auth_mw_list: Sequence[Any]
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

        return auth_mw_list
