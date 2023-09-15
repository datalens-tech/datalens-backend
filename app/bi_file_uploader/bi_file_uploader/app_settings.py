import json

import attr

from bi_api_lib_ya.app_settings import default_yc_auth_settings

from bi_configs.utils import app_type_env_var_converter

from bi_file_uploader_api_lib.settings import FileUploaderAPISettings


@attr.s(frozen=True)
class DefaultFileUploaderAPISettings(FileUploaderAPISettings):
    APP_TYPE: AppType = s_attrib(  # type: ignore
        "YENV_TYPE",
        is_app_cfg_type=True,
        env_var_converter=app_type_env_var_converter,
    )

    YC_AUTH_SETTINGS: Optional[YCAuthSettings] = s_attrib(  # type: ignore
        "YC_AUTH_SETTINGS", fallback_factory=default_yc_auth_settings,
    )
    SA_KEY_DATA: Optional[dict[str, str]] = s_attrib(  # type: ignore
        "SA_KEY_DATA", sensitive=True, missing=None, env_var_converter=json.loads,
    )
