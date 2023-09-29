import json
from typing import Optional

import attr

from bi_api_lib_ya.app_settings import YCAuthSettings
from bi_defaults.environments import CommonExternalInstallation
from dl_configs.settings_loaders.meta_definition import s_attrib
from dl_file_uploader_api_lib.settings import FileUploaderAPISettings


def default_yc_auth_settings(cfg: CommonExternalInstallation) -> Optional[YCAuthSettings]:
    assert isinstance(cfg, CommonExternalInstallation) or hasattr(cfg, "YC_AUTHORIZE_PERMISSION")
    return YCAuthSettings(  # type: ignore
        YC_AUTHORIZE_PERMISSION=cfg.YC_AUTHORIZE_PERMISSION,
        YC_AS_ENDPOINT=cfg.YC_AS_ENDPOINT,
        YC_SS_ENDPOINT=cfg.YC_SS_ENDPOINT,
        YC_TS_ENDPOINT=cfg.YC_TS_ENDPOINT,
        YC_API_ENDPOINT_IAM=cfg.YC_API_ENDPOINT_IAM,
    )


@attr.s(frozen=True)
class FileUploaderAPISettingsNebius(FileUploaderAPISettings):
    YC_AUTH_SETTINGS: Optional[YCAuthSettings] = s_attrib(  # type: ignore
        "YC_AUTH_SETTINGS",
        fallback_factory=default_yc_auth_settings,
    )
    SA_KEY_DATA: Optional[dict[str, str]] = s_attrib(  # type: ignore
        "SA_KEY_DATA",
        sensitive=True,
        missing=None,
        env_var_converter=json.loads,
    )
