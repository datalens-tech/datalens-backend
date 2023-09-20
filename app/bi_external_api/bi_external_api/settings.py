from typing import Optional

import attr

from bi_api_lib_ya.app_settings import (
    YCAuthSettings,
    default_yc_auth_settings,
)
from bi_defaults.yenv_type import (
    AppType,
    app_type_env_var_converter,
)
from bi_external_api.enums import ExtAPIType
from dl_configs.settings_loaders.meta_definition import s_attrib


def ext_api_type_env_var_converter(env_value: str) -> ExtAPIType:
    mapping = {"dc": ExtAPIType.DC, "unified_il": ExtAPIType.UNIFIED_NEBIUS_IL}
    try:
        return mapping[env_value]
    except KeyError:
        raise ValueError(f"Unknown external API type value: {env_value}")


@attr.s(frozen=True)
class ExternalAPISettings:
    APP_TYPE: AppType = s_attrib(  # type: ignore
        "YENV_TYPE",
        is_app_cfg_type=True,
        env_var_converter=app_type_env_var_converter,
    )
    API_TYPE: ExtAPIType = s_attrib(  # type: ignore
        "EXT_API_TYPE",
        env_var_converter=ext_api_type_env_var_converter,
        missing=ExtAPIType.CORE,
    )
    YC_AUTH_SETTINGS: Optional[YCAuthSettings] = s_attrib(
        "YC_AUTH_SETTINGS",
        fallback_factory=default_yc_auth_settings,  # type: ignore
    )
    DATASET_CONTROL_PLANE_API_BASE_URL: str = s_attrib("DATASET_CONTROL_PLANE_API_BASE_URL")  # type: ignore
    US_BASE_URL: str = s_attrib("US_BASE_URL", fallback_cfg_key="US_BASE_URL")  # type: ignore
    US_MASTER_TOKEN: Optional[str] = s_attrib("US_MASTER_TOKEN", missing=None, sensitive=True)  # type: ignore
    DASH_API_BASE_URL: Optional[str] = s_attrib("DASH_API_BASE_URL", missing=None)  # type: ignore
    CHARTS_API_BASE_URL: Optional[str] = s_attrib("CHARTS_API_BASE_URL", missing=None)  # type: ignore
    SENTRY_DSN: Optional[str] = s_attrib("SENTRY_DSN", missing=None)  # type: ignore

    DO_ADD_EXC_MESSAGE: bool = s_attrib(  # type: ignore
        "DO_ADD_EXC_MESSAGE",
        missing=True,
    )
    INT_API_CLI_FORCE_CLOSE_HTTP_CONN: bool = s_attrib(  # type: ignore
        "INT_API_CLI_FORCE_CLOSE_HTTP_CONN",
        missing=False,
    )


@attr.s(frozen=True)
class GrpcProxySettings:
    BIND_HOST: str = s_attrib(
        "EXT_API_GRPC_BIND_HOST",
    )  # type: ignore
    BIND_PORT: str = s_attrib(
        "EXT_API_GRPC_BIND_PORT",
    )  # type: ignore
    TARGET_ENDPOINT: str = s_attrib(
        "EXT_API_GRPC_TARGET_ENDPOINT",
    )  # type: ignore
    WORKERS_NUM: int = s_attrib(
        "EXT_API_GRPC_WORKERS_NUM",
        missing=10,
        env_var_converter=int,
    )  # type: ignore
    SENTRY_DSN: Optional[str] = s_attrib("SENTRY_DSN", missing=None)  # type: ignore
