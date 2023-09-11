from __future__ import annotations

import json
from typing import Optional, Any

import attr

from bi_configs.enums import AppType, EnvType
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.loader_env import NOT_SET
from bi_configs.settings_loaders.meta_definition import s_attrib
from bi_configs.settings_loaders.settings_obj_base import SettingsBase
from bi_configs.settings_submodels import YCAuthSettings, default_yc_auth_settings
from bi_configs.utils import app_type_env_var_converter, env_type_env_var_converter
from bi_defaults.environments import CommonInstallation, CommonExternalInstallation

from bi_api_lib.app_settings import AppSettings, DataApiAppSettings, ControlApiAppSettings

from bi_cloud_integration.sa_creds import SACredsMode


@attr.s(frozen=True)
class BaseAppSettings(AppSettings):
    APP_TYPE: AppType = s_attrib(  # type: ignore
        "YENV_TYPE",
        is_app_cfg_type=True,
        env_var_converter=app_type_env_var_converter,
    )
    ENV_TYPE: Optional[EnvType] = None
    YC_BILLING_HOST: Optional[str] = s_attrib("YC_BILLING_HOST", fallback_cfg_key="YC_BILLING_HOST", missing=None)  # type: ignore

    # TODO: delete
    MDB_FORCE_IGNORE_MANAGED_NETWORK: bool = s_attrib("DL_MDB_FORCE_IGNORE_MANAGED_NETWORK", missing=False)  # type: ignore  # TODO: fix

    CHYT_MIRRORING: Optional[CHYTMirroringConfig] = s_attrib("DL_CHYT_MIRRORING", enabled_key_name="ON", missing=None)  # type: ignore  # TODO: fix

    # Sentry
    @staticmethod
    def default_sentry_enabled(fallback_cfg: CommonInstallation):  # type: ignore  # TODO: fix
        if isinstance(fallback_cfg, CommonInstallation) or hasattr(fallback_cfg, 'SENTRY_ENABLED'):
            return fallback_cfg.SENTRY_ENABLED
        return NOT_SET

    @staticmethod
    def default_sentry_dsn(fallback_cfg: CommonInstallation, app_type: AppType):  # type: ignore  # TODO: fix
        if not isinstance(fallback_cfg, (ObjectLikeConfig, CommonInstallation)):
            return NOT_SET

        if app_type == AppType.CLOUD_PUBLIC:
            assert isinstance(fallback_cfg, (ObjectLikeConfig, CommonExternalInstallation))
            return fallback_cfg.SENTRY_DSN_PUBLIC_DATASET_API
        elif app_type == AppType.CLOUD_EMBED:
            assert isinstance(fallback_cfg, (ObjectLikeConfig, CommonExternalInstallation))
            return fallback_cfg.SENTRY_DSN_SEC_EMBEDS_DATASET_API
        else:
            return fallback_cfg.SENTRY_DSN_DATASET_API

    SENTRY_ENABLED: bool = s_attrib("DL_SENTRY_ENABLED", fallback_factory=default_sentry_enabled, missing=False)  # type: ignore  # TODO: fix
    SENTRY_DSN: Optional[str] = s_attrib("DL_SENTRY_DSN", fallback_factory=default_sentry_dsn, missing=None)  # type: ignore  # TODO: fix

    PUBLIC_CH_QUERY_TIMEOUT: Optional[int] = s_attrib(  # type: ignore  # TODO: fix
        "DL_PUBLIC_CH_QUERY_TIMEOUT",
        fallback_factory=lambda: 30,  # type: ignore  # TODO: fix  # previously it was None on direct instantiation but 30 in env loading default
        missing=None,
    )

    YC_AUTH_SETTINGS: Optional[YCAuthSettings] = s_attrib("YC_AUTH_SETTINGS", fallback_factory=default_yc_auth_settings)  # type: ignore  # TODO: fix
    YC_RM_CP_ENDPOINT: Optional[str] = s_attrib(  # type: ignore  # TODO: fix
        "YC_ENDPOINT_RM_CP",
        fallback_cfg_key="YC_API_ENDPOINT_RM",
        missing=None,
    )
    YC_IAM_TS_ENDPOINT: Optional[str] = s_attrib(  # type: ignore  # TODO: fix
        "YC_ENDPOINT_IAM_TS",
        fallback_cfg_key="YC_TS_ENDPOINT",
        missing=None,
    )

    BLACKBOX_NAME: Optional[str] = s_attrib("DL_BLACKBOX_NAME", fallback_cfg_key="BLACKBOX_NAME", missing=None)  # type: ignore  # TODO: fix

    YC_SA_CREDS_MODE: Optional[SACredsMode] = s_attrib(  # type: ignore  # TODO: fix
        "YC_SA_CREDS_MODE", env_var_converter=lambda s: SACredsMode[s.lower()], missing=SACredsMode.local_metadata
    )
    YC_SA_CREDS_KEY_DATA: Optional[dict[str, str]] = s_attrib(  # type: ignore  # TODO: fix
        "YC_SA_CREDS_KEY_DATA", missing=None, env_var_converter=json.loads
    )

    DLS_HOST: Optional[str] = None
    DLS_API_KEY: Optional[str] = None

    DEFAULT_LOCALE: Optional[str] = 'en'


@attr.s(frozen=True)
class AsyncAppSettings(BaseAppSettings, DataApiAppSettings):
    PUBLIC_API_KEY: Optional[str] = s_attrib("PUBLIC_API_KEY", sensitive=True, missing=None)  # type: ignore  # TODO: fix
    US_PUBLIC_API_TOKEN: Optional[str] = s_attrib("US_PUBLIC_API_TOKEN", sensitive=True, missing=None)  # type: ignore  # TODO: fix

    @property
    def app_name(self) -> str:
        if self.APP_TYPE == AppType.CLOUD_PUBLIC:
            return 'bi_public_data_api'
        if self.APP_TYPE in (AppType.DATA_CLOUD_EMBED, AppType.CLOUD_EMBED):
            return 'bi_sec_embeds_data_api'
        return 'bi_data_api'

    @property
    def jaeger_service_name(self) -> str:
        if self.APP_TYPE == AppType.CLOUD_PUBLIC:
            return 'bi-public-data-api'
        if self.APP_TYPE in (AppType.DATA_CLOUD_EMBED, AppType.CLOUD_EMBED):
            return 'bi-sec-embeds-data-api'
        return 'bi-data-api'

    @property
    def app_prefix(self) -> str:
        return 'p' if self.APP_TYPE == AppType.CLOUD_PUBLIC else 'y'


@attr.s(frozen=True)
class ControlPlaneAppSettings(BaseAppSettings, ControlApiAppSettings):
    ENV_TYPE: Optional[EnvType] = s_attrib(  # type: ignore
        "YENV_TYPE",
        env_var_converter=env_type_env_var_converter,
    )

    # BlackBox/Passport
    BLACKBOX_RETRY_PARAMS: dict[str, Any] = s_attrib(  # type: ignore  # TODO: fix
        "BLACKBOX_RETRY_TIMEOUT",
        env_var_converter=json.loads,
        missing_factory=dict
    )
    BLACKBOX_TIMEOUT: int = s_attrib("BLACKBOX_TIMEOUT", missing=1)  # type: ignore  # TODO: fix

    # DLS
    DLS_HOST: str = s_attrib("DLS_HOST", fallback_cfg_key="DATALENS_API_LB_DLS_BASE_URL", missing=None)  # type: ignore  # TODO: fix
    DLS_API_KEY: Optional[str] = s_attrib("DLS_API_KEY", missing=None)  # type: ignore  # TODO: fix


# TODO SPLIT: INTERNAL


@attr.s(frozen=True)
class CHYTMirroringConfig(SettingsBase):
    # Fraction of requests to send to mirroring.
    FRAC: float = s_attrib("FRAC", missing=0.0)  # type: ignore  # TODO: fix
    # Map (cluster, user clique alias) -> mirror clique alias
    # Example: [["hahn", "*user1_clique", "*mirror"], ["hahn", null, "*mirror"]]
    MAP: dict[tuple[str, Optional[str]], str] = s_attrib(  # type: ignore  # TODO: fix
        "MAP",
        env_var_converter=lambda s: {
            (mmap_item[0], mmap_item[1]): mmap_item[2]
            for mmap_item in json.loads(s)
        },
        missing_factory=dict,
    )
    # Number of seconds to wait for mirror response (for response logging only)
    REQ_TIMEOUT_SEC: float = s_attrib("REQ_TIMEOUT_SEC", missing=5.0)  # type: ignore  # TODO: fix


# TODO SPLIT YANDEX-TEAM


@attr.s(frozen=True)
class AppSettingsInternal(AppSettings):
    DLS_HOST: str = s_attrib("DLS_HOST", fallback_cfg_key="DATALENS_API_LB_DLS_BASE_URL", missing=None)  # type: ignore
    DLS_API_KEY: Optional[str] = s_attrib("DLS_API_KEY", missing=None)  # type: ignore


@attr.s(frozen=True)
class ControlApiAppSettingsInternal(AppSettingsInternal):
    BLACKBOX_RETRY_PARAMS: dict[str, Any] = s_attrib(  # type: ignore
        "BLACKBOX_RETRY_TIMEOUT",
        env_var_converter=json.loads,
        missing_factory=dict
    )
    BLACKBOX_TIMEOUT: int = s_attrib("BLACKBOX_TIMEOUT", missing=1)  # type: ignore


@attr.s(frozen=True)
class DataApiAppSettingsInternal(AppSettingsInternal):
    CHYT_MIRRORING: Optional[CHYTMirroringConfig] = s_attrib("DL_CHYT_MIRRORING", enabled_key_name="ON", missing=None)  # type: ignore
