from __future__ import annotations

import json
from typing import Optional, Any, ClassVar

import attr
from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig

from bi_configs.connectors_settings import ConnectorsSettingsByType, connectors_settings_fallback_factory
from bi_configs.crypto_keys import CryptoKeysConfig
from bi_configs.enums import EnvType, AppType, RedisMode
from bi_configs.environments import (
    CommonInstallation,
    CommonExternalInstallation,
)
from bi_configs.rqe import RQEConfig
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.loader_env import NOT_SET
from bi_configs.settings_loaders.meta_definition import s_attrib, required
from bi_configs.settings_loaders.settings_obj_base import SettingsBase
from bi_configs.settings_submodels import RedisSettings, YCAuthSettings, default_yc_auth_settings
from bi_configs.utils import split_by_comma, app_type_env_var_converter, env_type_env_var_converter
from bi_constants.enums import USAuthMode

from bi_api_commons.base_models import TenantDef
from bi_cloud_integration.sa_creds import SACredsMode
from bi_core.components.ids import FieldIdGeneratorType
from bi_formula.parser.factory import ParserType


@attr.s(frozen=True)
class CachesTTLSettings(SettingsBase):
    MATERIALIZED: Optional[int] = s_attrib("SEC_MATERIALIZED_DATASET")  # type: ignore  # TODO: remove
    OTHER: Optional[int] = s_attrib("SEC_OTHER")  # type: ignore  # TODO: fix


@attr.s(frozen=True)
class MDBSettings(SettingsBase):
    DOMAINS: tuple[str, ...] = s_attrib("DOMAINS", missing_factory=tuple, env_var_converter=split_by_comma)  # type: ignore  # TODO: fix
    CNAME_DOMAINS: tuple[str, ...] = s_attrib("CNAME_DOMAINS", missing_factory=tuple, env_var_converter=split_by_comma)  # type: ignore  # TODO: fix
    MANAGED_NETWORK_ENABLED: bool = s_attrib("MANAGED_NETWORK_ENABLED", missing=True)  # type: ignore  # TODO: fix
    MANAGED_NETWORK_REMAP: dict[str, str] = s_attrib(  # type: ignore  # TODO: fix
        "MANAGED_NETWORK_REMAP", missing_factory=dict, env_var_converter=json.loads
    )


def _list_to_tuple(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(value)
    return value


# TODO SPLIT: COMMON

@attr.s(frozen=True)
class AppSettings:
    APP_TYPE: AppType = s_attrib(  # type: ignore  # TODO SPLIT: remove after SupportedFunctionsManager stops using it
        "YENV_TYPE",
        is_app_cfg_type=True,
        env_var_converter=app_type_env_var_converter,
    )

    BLEEDING_EDGE_USERS: tuple[str, ...] = s_attrib(  # type: ignore
        "DL_BLEEDING_EDGE_USERS",
        env_var_converter=split_by_comma,
        missing=(),
    )

    MDB: MDBSettings = s_attrib("MDB", missing_factory=MDBSettings)  # type: ignore

    SENTRY_ENABLED: bool = s_attrib("DL_SENTRY_ENABLED", missing=False)  # type: ignore
    SENTRY_DSN: Optional[str] = s_attrib("DL_SENTRY_DSN", missing=None)  # type: ignore

    CRYPTO_KEYS_CONFIG: CryptoKeysConfig = s_attrib(  # type: ignore
        "DL_CRY",
        json_converter=CryptoKeysConfig.from_json,
        sensitive=True,
    )
    US_BASE_URL: str = s_attrib("US_HOST", fallback_cfg_key="US_BASE_URL")  # type: ignore
    US_MASTER_TOKEN: Optional[str] = s_attrib("US_MASTER_TOKEN", sensitive=True, missing=None)  # type: ignore

    RQE_FORCE_OFF: bool = s_attrib("RQE_FORCE_OFF", missing=False)  # type: ignore
    RQE_CONFIG: RQEConfig = s_attrib("RQE", fallback_factory=RQEConfig.get_default)  # type: ignore
    RQE_CACHES_ON: bool = s_attrib("RQE_CACHES_ON", missing=False)  # type: ignore
    RQE_CACHES_TTL: int = s_attrib("RQE_CACHES_TTL", missing=60 * 10)  # type: ignore
    RQE_CACHES_REDIS: Optional[RedisSettings] = s_attrib(  # type: ignore
        "RQE_CACHES_REDIS",
        fallback_factory=(
            lambda cfg: RedisSettings(  # type: ignore  # TODO: fix
                MODE=RedisMode.single_host,
                CLUSTER_NAME=cfg.REDIS_RQE_CACHES_CLUSTER_NAME,
                HOSTS=_list_to_tuple(cfg.REDIS_RQE_CACHES_HOSTS),
                PORT=cfg.REDIS_RQE_CACHES_PORT,
                SSL=cfg.REDIS_RQE_CACHES_SSL,
                DB=cfg.REDIS_RQE_CACHES_DB,
                PASSWORD=required(str),
            ) if isinstance(cfg, CommonExternalInstallation) or (isinstance(cfg, ObjectLikeConfig) and cfg.get('REDIS_RQE_CACHES_CLUSTER_NAME')) else None
        ),
        missing=None,
    )

    SAMPLES_CH_HOSTS: tuple[str, ...] = s_attrib(  # type: ignore  # TODO SPLIT: this has to go at some point
        "SAMPLES_CH_HOST",
        env_var_converter=split_by_comma,
        missing_factory=list
    )

    BI_COMPENG_PG_ON: bool = s_attrib("BI_COMPENG_PG_ON", missing=True)  # type: ignore
    BI_COMPENG_PG_URL: Optional[str] = s_attrib("BI_COMPENG_PG_URL", missing=None)  # type: ignore

    FORMULA_PARSER_TYPE: Optional[ParserType] = s_attrib(  # type: ignore
        "BI_FORMULA_PARSER_TYPE",
        env_var_converter=lambda s: ParserType[s.lower()],
        missing=ParserType.antlr_py,
    )

    CONNECTORS: Optional[ConnectorsSettingsByType] = s_attrib(  # type: ignore
        "CONNECTORS",
        fallback_factory=connectors_settings_fallback_factory,
    )

    FIELD_ID_GENERATOR_TYPE: FieldIdGeneratorType = s_attrib(  # type: ignore
        "FIELD_ID_GENERATOR_TYPE",
        env_var_converter=lambda s: FieldIdGeneratorType[s.lower()],
        missing=FieldIdGeneratorType.readable,
    )

    REDIS_ARQ: Optional[RedisSettings] = None

    FILE_UPLOADER_BASE_URL: Optional[str] = s_attrib(  # type: ignore
        "FILE_UPLOADER_BASE_URL", fallback_cfg_key="DATALENS_API_LB_UPLOADS_BASE_URL", missing=None,
    )
    FILE_UPLOADER_MASTER_TOKEN: Optional[str] = s_attrib(  # type: ignore
        "FILE_UPLOADER_MASTER_TOKEN", sensitive=True, missing=None,
    )

    DEFAULT_LOCALE: Optional[str] = 'en'


@attr.s(frozen=True)
class ControlApiAppSettings(AppSettings):
    DO_DSRC_IDX_FETCH: bool = s_attrib("DL_DO_DS_IDX_FETCH", missing=False)  # type: ignore

    REDIS_ARQ: Optional[RedisSettings] = s_attrib(  # type: ignore
        "REDIS_ARQ",
        fallback_factory=(
            lambda cfg: RedisSettings(  # type: ignore
                MODE=RedisMode(cfg.REDIS_PERSISTENT_MODE),
                CLUSTER_NAME=cfg.REDIS_PERSISTENT_CLUSTER_NAME,
                HOSTS=_list_to_tuple(cfg.REDIS_PERSISTENT_HOSTS),
                PORT=cfg.REDIS_PERSISTENT_PORT,
                SSL=cfg.REDIS_PERSISTENT_SSL,
                DB=cfg.REDIS_FILE_UPLOADER_TASKS_DB,
                PASSWORD=required(str),
            ) if isinstance(cfg, CommonInstallation) or (isinstance(cfg, ObjectLikeConfig) and cfg.get('REDIS_PERSISTENT_MODE')) else None
        ),
        missing=None,
    )

    app_prefix: ClassVar[str] = 'a'


@attr.s(frozen=True)
class DataApiAppSettings(AppSettings):
    COMMON_TIMEOUT_SEC: int = s_attrib("COMMON_TIMEOUT_SEC", missing=90)  # type: ignore

    CACHES_ON: bool = s_attrib("CACHES_ON", missing=True)  # type: ignore
    CACHES_REDIS: Optional[RedisSettings] = s_attrib(  # type: ignore
        "CACHES_REDIS",
        fallback_factory=(
            lambda cfg: RedisSettings(  # type: ignore
                MODE=RedisMode.sentinel,
                CLUSTER_NAME=cfg.REDIS_CACHES_CLUSTER_NAME,
                HOSTS=_list_to_tuple(cfg.REDIS_CACHES_HOSTS),
                PORT=cfg.REDIS_CACHES_PORT,
                SSL=cfg.REDIS_CACHES_SSL,
                DB=cfg.REDIS_CACHES_DB,
                PASSWORD=required(str),
            ) if isinstance(cfg, CommonInstallation) or (isinstance(cfg, ObjectLikeConfig) and cfg.get('REDIS_CACHES_CLUSTER_NAME')) else None
        ),
        missing=None,
    )
    MUTATIONS_CACHES_ON: bool = s_attrib("MUTATIONS_CACHES_ON", missing=False)  # type: ignore
    MUTATIONS_CACHES_DEFAULT_TTL: float = s_attrib("MUTATIONS_CACHES_DEFAULT_TTL", missing=3 * 60 * 60)  # type: ignore
    MUTATIONS_REDIS: Optional[RedisSettings] = s_attrib(
        "MUTATIONS_REDIS",
        fallback_factory=(
            lambda cfg: RedisSettings(  # type: ignore
                MODE=RedisMode.sentinel,
                CLUSTER_NAME=cfg.REDIS_CACHES_CLUSTER_NAME,
                HOSTS=_list_to_tuple(cfg.REDIS_CACHES_HOSTS),
                PORT=cfg.REDIS_CACHES_PORT,
                SSL=cfg.REDIS_CACHES_SSL,
                DB=cfg.REDIS_MUTATIONS_CACHES_DB,
                PASSWORD=required(str),
            ) if isinstance(cfg, CommonInstallation) or (isinstance(cfg, ObjectLikeConfig) and cfg.get('REDIS_CACHES_CLUSTER_NAME')) else None
        ),
        missing=None,
    )
    CACHES_TTL_SETTINGS: Optional[CachesTTLSettings] = s_attrib(  # type: ignore
        "CACHES_TTL",
        fallback_factory=lambda: CachesTTLSettings(  # type: ignore
            MATERIALIZED=3600,
            OTHER=300,
        ),
        missing=None,
    )

    BI_ASYNC_APP_DISABLE_KEEPALIVE: bool = s_attrib("BI_ASYNC_APP_DISABLE_KEEPALIVE", missing=False)  # type: ignore  # TODO: fix

    @property
    def app_name(self) -> str:  # TODO SPLIT: make it a setting
        return 'bi_data_api'

    @property
    def jaeger_service_name(self) -> str:  # TODO SPLIT: make it a setting
        return 'bi-data-api'

    app_prefix: ClassVar[str] = 'y'

# # #


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

# # #


# TODO SPLIT: LEGACY (All in one) (Currently used)

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

    MDB: MDBSettings = s_attrib(  # type: ignore  # TODO: fix
        "MDB",
        missing_factory=MDBSettings  # type: ignore  # TODO: fix
    )

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

    SAMPLES_CH_HOSTS: tuple[str, ...] = s_attrib(  # type: ignore  # TODO: fix
        "SAMPLES_CH_HOST",
        env_var_converter=split_by_comma,
        missing_factory=list
    )

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
class AsyncAppSettings(BaseAppSettings):
    COMMON_TIMEOUT_SEC: int = s_attrib("COMMON_TIMEOUT_SEC", missing=90)  # type: ignore  # TODO: fix

    # Caches
    CACHES_ON: bool = s_attrib("CACHES_ON", missing=True)  # type: ignore  # TODO: fix
    CACHES_REDIS: Optional[RedisSettings] = s_attrib(  # type: ignore  # TODO: fix
        "CACHES_REDIS",
        fallback_factory=(
            lambda cfg: RedisSettings(  # type: ignore  # TODO: fix
                MODE=RedisMode.sentinel,
                CLUSTER_NAME=cfg.REDIS_CACHES_CLUSTER_NAME,
                HOSTS=_list_to_tuple(cfg.REDIS_CACHES_HOSTS),
                PORT=cfg.REDIS_CACHES_PORT,
                SSL=cfg.REDIS_CACHES_SSL,
                DB=cfg.REDIS_CACHES_DB,
                PASSWORD=required(str),
            ) if isinstance(cfg, CommonInstallation) or (isinstance(cfg, ObjectLikeConfig) and cfg.get('REDIS_CACHES_CLUSTER_NAME')) else None
        ),
        missing=None,
    )
    MUTATIONS_CACHES_ON: bool = s_attrib("MUTATIONS_CACHES_ON", missing=False)  # type: ignore  # TODO: fix
    MUTATIONS_CACHES_DEFAULT_TTL: float = s_attrib("MUTATIONS_CACHES_DEFAULT_TTL", missing=3 * 60 * 60)  # type: ignore
    MUTATIONS_REDIS: Optional[RedisSettings] = s_attrib(
        "MUTATIONS_REDIS",
        fallback_factory=(
            lambda cfg: RedisSettings(  # type: ignore  # TODO: fix
                MODE=RedisMode.sentinel,
                CLUSTER_NAME=cfg.REDIS_CACHES_CLUSTER_NAME,
                HOSTS=_list_to_tuple(cfg.REDIS_CACHES_HOSTS),
                PORT=cfg.REDIS_CACHES_PORT,
                SSL=cfg.REDIS_CACHES_SSL,
                DB=cfg.REDIS_MUTATIONS_CACHES_DB,
                PASSWORD=required(str),
            ) if isinstance(cfg, CommonInstallation) or (isinstance(cfg, ObjectLikeConfig) and cfg.get('REDIS_CACHES_CLUSTER_NAME')) else None
        ),
        missing=None,
    )
    CACHES_TTL_SETTINGS: Optional[CachesTTLSettings] = s_attrib(  # type: ignore  # TODO: fix
        "CACHES_TTL",
        fallback_factory=lambda: CachesTTLSettings(  # type: ignore  # TODO: fix
            MATERIALIZED=3600,
            OTHER=300,
        ),
        missing=None,
    )

    # Public
    PUBLIC_API_KEY: Optional[str] = s_attrib("PUBLIC_API_KEY", sensitive=True, missing=None)  # type: ignore  # TODO: fix
    US_PUBLIC_API_TOKEN: Optional[str] = s_attrib("US_PUBLIC_API_TOKEN", sensitive=True, missing=None)  # type: ignore  # TODO: fix
    BI_ASYNC_APP_DISABLE_KEEPALIVE: bool = s_attrib("BI_ASYNC_APP_DISABLE_KEEPALIVE", missing=False)  # type: ignore  # TODO: fix

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
class ControlPlaneAppSettings(BaseAppSettings):
    ENV_TYPE: Optional[EnvType] = s_attrib(  # type: ignore
        "YENV_TYPE",
        env_var_converter=env_type_env_var_converter,
    )

    CONNECTOR_AVAILABILITY: ConnectorAvailabilityConfig = s_attrib(  # type: ignore
        "CONNECTOR_AVAILABILITY",
        fallback_factory=lambda cfg: ConnectorAvailabilityConfig.from_settings(cfg.CONNECTOR_AVAILABILITY),
    )

    DO_DSRC_IDX_FETCH: bool = s_attrib("DL_DO_DS_IDX_FETCH", missing=False)  # type: ignore  # TODO: fix

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

    REDIS_ARQ: Optional[RedisSettings] = s_attrib(  # type: ignore  # TODO: fix
        "REDIS_ARQ",
        fallback_factory=(
            lambda cfg: RedisSettings(  # type: ignore  # TODO: fix
                MODE=RedisMode(cfg.REDIS_PERSISTENT_MODE),
                CLUSTER_NAME=cfg.REDIS_PERSISTENT_CLUSTER_NAME,
                HOSTS=_list_to_tuple(cfg.REDIS_PERSISTENT_HOSTS),
                PORT=cfg.REDIS_PERSISTENT_PORT,
                SSL=cfg.REDIS_PERSISTENT_SSL,
                DB=cfg.REDIS_FILE_UPLOADER_TASKS_DB,
                PASSWORD=required(str),
            ) if isinstance(cfg, CommonInstallation) or (isinstance(cfg, ObjectLikeConfig) and cfg.get('REDIS_PERSISTENT_MODE')) else None
        ),
        missing=None,
    )

    app_prefix: ClassVar[str] = 'a'

# # #


@attr.s(frozen=True)
class TestAppSettings:
    use_bb_in_test: bool = attr.ib(kw_only=True, default=False)
    tvm_info: Optional[str] = attr.ib(kw_only=True, default=None, repr=False)


@attr.s(frozen=True, kw_only=True)
class ControlPlaneAppTestingsSettings:
    us_auth_mode_override: Optional[USAuthMode] = attr.ib(default=None)
    fake_tenant: Optional[TenantDef] = attr.ib(default=None)
