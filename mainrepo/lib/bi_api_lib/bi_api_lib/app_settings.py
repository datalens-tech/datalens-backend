from __future__ import annotations

import json
from typing import Optional, Any, ClassVar

import attr
from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig

from bi_configs.crypto_keys import CryptoKeysConfig
from bi_configs.enums import RedisMode
from bi_configs.environments import (
    CommonInstallation,
    CommonExternalInstallation,
)
from bi_configs.rqe import RQEConfig
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import s_attrib, required
from bi_configs.settings_loaders.settings_obj_base import SettingsBase
from bi_configs.settings_submodels import RedisSettings
from bi_configs.utils import split_by_comma
from bi_constants.enums import USAuthMode

from bi_api_commons.base_models import TenantDef
from bi_core.components.ids import FieldIdGeneratorType
from bi_formula.parser.factory import ParserType


@attr.s(frozen=True)
class CachesTTLSettings(SettingsBase):
    MATERIALIZED: Optional[int] = s_attrib("SEC_MATERIALIZED_DATASET")  # type: ignore
    OTHER: Optional[int] = s_attrib("SEC_OTHER")  # type: ignore


@attr.s(frozen=True)
class MDBSettings(SettingsBase):
    DOMAINS: tuple[str, ...] = s_attrib("DOMAINS", missing_factory=tuple, env_var_converter=split_by_comma)  # type: ignore  # TODO: fix
    CNAME_DOMAINS: tuple[str, ...] = s_attrib("CNAME_DOMAINS", missing_factory=tuple, env_var_converter=split_by_comma)  # type: ignore
    MANAGED_NETWORK_ENABLED: bool = s_attrib("MANAGED_NETWORK_ENABLED", missing=True)  # type: ignore
    MANAGED_NETWORK_REMAP: dict[str, str] = s_attrib(  # type: ignore
        "MANAGED_NETWORK_REMAP", missing_factory=dict, env_var_converter=json.loads
    )


def _list_to_tuple(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(value)
    return value


@attr.s(frozen=True)
class AppSettings:
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
            lambda cfg: RedisSettings(  # type: ignore
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

    SAMPLES_CH_HOSTS: tuple[str, ...] = s_attrib(  # type: ignore
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
    FORMULA_SUPPORTED_FUNC_TAGS: tuple[str] = s_attrib(
        "FORMULA_SUPPORTED_FUNC_TAGS",
        env_var_converter=split_by_comma,
        missing=('stable',),
    )

    CONNECTOR_WHITELIST: Optional[tuple[str, ...]] = s_attrib(  # type: ignore
        'CONNECTOR_WHITELIST',
        env_var_converter=split_by_comma,
        fallback_factory=lambda cfg: split_by_comma(cfg.CONNECTOR_WHITELIST) if hasattr(cfg, 'CONNECTOR_WHITELIST') else None
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

    CONNECTOR_AVAILABILITY: ConnectorAvailabilityConfig = s_attrib(  # type: ignore
        "CONNECTOR_AVAILABILITY",
        fallback_factory=lambda cfg: ConnectorAvailabilityConfig.from_settings(cfg.CONNECTOR_AVAILABILITY),
    )

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

    BI_ASYNC_APP_DISABLE_KEEPALIVE: bool = s_attrib("BI_ASYNC_APP_DISABLE_KEEPALIVE", missing=False)  # type: ignore

    @property
    def app_name(self) -> str:
        return 'bi_data_api'

    @property
    def jaeger_service_name(self) -> str:
        return 'bi-data-api'

    app_prefix: ClassVar[str] = 'y'


@attr.s(frozen=True)
class TestAppSettings:
    use_bb_in_test: bool = attr.ib(kw_only=True, default=False)
    tvm_info: Optional[str] = attr.ib(kw_only=True, default=None, repr=False)


@attr.s(frozen=True, kw_only=True)
class ControlApiAppTestingsSettings:
    us_auth_mode_override: Optional[USAuthMode] = attr.ib(default=None)
    fake_tenant: Optional[TenantDef] = attr.ib(default=None)
