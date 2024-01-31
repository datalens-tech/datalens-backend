from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Optional,
)

import attr

from dl_api_commons.base_models import TenantDef
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_configs.crypto_keys import CryptoKeysConfig
from dl_configs.enums import RedisMode
from dl_configs.environments import is_setting_applicable
from dl_configs.rqe import RQEConfig
from dl_configs.settings_loaders.meta_definition import (
    required,
    s_attrib,
)
from dl_configs.settings_loaders.settings_obj_base import SettingsBase
from dl_configs.settings_submodels import RedisSettings
from dl_configs.utils import (
    get_root_certificates_path,
    split_by_comma,
)
from dl_constants.enums import (
    DataPivotEngineType,
    QueryProcessingMode,
    USAuthMode,
)
from dl_core.components.ids import FieldIdGeneratorType
from dl_formula.parser.factory import ParserType
from dl_pivot_pandas.pandas.constants import PIVOT_ENGINE_TYPE_PANDAS


@attr.s(frozen=True)
class CachesTTLSettings(SettingsBase):
    MATERIALIZED: Optional[int] = s_attrib("SEC_MATERIALIZED_DATASET")  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "int | None")  [assignment]
    OTHER: Optional[int] = s_attrib("SEC_OTHER")  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "int | None")  [assignment]


def _list_to_tuple(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(value)
    return value


@attr.s(frozen=True)
class AppSettings:
    BLEEDING_EDGE_USERS: tuple[str, ...] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "tuple[str, ...]")  [assignment]
        "DL_BLEEDING_EDGE_USERS",
        env_var_converter=split_by_comma,
        missing=(),
    )

    SENTRY_ENABLED: bool = s_attrib("DL_SENTRY_ENABLED", missing=False)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool")  [assignment]
    SENTRY_DSN: Optional[str] = s_attrib("DL_SENTRY_DSN", missing=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]

    CRYPTO_KEYS_CONFIG: CryptoKeysConfig = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "CryptoKeysConfig")  [assignment]
        "DL_CRY",
        json_converter=CryptoKeysConfig.from_json,
        sensitive=True,
    )
    US_BASE_URL: str = s_attrib("US_HOST", fallback_cfg_key="US_BASE_URL")  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]
    US_MASTER_TOKEN: Optional[str] = s_attrib("US_MASTER_TOKEN", sensitive=True, missing=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]

    RQE_FORCE_OFF: bool = s_attrib("RQE_FORCE_OFF", missing=False)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool")  [assignment]
    RQE_CONFIG: RQEConfig = s_attrib("RQE", fallback_factory=RQEConfig.get_default)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "RQEConfig")  [assignment]
    RQE_CACHES_ON: bool = s_attrib("RQE_CACHES_ON", missing=False)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool")  [assignment]
    RQE_CACHES_TTL: int = s_attrib("RQE_CACHES_TTL", missing=60 * 10)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "int")  [assignment]
    RQE_CACHES_REDIS: Optional[RedisSettings] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "RedisSettings | None")  [assignment]
        "RQE_CACHES_REDIS",
        fallback_factory=(
            lambda cfg: RedisSettings(  # type: ignore  # 2024-01-30 # TODO: Unexpected keyword argument "MODE" for "RedisSettings"  [call-arg]
                MODE=RedisMode.single_host,
                CLUSTER_NAME=cfg.REDIS_RQE_CACHES_CLUSTER_NAME,
                HOSTS=_list_to_tuple(cfg.REDIS_RQE_CACHES_HOSTS),
                PORT=cfg.REDIS_RQE_CACHES_PORT,
                SSL=cfg.REDIS_RQE_CACHES_SSL,
                DB=cfg.REDIS_RQE_CACHES_DB,
                PASSWORD=required(str),
            )
            if is_setting_applicable(cfg, "REDIS_RQE_CACHES_DB")
            else None
        ),
        missing=None,
    )

    SAMPLES_CH_HOSTS: tuple[str, ...] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "tuple[str, ...]")  [assignment]
        "SAMPLES_CH_HOST", env_var_converter=split_by_comma, missing_factory=list
    )

    BI_COMPENG_PG_ON: bool = s_attrib("BI_COMPENG_PG_ON", missing=True)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool")  [assignment]
    BI_COMPENG_PG_URL: Optional[str] = s_attrib("BI_COMPENG_PG_URL", missing=None)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]

    FORMULA_PARSER_TYPE: Optional[ParserType] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "ParserType | None")  [assignment]
        "BI_FORMULA_PARSER_TYPE",
        env_var_converter=lambda s: ParserType[s.lower()],
        missing=ParserType.antlr_py,
    )
    FORMULA_SUPPORTED_FUNC_TAGS: tuple[str] = s_attrib(  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "tuple[str]")  [assignment]
        "FORMULA_SUPPORTED_FUNC_TAGS",
        env_var_converter=split_by_comma,
        missing=("stable",),
    )

    BI_API_CONNECTOR_WHITELIST: Optional[list[str]] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "list[str] | None")  [assignment]
        "BI_API_CONNECTOR_WHITELIST",
        env_var_converter=lambda s: list(split_by_comma(s)),
        fallback_cfg_key="BI_API_CONNECTOR_WHITELIST",
        missing=None,
    )
    CORE_CONNECTOR_WHITELIST: Optional[list[str]] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "list[str] | None")  [assignment]
        "CORE_CONNECTOR_WHITELIST",
        env_var_converter=lambda s: list(split_by_comma(s)),
        fallback_cfg_key="CORE_CONNECTOR_WHITELIST",
        missing=None,
    )

    FIELD_ID_GENERATOR_TYPE: FieldIdGeneratorType = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "FieldIdGeneratorType")  [assignment]
        "FIELD_ID_GENERATOR_TYPE",
        env_var_converter=lambda s: FieldIdGeneratorType[s.lower()],
        missing=FieldIdGeneratorType.readable,
    )

    REDIS_ARQ: Optional[RedisSettings] = None

    FILE_UPLOADER_BASE_URL: Optional[str] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]
        "FILE_UPLOADER_BASE_URL",
        fallback_cfg_key="DATALENS_API_LB_UPLOADS_BASE_URL",
        missing=None,
    )
    FILE_UPLOADER_MASTER_TOKEN: Optional[str] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str | None")  [assignment]
        "FILE_UPLOADER_MASTER_TOKEN",
        sensitive=True,
        missing=None,
    )

    DEFAULT_LOCALE: Optional[str] = "en"

    QUERY_PROCESSING_MODE: QueryProcessingMode = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "QueryProcessingMode")  [assignment]
        "QUERY_PROCESSING_MODE",
        env_var_converter=lambda s: QueryProcessingMode[s.lower()],
        missing=QueryProcessingMode.basic,
    )
    CA_FILE_PATH: str = s_attrib("CA_FILE_PATH", missing=get_root_certificates_path())  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]

    PIVOT_ENGINE_TYPE: Optional[DataPivotEngineType] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "DataPivotEngineType | None")  [assignment]
        "PIVOT_ENGINE_TYPE",
        env_var_converter=lambda s: DataPivotEngineType[s.lower()],
        missing=PIVOT_ENGINE_TYPE_PANDAS,  # TODO: Switch to another default
    )


@attr.s(frozen=True)
class ControlApiAppSettings(AppSettings):
    DO_DSRC_IDX_FETCH: bool = s_attrib("DL_DO_DS_IDX_FETCH", missing=False)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool")  [assignment]

    CONNECTOR_AVAILABILITY: ConnectorAvailabilityConfig = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "ConnectorAvailabilityConfig")  [assignment]
        "CONNECTOR_AVAILABILITY",
        fallback_factory=lambda cfg: ConnectorAvailabilityConfig.from_settings(cfg.CONNECTOR_AVAILABILITY),
    )

    REDIS_ARQ: Optional[RedisSettings] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "RedisSettings | None")  [assignment]
        # TODO: move this values to a separate key
        "REDIS_ARQ",
        fallback_factory=(
            lambda cfg: RedisSettings(  # type: ignore  # 2024-01-30 # TODO: Unexpected keyword argument "MODE" for "RedisSettings"  [call-arg]
                MODE=RedisMode(cfg.REDIS_PERSISTENT_MODE),
                CLUSTER_NAME=cfg.REDIS_PERSISTENT_CLUSTER_NAME,
                HOSTS=_list_to_tuple(cfg.REDIS_PERSISTENT_HOSTS),
                PORT=cfg.REDIS_PERSISTENT_PORT,
                SSL=cfg.REDIS_PERSISTENT_SSL,
                DB=cfg.REDIS_FILE_UPLOADER_TASKS_DB,
                PASSWORD=required(str),
            )
            if is_setting_applicable(cfg, "REDIS_PERSISTENT_CLUSTER_NAME")
            else None
        ),
        missing=None,
    )

    app_prefix: ClassVar[str] = "a"


@attr.s(frozen=True)
class DataApiAppSettings(AppSettings):
    COMMON_TIMEOUT_SEC: int = s_attrib("COMMON_TIMEOUT_SEC", missing=90)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "int")  [assignment]

    CACHES_ON: bool = s_attrib("CACHES_ON", missing=True)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool")  [assignment]
    CACHES_REDIS: Optional[RedisSettings] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "RedisSettings | None")  [assignment]
        # TODO: move this values to a separate key
        "CACHES_REDIS",
        fallback_factory=(
            lambda cfg: RedisSettings(  # type: ignore  # 2024-01-30 # TODO: Unexpected keyword argument "MODE" for "RedisSettings"  [call-arg]
                MODE=RedisMode.sentinel,
                CLUSTER_NAME=cfg.REDIS_CACHES_CLUSTER_NAME,
                HOSTS=_list_to_tuple(cfg.REDIS_CACHES_HOSTS),
                PORT=cfg.REDIS_CACHES_PORT,
                SSL=cfg.REDIS_CACHES_SSL,
                DB=cfg.REDIS_CACHES_DB,
                PASSWORD=required(str),
            )
            if is_setting_applicable(cfg, "REDIS_CACHES_DB")
            else None
        ),
        missing=None,
    )
    MUTATIONS_CACHES_ON: bool = s_attrib("MUTATIONS_CACHES_ON", missing=False)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool")  [assignment]
    MUTATIONS_CACHES_DEFAULT_TTL: float = s_attrib("MUTATIONS_CACHES_DEFAULT_TTL", missing=3 * 60 * 60)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "float")  [assignment]
    MUTATIONS_REDIS: Optional[RedisSettings] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "RedisSettings | None")  [assignment]
        # TODO: move this values to a separate key
        "MUTATIONS_REDIS",
        fallback_factory=(
            lambda cfg: RedisSettings(  # type: ignore  # 2024-01-30 # TODO: Unexpected keyword argument "MODE" for "RedisSettings"  [call-arg]
                MODE=RedisMode.sentinel,
                CLUSTER_NAME=cfg.REDIS_CACHES_CLUSTER_NAME,
                HOSTS=_list_to_tuple(cfg.REDIS_CACHES_HOSTS),
                PORT=cfg.REDIS_CACHES_PORT,
                SSL=cfg.REDIS_CACHES_SSL,
                DB=cfg.REDIS_MUTATIONS_CACHES_DB,
                PASSWORD=required(str),
            )
            if is_setting_applicable(cfg, "REDIS_MUTATIONS_CACHES_DB")
            else None
        ),
        missing=None,
    )
    CACHES_TTL_SETTINGS: Optional[CachesTTLSettings] = s_attrib(  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "CachesTTLSettings | None")  [assignment]
        "CACHES_TTL",
        fallback_factory=lambda: CachesTTLSettings(  # type: ignore  # 2024-01-30 # TODO: Argument "fallback_factory" to "s_attrib" has incompatible type "Callable[[], CachesTTLSettings]"; expected "Callable[[Any, Any], Any] | Callable[[Any], Any] | None"  [arg-type]
            MATERIALIZED=3600,
            OTHER=300,
        ),
        missing=None,
    )

    BI_ASYNC_APP_DISABLE_KEEPALIVE: bool = s_attrib("BI_ASYNC_APP_DISABLE_KEEPALIVE", missing=False)  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "bool")  [assignment]

    CA_FILE_PATH: str = s_attrib("CA_FILE_PATH", missing=get_root_certificates_path())  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Attribute[Any]", variable has type "str")  [assignment]

    @property
    def app_name(self) -> str:
        return "bi_data_api"

    @property
    def jaeger_service_name(self) -> str:
        return "bi-data-api"

    app_prefix: ClassVar[str] = "y"


@attr.s(frozen=True, kw_only=True)
class ControlApiAppTestingsSettings:
    us_auth_mode_override: Optional[USAuthMode] = attr.ib(default=None)
    fake_tenant: Optional[TenantDef] = attr.ib(default=None)
