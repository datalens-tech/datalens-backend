from __future__ import annotations

import uuid
from typing import Any, Callable, Optional

import attr
import json

from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_constants.enums import RawSQLLevel

from bi_configs.settings_loaders.meta_definition import s_attrib, required
from bi_configs.environments import (
    CommonInstallation,
)
from bi_configs.settings_loaders.settings_obj_base import SettingsBase
from bi_configs.utils import split_by_comma
from bi_configs import connectors_data as cd


def _env_loading_unimplemented(_: str) -> Any:
    raise NotImplementedError("Loading from environment is not supported.")


def _subselect_templates_env_var_converter(v: str) -> tuple[dict[str, str], ...]:
    templates: tuple[dict[str, str], ...] = tuple(json.loads(v))
    for tpl in templates:
        tpl_keys = set(tpl.keys())
        assert tpl_keys == {'sql_query', 'title'}, f'Unexpected keys for a subselect template: {tpl_keys}'
        tpl['sql_query'] = cd.normalize_sql_query(tpl['sql_query'])

    return templates


@attr.s(frozen=True)
class PartnerKeys:
    """Keys by versions"""
    dl_private: dict[str, str] = attr.ib(repr=False)
    partner_public: dict[str, str] = attr.ib(repr=False)

    @classmethod
    def from_json(cls, json_data: str) -> Optional[PartnerKeys]:
        if not json_data:
            return None
        data = json.loads(json_data)
        return PartnerKeys(
            dl_private=data['dl_private'],
            partner_public=data['partner_public'],
        )


@attr.s(frozen=True)
class ConnectorSettingsBase(SettingsBase):
    """"""


ConnectorsConfigType = ObjectLikeConfig | CommonInstallation
SettingsFallbackType = Callable[[ConnectorsConfigType], dict[str, ConnectorSettingsBase]]


@attr.s(frozen=True)
class MetricaConnectorSettings(ConnectorSettingsBase):
    COUNTER_ALLOW_MANUAL_INPUT: bool = s_attrib("COUNTER_ALLOW_MANUAL_INPUT", missing=False)  # type: ignore
    ALLOW_AUTO_DASH_CREATION: bool = s_attrib("ALLOW_AUTO_DASH_CREATION", missing=False)  # type: ignore


@attr.s(frozen=True)
class AppmetricaConnectorSettings(ConnectorSettingsBase):
    COUNTER_ALLOW_MANUAL_INPUT: bool = s_attrib("COUNTER_ALLOW_MANUAL_INPUT", missing=False)  # type: ignore
    ALLOW_AUTO_DASH_CREATION: bool = s_attrib("ALLOW_AUTO_DASH_CREATION", missing=False)  # type: ignore


@attr.s(frozen=True)
class YQConnectorSettings(ConnectorSettingsBase):
    HOST: str = s_attrib("HOST")  # type: ignore
    PORT: int = s_attrib("PORT")  # type: ignore
    DB_NAME: str = s_attrib("DB")  # type: ignore

    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore


@attr.s(frozen=True)
class YDBConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore
    DEFAULT_HOST_VALUE: Optional[str] = s_attrib("DEFAULT_HOST_VALUE", missing=None)  # type: ignore


@attr.s(frozen=True)
class ClickHouseConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore


@attr.s(frozen=True)
class CHYTConnectorSettings(ConnectorSettingsBase):
    """
    PUBLIC_CLIQUES:     cliques which usage is discouraged due to their high load by other users
    FORBIDDEN_CLIQUES:  cliques that cannot be used at all
    DEFAULT_CLIQUE:     clique that is set by default in the connection form
    """

    PUBLIC_CLIQUES: tuple[str] = s_attrib("PUBLIC_CLIQUES", missing_factory=tuple, env_var_converter=split_by_comma)  # type: ignore
    FORBIDDEN_CLIQUES: tuple[str] = s_attrib("FORBIDDEN_CLIQUES", missing_factory=tuple, env_var_converter=split_by_comma)  # type: ignore
    DEFAULT_CLIQUE: Optional[str] = s_attrib("DEFAULT_CLIQUE", missing=None)  # type: ignore


@attr.s(frozen=True)
class GreenplumConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore


@attr.s(frozen=True)
class MysqlConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore


@attr.s(frozen=True)
class PostgresConnectorSettings(ConnectorSettingsBase):
    USE_MDB_CLUSTER_PICKER: bool = s_attrib("USE_MDB_CLUSTER_PICKER", missing=False)  # type: ignore


@attr.s(frozen=True)
class MonitoringConnectorSettings(ConnectorSettingsBase):
    HOST: str = s_attrib("HOST")  # type: ignore
    URL_PATH: str = s_attrib("URL_PATH", missing="monitoring/v2")


@attr.s(frozen=True)
class FrozenConnectorSettingsBase(ConnectorSettingsBase):
    ALLOW_PUBLIC_USAGE: bool = True

    HOST: str = s_attrib("HOST")  # type: ignore
    PORT: int = s_attrib("PORT", missing=8443)  # type: ignore
    USERNAME: str = s_attrib("USERNAME")  # type: ignore
    PASSWORD: str = s_attrib("PASSWORD", sensitive=True, missing=None)  # type: ignore
    USE_MANAGED_NETWORK: bool = s_attrib("USE_MANAGED_NETWORK")  # type: ignore
    ALLOWED_TABLES: list[str] = s_attrib(  # type: ignore
        "ALLOWED_TABLES",
        env_var_converter=json.loads,
    )
    SUBSELECT_TEMPLATES: tuple[dict[str, str], ...] = s_attrib(  # type: ignore
        "SUBSELECT_TEMPLATES",
        env_var_converter=_subselect_templates_env_var_converter,
    )


@attr.s(frozen=True)
class CHFrozenConnectorSettings(FrozenConnectorSettingsBase):
    SECURE: bool = s_attrib("SECURE", missing=True)  # type: ignore
    DB_NAME: str = s_attrib("DB_NAME")  # type: ignore
    RAW_SQL_LEVEL: RawSQLLevel = s_attrib("RAW_SQL_LEVEL", missing=RawSQLLevel.off, env_var_converter=RawSQLLevel)  # type: ignore
    PASS_DB_QUERY_TO_USER: bool = s_attrib("PASS_DB_QUERY_TO_USER", missing=False)  # type: ignore


@attr.s(frozen=True)
class CHFrozenBumpyRoadsConnectorSettings(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class CHFrozenCovidConnectorSettings(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class CHFrozenDemoConnectorSettings(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class CHFrozenDTPConnectorSettings(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class CHFrozenGKHConnectorSettings(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class CHFrozenHorecaConnectorSettings(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class CHFrozenSamplesConnectorSettings(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class CHFrozenTransparencyConnectorSettings(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class CHFrozenWeatherConnectorSettings(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class ServiceConnectorSettingsBase(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class CHYaMusicPodcastStatsConnectorSettings(ServiceConnectorSettingsBase):
    """"""


@attr.s(frozen=True)
class MarketCouriersConnectorSettings(ServiceConnectorSettingsBase):
    """"""


@attr.s(frozen=True)
class SchoolbookConnectorSettings(ServiceConnectorSettingsBase):
    """"""


@attr.s(frozen=True)
class SMBHeatmapsConnectorSettings(ServiceConnectorSettingsBase):
    """"""


@attr.s(frozen=True)
class BillingConnectorSettings(ServiceConnectorSettingsBase):
    ALLOW_PUBLIC_USAGE: bool = False


@attr.s(frozen=True)
class PartnerConnectorSettingsBase(ConnectorSettingsBase):
    ALLOW_PUBLIC_USAGE: bool = True

    SECURE: bool = s_attrib("SECURE", missing=True)  # type: ignore
    HOST: str = s_attrib("HOST")  # type: ignore
    PORT: int = s_attrib("PORT", missing=8443)  # type: ignore
    USERNAME: str = s_attrib("USERNAME")  # type: ignore
    PASSWORD: str = s_attrib("PASSWORD", sensitive=True, missing=None)  # type: ignore
    USE_MANAGED_NETWORK: bool = s_attrib("USE_MANAGED_NETWORK")  # type: ignore
    PARTNER_KEYS: PartnerKeys = s_attrib(  # type: ignore
        "PARTNER_KEYS",
        env_var_converter=PartnerKeys.from_json,
        missing=None,
    )


@attr.s(frozen=True)
class EqueoConnectorSettings(PartnerConnectorSettingsBase):
    """"""


@attr.s(frozen=True)
class KonturMarketConnectorSettings(PartnerConnectorSettingsBase):
    """"""


@attr.s(frozen=True)
class MoySkladConnectorSettings(PartnerConnectorSettingsBase):
    """"""


@attr.s(frozen=True)
class FileS3ConnectorSettings(ConnectorSettingsBase):
    SECURE: bool = s_attrib("SECURE", missing=True)  # type: ignore
    HOST: str = s_attrib("HOST")  # type: ignore
    PORT: int = s_attrib("PORT", missing=8443)  # type: ignore
    USERNAME: str = s_attrib("USERNAME")  # type: ignore
    PASSWORD: str = s_attrib("PASSWORD", sensitive=True, missing=None)  # type: ignore

    ACCESS_KEY_ID: str = s_attrib("ACCESS_KEY_ID", sensitive=True, missing=None)  # type: ignore
    SECRET_ACCESS_KEY: str = s_attrib("SECRET_ACCESS_KEY", sensitive=True, missing=None)  # type: ignore
    S3_ENDPOINT: str = s_attrib("S3_ENDPOINT")  # type: ignore
    BUCKET: str = s_attrib("BUCKET")  # type: ignore

    REPLACE_SECRET_SALT: str = s_attrib(  # type: ignore
        "REPLACE_SECRET_SALT", sensitive=True, missing_factory=lambda: str(uuid.uuid4())
    )
    # ^ Note that this is used in a query, which, in turn, is used in a cache key at the moment
    #   This means that the value must be set explicitly to preserve caches between restarts and instances


@attr.s(frozen=True)
class UsageTrackingConnectionSettings(ServiceConnectorSettingsBase):
    REQUIRED_IAM_ROLE: str = s_attrib("REQUIRED_IAM_ROLE", missing=None)  # type: ignore


@attr.s(frozen=True)
class UsageTrackingYaTeamConnectionSettings(ServiceConnectorSettingsBase):
    MAX_EXECUTION_TIME: int = s_attrib("MAX_EXECUTION_TIME", missing=None)  # type: ignore
    """"""


@attr.s(frozen=True)
class ConnectorsSettingsByType(SettingsBase):
    CH_FROZEN_BUMPY_ROADS: Optional[CHFrozenBumpyRoadsConnectorSettings] = s_attrib("CH_FROZEN_BUMPY_ROADS", missing=None)  # type: ignore
    CH_FROZEN_COVID: Optional[CHFrozenCovidConnectorSettings] = s_attrib("CH_FROZEN_COVID", missing=None)  # type: ignore
    CH_FROZEN_DEMO: Optional[CHFrozenDemoConnectorSettings] = s_attrib("CH_FROZEN_DEMO", missing=None)  # type: ignore
    CH_FROZEN_DTP: Optional[CHFrozenDTPConnectorSettings] = s_attrib("CH_FROZEN_DTP", missing=None)  # type: ignore
    CH_FROZEN_GKH: Optional[CHFrozenGKHConnectorSettings] = s_attrib("CH_FROZEN_GKH", missing=None)  # type: ignore
    CH_FROZEN_SAMPLES: Optional[CHFrozenSamplesConnectorSettings] = s_attrib("CH_FROZEN_SAMPLES", missing=None)  # type: ignore
    CH_FROZEN_TRANSPARENCY: Optional[CHFrozenTransparencyConnectorSettings] = s_attrib("CH_FROZEN_TRANSPARENCY", missing=None)  # type: ignore
    CH_FROZEN_WEATHER: Optional[CHFrozenWeatherConnectorSettings] = s_attrib("CH_FROZEN_WEATHER", missing=None)  # type: ignore
    CH_FROZEN_HORECA: Optional[CHFrozenHorecaConnectorSettings] = s_attrib("CH_FROZEN_HORECA", missing=None)  # type: ignore
    FILE: Optional[FileS3ConnectorSettings] = s_attrib("FILE", missing=None)  # type: ignore
    YQ: Optional[YQConnectorSettings] = s_attrib("YQ", missing=None)  # type: ignore
    MONITORING: Optional[MonitoringConnectorSettings] = s_attrib("MONITORING", missing=None)  # type: ignore
    CH_YA_MUSIC_PODCAST_STATS: Optional[CHYaMusicPodcastStatsConnectorSettings] = s_attrib(  # type: ignore
        "CH_YA_MUSIC_PODCAST_STATS",
        missing=None,
    )
    CH_BILLING_ANALYTICS: Optional[BillingConnectorSettings] = s_attrib("CH_BILLING_ANALYTICS", missing=None)  # type: ignore
    MARKET_COURIERS: Optional[MarketCouriersConnectorSettings] = s_attrib("MARKET_COURIERS", missing=None)  # type: ignore
    SCHOOLBOOK: Optional[SchoolbookConnectorSettings] = s_attrib("SCHOOLBOOK", missing=None)  # type: ignore
    SMB_HEATMAPS: Optional[SMBHeatmapsConnectorSettings] = s_attrib("SMB_HEATMAPS", missing=None)  # type: ignore
    MOYSKLAD: Optional[MoySkladConnectorSettings] = s_attrib("MOYSKLAD", missing=None)  # type: ignore
    EQUEO: Optional[EqueoConnectorSettings] = s_attrib("EQUEO", missing=None)  # type: ignore
    KONTUR_MARKET: Optional[KonturMarketConnectorSettings] = s_attrib("KONTUR_MARKET", missing=None)  # type: ignore
    USAGE_TRACKING: Optional[UsageTrackingConnectionSettings] = s_attrib("USAGE_TRACKING", missing=None)  # type: ignore
    USAGE_TRACKING_YA_TEAM: Optional[UsageTrackingYaTeamConnectionSettings] = s_attrib(
        "USAGE_TRACKING_YA_TEAM",
        missing=None
    )  # type: ignore
    METRICA: Optional[MetricaConnectorSettings] = s_attrib("METRICA", missing=None)  # type: ignore
    APPMETRICA: Optional[AppmetricaConnectorSettings] = s_attrib("APPMETRICA", missing=None)  # type: ignore
    YDB: Optional[YDBConnectorSettings] = s_attrib("YDB", missing=None)  # type: ignore
    CLICKHOUSE: Optional[ClickHouseConnectorSettings] = s_attrib("CLICKHOUSE", missing=None)  # type: ignore
    CHYT: Optional[CHYTConnectorSettings] = s_attrib("CHYT", missing=None)  # type: ignore
    GREENPLUM: Optional[GreenplumConnectorSettings] = s_attrib("GREENPLUM", missing=None)  # type: ignore
    MYSQL: Optional[MysqlConnectorSettings] = s_attrib("MYSQL", missing=None)  # type: ignore
    POSTGRES: Optional[PostgresConnectorSettings] = s_attrib("POSTGRES", missing=None)  # type: ignore


def connectors_settings_file_only_fallback_factory(cfg: CommonInstallation) -> Optional[ConnectorsSettingsByType]:
    return ConnectorsSettingsByType(  # type: ignore
        FILE=FileS3ConnectorSettings(  # type: ignore
            HOST=cfg.CONN_FILE_CH_HOST,
            PORT=cfg.CONN_FILE_CH_PORT,
            USERNAME=cfg.CONN_FILE_CH_USERNAME,
            PASSWORD=required(str),
            ACCESS_KEY_ID=required(str),
            SECRET_ACCESS_KEY=required(str),
            S3_ENDPOINT=cfg.S3_ENDPOINT_URL,
            BUCKET=cfg.FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME,
        ) if isinstance(cfg, cd.ConnectorsDataFileBase) and isinstance(cfg, CommonInstallation) else None,
    )
