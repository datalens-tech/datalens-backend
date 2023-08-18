from __future__ import annotations

import uuid
from typing import Any, Optional, Union

import attr
import json

from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_constants.enums import RawSQLLevel

from bi_configs.settings_loaders.meta_definition import s_attrib, required
from bi_configs.environments import (
    CommonInstallation,
    DataCloudInstallation,
    NebiusInstallation,
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
class CHFrozenSamplesConnectorSettings(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class CHFrozenTransparencyConnectorSettings(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class CHFrozenWeatherConnectorSettings(CHFrozenConnectorSettings):
    """"""


@attr.s(frozen=True)
class CHFrozenHorecaConnectorSettings(CHFrozenConnectorSettings):
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
class MoySkladConnectorSettings(PartnerConnectorSettingsBase):
    """"""


@attr.s(frozen=True)
class EqueoConnectorSettings(PartnerConnectorSettingsBase):
    """"""


@attr.s(frozen=True)
class KonturMarketConnectorSettings(PartnerConnectorSettingsBase):
    """"""


@attr.s(frozen=True)
class BitrixConnectorSettings(PartnerConnectorSettingsBase):
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
    BITRIX: Optional[BitrixConnectorSettings] = s_attrib("BITRIX", missing=None)  # type: ignore
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


def connectors_settings_fallback_factory_config(full_cfg: ObjectLikeConfig) -> Optional[ConnectorsSettingsByType]:
    cfg = full_cfg.CONNECTORS_DATA
    return ConnectorsSettingsByType(  # type: ignore
        CH_FROZEN_BUMPY_ROADS=CHFrozenBumpyRoadsConnectorSettings(
            HOST=cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_HOST,
            PORT=cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_PORT,
            DB_NAME=cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_DB_MAME,
            USERNAME=cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_BUMPY_ROADS.CONN_CH_FROZEN_BUMPY_ROADS_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'CH_FROZEN_BUMPY_ROADS') else None,
        CH_FROZEN_COVID=CHFrozenCovidConnectorSettings(
            HOST=cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_HOST,
            PORT=cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_PORT,
            DB_NAME=cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_DB_MAME,
            USERNAME=cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_COVID.CONN_CH_FROZEN_COVID_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'CH_FROZEN_COVID') else None,
        CH_FROZEN_DEMO=CHFrozenDemoConnectorSettings(
            HOST=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_HOST,
            PORT=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_PORT,
            DB_NAME=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_DB_MAME,
            USERNAME=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_SUBSELECT_TEMPLATES),
            RAW_SQL_LEVEL=RawSQLLevel(cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_RAW_SQL_LEVEL),
            PASS_DB_QUERY_TO_USER=cfg.CH_FROZEN_DEMO.CONN_CH_FROZEN_DEMO_PASS_DB_QUERY_TO_USER,
            PASSWORD=required(str),
        ) if hasattr(cfg, 'CH_FROZEN_DEMO') else None,
        CH_FROZEN_DTP=CHFrozenDTPConnectorSettings(
            HOST=cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_HOST,
            PORT=cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_PORT,
            DB_NAME=cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_DB_MAME,
            USERNAME=cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_DTP.CONN_CH_FROZEN_DTP_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'CH_FROZEN_DTP') else None,
        CH_FROZEN_GKH=CHFrozenGKHConnectorSettings(
            HOST=cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_HOST,
            PORT=cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_PORT,
            DB_NAME=cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_DB_MAME,
            USERNAME=cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_GKH.CONN_CH_FROZEN_GKH_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'CH_FROZEN_GKH') else None,
        CH_FROZEN_SAMPLES=CHFrozenSamplesConnectorSettings(
            HOST=cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_HOST,
            PORT=cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_PORT,
            DB_NAME=cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_DB_MAME,
            USERNAME=cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_SAMPLES.CONN_CH_FROZEN_SAMPLES_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'CH_FROZEN_SAMPLES') else None,
        CH_FROZEN_TRANSPARENCY=CHFrozenTransparencyConnectorSettings(
            HOST=cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_HOST,
            PORT=cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_PORT,
            DB_NAME=cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_DB_MAME,
            USERNAME=cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_TRANSPARENCY.CONN_CH_FROZEN_TRANSPARENCY_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'CH_FROZEN_TRANSPARENCY') else None,
        CH_FROZEN_WEATHER=CHFrozenWeatherConnectorSettings(
            HOST=cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_HOST,
            PORT=cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_PORT,
            DB_NAME=cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_DB_MAME,
            USERNAME=cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_WEATHER.CONN_CH_FROZEN_WEATHER_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'CH_FROZEN_WEATHER') else None,
        CH_FROZEN_HORECA=CHFrozenHorecaConnectorSettings(
            HOST=cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_HOST,
            PORT=cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_PORT,
            DB_NAME=cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_DB_MAME,
            USERNAME=cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_FROZEN_HORECA.CONN_CH_FROZEN_HORECA_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'CH_FROZEN_HORECA') else None,
        METRICA=MetricaConnectorSettings(),
        APPMETRICA=AppmetricaConnectorSettings(),
        YDB=YDBConnectorSettings(),
        CHYT=CHYTConnectorSettings(
            PUBLIC_CLIQUES=tuple(cfg.CHYT.CONN_CHYT_PUBLIC_CLIQUES),
            FORBIDDEN_CLIQUES=tuple(cfg.CHYT.CONN_CHYT_FORBIDDEN_CLIQUES),
            DEFAULT_CLIQUE=cfg.CHYT.CONN_CHYT_DEFAULT_CLIQUE,
        ) if hasattr(cfg, 'CHYT') else None,
        CLICKHOUSE=ClickHouseConnectorSettings(),
        GREENPLUM=GreenplumConnectorSettings(),
        MYSQL=MysqlConnectorSettings(),
        POSTGRES=PostgresConnectorSettings(),
        YQ=YQConnectorSettings(  # type: ignore
            HOST=cfg.YQ.CONN_YQ_HOST,
            PORT=cfg.YQ.CONN_YQ_PORT,
            DB_NAME=cfg.YQ.CONN_YQ_DB_NAME,
        ) if hasattr(cfg, 'YQ') else None,
        MONITORING=MonitoringConnectorSettings(  # type: ignore
            HOST=cfg.MONITORING.CONN_MONITORING_HOST,
        ) if hasattr(cfg, 'MONITORING') else None,
        CH_YA_MUSIC_PODCAST_STATS=CHYaMusicPodcastStatsConnectorSettings(  # type: ignore
            HOST=cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_HOST,
            PORT=cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_PORT,
            DB_NAME=cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_DB_MAME,
            USERNAME=cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'CH_YA_MUSIC_PODCAST_STATS') else None,
        FILE=FileS3ConnectorSettings(  # type: ignore
            HOST=cfg.FILE.CONN_FILE_CH_HOST,
            PORT=cfg.FILE.CONN_FILE_CH_PORT,
            USERNAME=cfg.FILE.CONN_FILE_CH_USERNAME,
            PASSWORD=required(str),
            ACCESS_KEY_ID=required(str),
            SECRET_ACCESS_KEY=required(str),
            S3_ENDPOINT=full_cfg.S3_ENDPOINT_URL,
            BUCKET=full_cfg.FILE_UPLOADER_S3_PERSISTENT_BUCKET_NAME,
        ) if hasattr(cfg, 'FILE') else None,
        CH_BILLING_ANALYTICS=BillingConnectorSettings(  # type: ignore
            HOST=cfg.CH_BILLING_ANALYTICS.CONN_BILLING_HOST,
            PORT=cfg.CH_BILLING_ANALYTICS.CONN_BILLING_PORT,
            DB_NAME=cfg.CH_BILLING_ANALYTICS.CONN_BILLING_DB_MAME,
            USERNAME=cfg.CH_BILLING_ANALYTICS.CONN_BILLING_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_BILLING_ANALYTICS.CONN_BILLING_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_BILLING_ANALYTICS.CONN_BILLING_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_BILLING_ANALYTICS.CONN_BILLING_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'CH_BILLING_ANALYTICS') else None,
        MARKET_COURIERS=MarketCouriersConnectorSettings(  # type: ignore
            HOST=cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_HOST,
            PORT=cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_PORT,
            DB_NAME=cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_DB_MAME,
            USERNAME=cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_USERNAME,
            USE_MANAGED_NETWORK=cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.MARKET_COURIERS.CONN_MARKET_COURIERS_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'MARKET_COURIERS') else None,
        SCHOOLBOOK=SchoolbookConnectorSettings(  # type: ignore
            HOST=cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_HOST,
            PORT=cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_PORT,
            DB_NAME=cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_DB_MAME,
            USERNAME=cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_USERNAME,
            USE_MANAGED_NETWORK=cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'SCHOOLBOOK') else None,
        SMB_HEATMAPS=SMBHeatmapsConnectorSettings(  # type: ignore
            HOST=cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_HOST,
            PORT=cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_PORT,
            DB_NAME=cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_DB_MAME,
            USERNAME=cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_USERNAME,
            USE_MANAGED_NETWORK=cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.SMB_HEATMAPS.CONN_SMB_HEATMAPS_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        ) if hasattr(cfg, 'SMB_HEATMAPS') else None,
        MOYSKLAD=MoySkladConnectorSettings(  # type: ignore
            HOST=cfg.MOYSKLAD.CONN_MOYSKLAD_HOST,
            PORT=cfg.MOYSKLAD.CONN_MOYSKLAD_PORT,
            USERNAME=cfg.MOYSKLAD.CONN_MOYSKLAD_USERNAME,
            USE_MANAGED_NETWORK=cfg.MOYSKLAD.CONN_MOYSKLAD_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        ) if hasattr(cfg, 'MOYSKLAD') else None,
        EQUEO=EqueoConnectorSettings(  # type: ignore
            HOST=cfg.EQUEO.CONN_EQUEO_HOST,
            PORT=cfg.EQUEO.CONN_EQUEO_PORT,
            USERNAME=cfg.EQUEO.CONN_EQUEO_USERNAME,
            USE_MANAGED_NETWORK=cfg.EQUEO.CONN_EQUEO_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        ) if hasattr(cfg, 'EQUEO') else None,
        KONTUR_MARKET=KonturMarketConnectorSettings(  # type: ignore
            HOST=cfg.KONTUR_MARKET.CONN_KONTUR_MARKET_HOST,
            PORT=cfg.KONTUR_MARKET.CONN_KONTUR_MARKET_PORT,
            USERNAME=cfg.KONTUR_MARKET.CONN_KONTUR_MARKET_USERNAME,
            USE_MANAGED_NETWORK=cfg.KONTUR_MARKET.CONN_KONTUR_MARKET_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        ) if hasattr(cfg, 'KONTUR_MARKET') else None,
        BITRIX=BitrixConnectorSettings(  # type: ignore
            HOST=cfg.BITRIX.CONN_BITRIX_HOST,
            PORT=cfg.BITRIX.CONN_BITRIX_PORT,
            USERNAME=cfg.BITRIX.CONN_BITRIX_USERNAME,
            USE_MANAGED_NETWORK=cfg.BITRIX.CONN_BITRIX_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        ) if hasattr(cfg, 'BITRIX') else None,
        USAGE_TRACKING=UsageTrackingConnectionSettings(  # type: ignore
            HOST=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_HOST,
            PORT=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_PORT,
            DB_NAME=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_DB_NAME,
            USERNAME=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_USERNAME,
            USE_MANAGED_NETWORK=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_SUBSELECT_TEMPLATES),
            REQUIRED_IAM_ROLE=cfg.USAGE_TRACKING.CONN_USAGE_TRACKING_REQUIRED_IAM_ROLE,
            PASSWORD=required(str),
        ) if hasattr(cfg, 'USAGE_TRACKING') else None,
        USAGE_TRACKING_YA_TEAM=UsageTrackingYaTeamConnectionSettings(  # type: ignore
            HOST=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_HOST,
            PORT=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_PORT,
            DB_NAME=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_DB_NAME,
            USERNAME=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_USERNAME,
            USE_MANAGED_NETWORK=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_SUBSELECT_TEMPLATES),
            MAX_EXECUTION_TIME=cfg.USAGE_TRACKING_YA_TEAM.CONN_USAGE_TRACKING_YA_TEAM_MAX_EXECUTION_TIME,
            PASSWORD=required(str),
        ) if hasattr(cfg, 'USAGE_TRACKING_YA_TEAM') else None,
    )


def connectors_settings_fallback_factory_defaults(cfg: CommonInstallation) -> Optional[ConnectorsSettingsByType]:
    return ConnectorsSettingsByType(  # type: ignore
        CH_FROZEN_BUMPY_ROADS=CHFrozenBumpyRoadsConnectorSettings(
            HOST=cfg.CONN_CH_FROZEN_BUMPY_ROADS_HOST,
            PORT=cfg.CONN_CH_FROZEN_BUMPY_ROADS_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_BUMPY_ROADS_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_BUMPY_ROADS_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_BUMPY_ROADS_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_BUMPY_ROADS_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_CH_FROZEN_BUMPY_ROADS_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataCHFrozenBumpyRoadsBase) else None,
        CH_FROZEN_COVID=CHFrozenCovidConnectorSettings(
            HOST=cfg.CONN_CH_FROZEN_COVID_HOST,
            PORT=cfg.CONN_CH_FROZEN_COVID_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_COVID_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_COVID_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_COVID_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_COVID_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_CH_FROZEN_COVID_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataCHFrozenCovidBase) else None,
        CH_FROZEN_DEMO=CHFrozenDemoConnectorSettings(
            HOST=cfg.CONN_CH_FROZEN_DEMO_HOST,
            PORT=cfg.CONN_CH_FROZEN_DEMO_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_DEMO_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_DEMO_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_DEMO_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_DEMO_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_CH_FROZEN_DEMO_SUBSELECT_TEMPLATES,
            RAW_SQL_LEVEL=cfg.CONN_CH_FROZEN_DEMO_RAW_SQL_LEVEL,
            PASS_DB_QUERY_TO_USER=cfg.CONN_CH_FROZEN_DEMO_PASS_DB_QUERY_TO_USER,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataCHFrozenDemoBase) else None,
        CH_FROZEN_DTP=CHFrozenDTPConnectorSettings(
            HOST=cfg.CONN_CH_FROZEN_DTP_HOST,
            PORT=cfg.CONN_CH_FROZEN_DTP_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_DTP_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_DTP_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_DTP_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_DTP_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_CH_FROZEN_DTP_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataCHFrozenDTPBase) else None,
        CH_FROZEN_GKH=CHFrozenGKHConnectorSettings(
            HOST=cfg.CONN_CH_FROZEN_GKH_HOST,
            PORT=cfg.CONN_CH_FROZEN_GKH_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_GKH_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_GKH_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_GKH_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_GKH_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_CH_FROZEN_GKH_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataCHFrozenGKHBase) else None,
        CH_FROZEN_SAMPLES=CHFrozenSamplesConnectorSettings(
            HOST=cfg.CONN_CH_FROZEN_SAMPLES_HOST,
            PORT=cfg.CONN_CH_FROZEN_SAMPLES_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_SAMPLES_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_SAMPLES_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_SAMPLES_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_SAMPLES_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_CH_FROZEN_SAMPLES_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataCHFrozenSamplesBase) else None,
        CH_FROZEN_TRANSPARENCY=CHFrozenTransparencyConnectorSettings(
            HOST=cfg.CONN_CH_FROZEN_TRANSPARENCY_HOST,
            PORT=cfg.CONN_CH_FROZEN_TRANSPARENCY_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_TRANSPARENCY_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_TRANSPARENCY_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_TRANSPARENCY_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_TRANSPARENCY_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_CH_FROZEN_TRANSPARENCY_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataCHFrozenTransparencyBase) else None,
        CH_FROZEN_WEATHER=CHFrozenWeatherConnectorSettings(
            HOST=cfg.CONN_CH_FROZEN_WEATHER_HOST,
            PORT=cfg.CONN_CH_FROZEN_WEATHER_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_WEATHER_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_WEATHER_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_WEATHER_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_WEATHER_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_CH_FROZEN_WEATHER_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataCHFrozenWeatherBase) else None,
        CH_FROZEN_HORECA=CHFrozenHorecaConnectorSettings(
            HOST=cfg.CONN_CH_FROZEN_HORECA_HOST,
            PORT=cfg.CONN_CH_FROZEN_HORECA_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_HORECA_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_HORECA_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_HORECA_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_HORECA_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_CH_FROZEN_HORECA_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataCHFrozenHorecaBase) else None,
        METRICA=MetricaConnectorSettings(),
        APPMETRICA=AppmetricaConnectorSettings(),
        YDB=YDBConnectorSettings(),
        CHYT=CHYTConnectorSettings(
            PUBLIC_CLIQUES=cfg.CONN_CHYT_PUBLIC_CLIQUES,
            FORBIDDEN_CLIQUES=cfg.CONN_CHYT_FORBIDDEN_CLIQUES,
            DEFAULT_CLIQUE=cfg.CONN_CHYT_DEFAULT_CLIQUE,
        ) if isinstance(cfg, cd.ConnectorsDataCHYTBase) else None,
        CLICKHOUSE=ClickHouseConnectorSettings(),
        GREENPLUM=GreenplumConnectorSettings(),
        MYSQL=MysqlConnectorSettings(),
        POSTGRES=PostgresConnectorSettings(),
        YQ=YQConnectorSettings(  # type: ignore
            HOST=cfg.CONN_YQ_HOST,
            PORT=cfg.CONN_YQ_PORT,
            DB_NAME=cfg.CONN_YQ_DB_NAME,
        ) if isinstance(cfg, cd.ConnectorsDataYQBase) else None,
        MONITORING=MonitoringConnectorSettings(  # type: ignore
            HOST=cfg.CONN_MONITORING_HOST,
        ) if isinstance(cfg, cd.ConnectorsDataMonitoringBase) else None,
        CH_YA_MUSIC_PODCAST_STATS=CHYaMusicPodcastStatsConnectorSettings(  # type: ignore
            HOST=cfg.CONN_MUSIC_HOST,
            PORT=cfg.CONN_MUSIC_PORT,
            DB_NAME=cfg.CONN_MUSIC_DB_MAME,
            USERNAME=cfg.CONN_MUSIC_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_MUSIC_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_MUSIC_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_MUSIC_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataMusicBase) else None,
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
        CH_BILLING_ANALYTICS=BillingConnectorSettings(  # type: ignore
            HOST=cfg.CONN_BILLING_HOST,
            PORT=cfg.CONN_BILLING_PORT,
            DB_NAME=cfg.CONN_BILLING_DB_MAME,
            USERNAME=cfg.CONN_BILLING_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_BILLING_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_BILLING_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_BILLING_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataBillingBase) else None,
        MARKET_COURIERS=MarketCouriersConnectorSettings(  # type: ignore
            HOST=cfg.CONN_MARKET_COURIERS_HOST,
            PORT=cfg.CONN_MARKET_COURIERS_PORT,
            DB_NAME=cfg.CONN_MARKET_COURIERS_DB_MAME,
            USERNAME=cfg.CONN_MARKET_COURIERS_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_MARKET_COURIERS_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_MARKET_COURIERS_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_MARKET_COURIERS_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataMarketCouriersBase) else None,
        SCHOOLBOOK=SchoolbookConnectorSettings(  # type: ignore
            HOST=cfg.CONN_SCHOOLBOOK_HOST,
            PORT=cfg.CONN_SCHOOLBOOK_PORT,
            DB_NAME=cfg.CONN_SCHOOLBOOK_DB_MAME,
            USERNAME=cfg.CONN_SCHOOLBOOK_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_SCHOOLBOOK_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_SCHOOLBOOK_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_SCHOOLBOOK_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataSchoolbookBase) else None,
        SMB_HEATMAPS=SMBHeatmapsConnectorSettings(  # type: ignore
            HOST=cfg.CONN_SMB_HEATMAPS_HOST,
            PORT=cfg.CONN_SMB_HEATMAPS_PORT,
            DB_NAME=cfg.CONN_SMB_HEATMAPS_DB_MAME,
            USERNAME=cfg.CONN_SMB_HEATMAPS_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_SMB_HEATMAPS_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_SMB_HEATMAPS_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_SMB_HEATMAPS_SUBSELECT_TEMPLATES,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataSMBHeatmapsBase) else None,
        MOYSKLAD=MoySkladConnectorSettings(  # type: ignore
            HOST=cfg.CONN_MOYSKLAD_HOST,
            PORT=cfg.CONN_MOYSKLAD_PORT,
            USERNAME=cfg.CONN_MOYSKLAD_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_MOYSKLAD_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        ) if isinstance(cfg, cd.ConnectorsDataMoyskladBase) else None,
        EQUEO=EqueoConnectorSettings(  # type: ignore
            HOST=cfg.CONN_EQUEO_HOST,
            PORT=cfg.CONN_EQUEO_PORT,
            USERNAME=cfg.CONN_EQUEO_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_EQUEO_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        ) if isinstance(cfg, cd.ConnectorsDataEqueoBase) else None,
        KONTUR_MARKET=KonturMarketConnectorSettings(  # type: ignore
            HOST=cfg.CONN_KONTUR_MARKET_HOST,
            PORT=cfg.CONN_KONTUR_MARKET_PORT,
            USERNAME=cfg.CONN_KONTUR_MARKET_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_KONTUR_MARKET_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        ) if isinstance(cfg, cd.ConnectorsDataKonturMarketBase) else None,
        BITRIX=BitrixConnectorSettings(  # type: ignore
            HOST=cfg.CONN_BITRIX_HOST,
            PORT=cfg.CONN_BITRIX_PORT,
            USERNAME=cfg.CONN_BITRIX_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_BITRIX_USE_MANAGED_NETWORK,
            PASSWORD=required(str),
            PARTNER_KEYS=required(PartnerKeys),
        ) if isinstance(cfg, cd.ConnectorsDataBitrixBase) else None,
        USAGE_TRACKING=UsageTrackingConnectionSettings(  # type: ignore
            HOST=cfg.CONN_USAGE_TRACKING_HOST,
            PORT=cfg.CONN_USAGE_TRACKING_PORT,
            DB_NAME=cfg.CONN_USAGE_TRACKING_DB_NAME,
            USERNAME=cfg.CONN_USAGE_TRACKING_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_USAGE_TRACKING_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_USAGE_TRACKING_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_USAGE_TRACKING_SUBSELECT_TEMPLATES,
            REQUIRED_IAM_ROLE=cfg.CONN_USAGE_TRACKING_REQUIRED_IAM_ROLE,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataUsageTrackingBase) else None,
        USAGE_TRACKING_YA_TEAM=UsageTrackingYaTeamConnectionSettings(  # type: ignore
            HOST=cfg.CONN_USAGE_TRACKING_YA_TEAM_HOST,
            PORT=cfg.CONN_USAGE_TRACKING_YA_TEAM_PORT,
            DB_NAME=cfg.CONN_USAGE_TRACKING_YA_TEAM_DB_NAME,
            USERNAME=cfg.CONN_USAGE_TRACKING_YA_TEAM_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_USAGE_TRACKING_YA_TEAM_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_USAGE_TRACKING_YA_TEAM_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=cfg.CONN_USAGE_TRACKING_YA_TEAM_SUBSELECT_TEMPLATES,
            MAX_EXECUTION_TIME=cfg.CONN_USAGE_TRACKING_YA_TEAM_MAX_EXECUTION_TIME,
            PASSWORD=required(str),
        ) if isinstance(cfg, cd.ConnectorsDataUsageTrackingYaTeamBase) else None,
    )


def connectors_settings_fallback_factory(cfg: Union[CommonInstallation, ObjectLikeConfig]) -> Optional[ConnectorSettingsBase]:
    if isinstance(cfg, (CommonInstallation, DataCloudInstallation, NebiusInstallation)):
        return connectors_settings_fallback_factory_defaults(cfg)
    if isinstance(cfg, ObjectLikeConfig):
        return connectors_settings_fallback_factory_config(cfg)
    raise ValueError(f'Unknown cfg type {type(cfg)}')


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
