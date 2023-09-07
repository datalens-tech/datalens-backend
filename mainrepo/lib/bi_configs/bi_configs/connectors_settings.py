from __future__ import annotations

import uuid
from typing import Any, Callable, Optional

import attr
import json

from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_constants.enums import RawSQLLevel

from bi_configs.settings_loaders.meta_definition import s_attrib
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
