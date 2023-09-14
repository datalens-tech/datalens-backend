from typing import Optional, ClassVar

import attr

from bi_configs.connectors_data import ConnectorsDataBase
from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase
from bi_configs.settings_loaders.meta_definition import required

from bi_constants.enums import RawSQLLevel

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config

from bi_connector_bundle_ch_filtered.base.core.settings import CHFrozenConnectorSettings


@attr.s(frozen=True)
class CHFrozenDemoConnectorSettings(CHFrozenConnectorSettings):
    """"""


class ConnectorsDataCHFrozenDemoBase(ConnectorsDataBase):
    CONN_CH_FROZEN_DEMO_HOST: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_DEMO_PORT: ClassVar[Optional[int]] = None
    CONN_CH_FROZEN_DEMO_DB_MAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_DEMO_USERNAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_DEMO_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_CH_FROZEN_DEMO_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_CH_FROZEN_DEMO_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None
    CONN_CH_FROZEN_DEMO_RAW_SQL_LEVEL: ClassVar[Optional[tuple[dict[str, str], ...]]] = None
    CONN_CH_FROZEN_DEMO_PASS_DB_QUERY_TO_USER: ClassVar[Optional[bool]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_FROZEN_DEMO'


def ch_frozen_demo_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_FROZEN_DEMO', connector_data_class=ConnectorsDataCHFrozenDemoBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_FROZEN_DEMO=CHFrozenDemoConnectorSettings(  # type: ignore
            HOST=cfg.CONN_CH_FROZEN_DEMO_HOST,
            PORT=cfg.CONN_CH_FROZEN_DEMO_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_DEMO_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_DEMO_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_DEMO_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_DEMO_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_CH_FROZEN_DEMO_SUBSELECT_TEMPLATES),  # type: ignore
            RAW_SQL_LEVEL=RawSQLLevel(cfg.CONN_CH_FROZEN_DEMO_RAW_SQL_LEVEL),
            PASS_DB_QUERY_TO_USER=cfg.CONN_CH_FROZEN_DEMO_PASS_DB_QUERY_TO_USER,
            PASSWORD=required(str),
        )
    )


class CHFrozenDemoSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenDemoConnectorSettings
    fallback = ch_frozen_demo_settings_fallback
