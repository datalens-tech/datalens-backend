from typing import Optional, ClassVar

import attr

from dl_configs.connectors_data import ConnectorsDataBase
from dl_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase
from dl_configs.settings_loaders.meta_definition import required

from dl_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config

from bi_connector_bundle_ch_filtered.base.core.settings import CHFrozenConnectorSettings


@attr.s(frozen=True)
class CHFrozenCovidConnectorSettings(CHFrozenConnectorSettings):
    """"""


class ConnectorsDataCHFrozenCovidBase(ConnectorsDataBase):
    CONN_CH_FROZEN_COVID_HOST: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_COVID_PORT: ClassVar[Optional[int]] = None
    CONN_CH_FROZEN_COVID_DB_MAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_COVID_USERNAME: ClassVar[Optional[str]] = None
    CONN_CH_FROZEN_COVID_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_CH_FROZEN_COVID_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_CH_FROZEN_COVID_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_FROZEN_COVID'


def ch_frozen_covid_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_FROZEN_COVID', connector_data_class=ConnectorsDataCHFrozenCovidBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_FROZEN_COVID=CHFrozenCovidConnectorSettings(  # type: ignore
            HOST=cfg.CONN_CH_FROZEN_COVID_HOST,
            PORT=cfg.CONN_CH_FROZEN_COVID_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_COVID_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_COVID_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_COVID_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_COVID_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_CH_FROZEN_COVID_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHFrozenCovidSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenCovidConnectorSettings
    fallback = ch_frozen_covid_settings_fallback
