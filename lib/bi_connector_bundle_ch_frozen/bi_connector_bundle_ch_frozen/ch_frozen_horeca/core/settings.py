from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, CHFrozenHorecaConnectorSettings
from bi_configs.settings_loaders.meta_definition import required
from bi_configs.connectors_data import ConnectorsDataCHFrozenHorecaBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def ch_frozen_horeca_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_FROZEN_HORECA', connector_data_class=ConnectorsDataCHFrozenHorecaBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_FROZEN_HORECA=CHFrozenHorecaConnectorSettings(  # type: ignore
            HOST=cfg.CONN_CH_FROZEN_HORECA_HOST,
            PORT=cfg.CONN_CH_FROZEN_HORECA_PORT,
            DB_NAME=cfg.CONN_CH_FROZEN_HORECA_DB_MAME,
            USERNAME=cfg.CONN_CH_FROZEN_HORECA_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_CH_FROZEN_HORECA_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_CH_FROZEN_HORECA_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_CH_FROZEN_HORECA_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHFrozenHorecaSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHFrozenHorecaConnectorSettings
    fallback = ch_frozen_horeca_settings_fallback
