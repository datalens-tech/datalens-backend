from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, SchoolbookConnectorSettings
from bi_configs.settings_loaders.meta_definition import required
from bi_configs.connectors_data import ConnectorsDataSchoolbookBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def schoolbook_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='SCHOOLBOOK', connector_data_class=ConnectorsDataSchoolbookBase,
    )
    if cfg is None:
        return {}
    return dict(
        SCHOOLBOOK=SchoolbookConnectorSettings(  # type: ignore
            HOST=cfg.CONN_SCHOOLBOOK_HOST,
            PORT=cfg.CONN_SCHOOLBOOK_PORT,
            DB_NAME=cfg.CONN_SCHOOLBOOK_DB_MAME,
            USERNAME=cfg.CONN_SCHOOLBOOK_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_SCHOOLBOOK_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_SCHOOLBOOK_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_SCHOOLBOOK_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHSchoolbookSettingDefinition(ConnectorSettingsDefinition):
    settings_class = SchoolbookConnectorSettings
    fallback = schoolbook_settings_fallback
