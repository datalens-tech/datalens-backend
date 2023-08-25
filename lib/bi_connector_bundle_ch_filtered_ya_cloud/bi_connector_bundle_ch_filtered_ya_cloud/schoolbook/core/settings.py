from bi_configs.connectors_settings import ConnectorSettingsBase, SchoolbookConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def schoolbook_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        SCHOOLBOOK=SchoolbookConnectorSettings(  # type: ignore
            HOST=cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_HOST,
            PORT=cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_PORT,
            DB_NAME=cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_DB_MAME,
            USERNAME=cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_USERNAME,
            USE_MANAGED_NETWORK=cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.SCHOOLBOOK.CONN_SCHOOLBOOK_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class CHSchoolbookSettingDefinition(ConnectorSettingsDefinition):
    settings_class = SchoolbookConnectorSettings
    fallback = schoolbook_settings_fallback
