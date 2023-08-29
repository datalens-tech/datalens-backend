from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, CHYTConnectorSettings
from bi_configs.connectors_data import ConnectorsDataCHYTBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


def chyt_internal_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CHYT', connector_data_class=ConnectorsDataCHYTBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_OVER_YT=CHYTConnectorSettings(  # type: ignore
            PUBLIC_CLIQUES=tuple(cfg.CONN_CHYT_PUBLIC_CLIQUES),
            FORBIDDEN_CLIQUES=tuple(cfg.CONN_CHYT_FORBIDDEN_CLIQUES),
            DEFAULT_CLIQUE=cfg.CONN_CHYT_DEFAULT_CLIQUE,
        )
    )


class CHYTInternalSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHYTConnectorSettings
    fallback = chyt_internal_settings_fallback


def chyt_internal_user_auth_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CHYT', connector_data_class=ConnectorsDataCHYTBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_OVER_YT_USER_AUTH=CHYTConnectorSettings(  # type: ignore
            PUBLIC_CLIQUES=tuple(cfg.CONN_CHYT_PUBLIC_CLIQUES),
            FORBIDDEN_CLIQUES=tuple(cfg.CONN_CHYT_FORBIDDEN_CLIQUES),
            DEFAULT_CLIQUE=cfg.CONN_CHYT_DEFAULT_CLIQUE,
        )
    )


class CHYTUserAuthSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHYTConnectorSettings
    fallback = chyt_internal_user_auth_settings_fallback
