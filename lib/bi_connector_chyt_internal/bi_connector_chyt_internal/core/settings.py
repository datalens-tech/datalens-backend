from bi_configs.connectors_settings import ConnectorSettingsBase, CHYTConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def chyt_internal_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_OVER_YT=CHYTConnectorSettings(
            PUBLIC_CLIQUES=tuple(cfg.CHYT.CONN_CHYT_PUBLIC_CLIQUES),
            FORBIDDEN_CLIQUES=tuple(cfg.CHYT.CONN_CHYT_FORBIDDEN_CLIQUES),
            DEFAULT_CLIQUE=cfg.CHYT.CONN_CHYT_DEFAULT_CLIQUE,
        )
    )


class CHYTInternalSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHYTConnectorSettings
    fallback = chyt_internal_settings_fallback


def chyt_internal_user_auth_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_OVER_YT_USER_AUTH=CHYTConnectorSettings(
            PUBLIC_CLIQUES=tuple(cfg.CHYT.CONN_CHYT_PUBLIC_CLIQUES),
            FORBIDDEN_CLIQUES=tuple(cfg.CHYT.CONN_CHYT_FORBIDDEN_CLIQUES),
            DEFAULT_CLIQUE=cfg.CHYT.CONN_CHYT_DEFAULT_CLIQUE,
        )
    )


class CHYTUserAuthSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHYTConnectorSettings
    fallback = chyt_internal_user_auth_settings_fallback
