from typing import ClassVar, Optional

from bi_configs.connectors_settings import ConnectorsConfigType, ConnectorSettingsBase, CHYTConnectorSettings
from bi_configs.connectors_data import ConnectorsDataBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


class ConnectorsDataCHYTBase(ConnectorsDataBase):
    CONN_CHYT_PUBLIC_CLIQUES: ClassVar[Optional[tuple[str]]] = None
    CONN_CHYT_FORBIDDEN_CLIQUES: ClassVar[Optional[tuple[str]]] = None
    CONN_CHYT_DEFAULT_CLIQUE: ClassVar[Optional[str]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CHYT'


def chyt_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CHYT', connector_data_class=ConnectorsDataCHYTBase,
    )
    if cfg is None:
        return {}
    return dict(
        CHYT=CHYTConnectorSettings(  # type: ignore
            PUBLIC_CLIQUES=tuple(cfg.CONN_CHYT_PUBLIC_CLIQUES),
            FORBIDDEN_CLIQUES=tuple(cfg.CONN_CHYT_FORBIDDEN_CLIQUES),
            DEFAULT_CLIQUE=cfg.CONN_CHYT_DEFAULT_CLIQUE,
        )
    )


class CHYTSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHYTConnectorSettings
    fallback = chyt_settings_fallback
