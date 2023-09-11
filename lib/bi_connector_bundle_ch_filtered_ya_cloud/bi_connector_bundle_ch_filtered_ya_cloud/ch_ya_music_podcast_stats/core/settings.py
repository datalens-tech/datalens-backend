from typing import ClassVar, Optional

from bi_configs.connectors_settings import (
    ConnectorsConfigType, ConnectorSettingsBase, CHYaMusicPodcastStatsConnectorSettings,
)
from bi_configs.settings_loaders.meta_definition import required
from bi_configs.connectors_data import ConnectorsDataBase

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition, get_connectors_settings_config


class ConnectorsDataMusicBase(ConnectorsDataBase):
    CONN_MUSIC_HOST: ClassVar[Optional[str]] = None
    CONN_MUSIC_PORT: ClassVar[Optional[int]] = None
    CONN_MUSIC_DB_MAME: ClassVar[Optional[str]] = None
    CONN_MUSIC_USERNAME: ClassVar[Optional[str]] = None
    CONN_MUSIC_USE_MANAGED_NETWORK: ClassVar[Optional[bool]] = None
    CONN_MUSIC_ALLOWED_TABLES: ClassVar[Optional[list[str]]] = None
    CONN_MUSIC_SUBSELECT_TEMPLATES: ClassVar[Optional[tuple[dict[str, str], ...]]] = None

    @classmethod
    def connector_name(cls) -> str:
        return 'CH_YA_MUSIC_PODCAST_STATS'


def ch_ya_music_podcast_stats_settings_fallback(full_cfg: ConnectorsConfigType) -> dict[str, ConnectorSettingsBase]:
    cfg = get_connectors_settings_config(
        full_cfg, object_like_config_key='CH_YA_MUSIC_PODCAST_STATS', connector_data_class=ConnectorsDataMusicBase,
    )
    if cfg is None:
        return {}
    return dict(
        CH_YA_MUSIC_PODCAST_STATS=CHYaMusicPodcastStatsConnectorSettings(  # type: ignore
            HOST=cfg.CONN_MUSIC_HOST,
            PORT=cfg.CONN_MUSIC_PORT,
            DB_NAME=cfg.CONN_MUSIC_DB_MAME,
            USERNAME=cfg.CONN_MUSIC_USERNAME,
            USE_MANAGED_NETWORK=cfg.CONN_MUSIC_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CONN_MUSIC_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CONN_MUSIC_SUBSELECT_TEMPLATES),  # type: ignore
            PASSWORD=required(str),
        )
    )


class CHYaMusicPodcastStatsSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHYaMusicPodcastStatsConnectorSettings
    fallback = ch_ya_music_podcast_stats_settings_fallback
