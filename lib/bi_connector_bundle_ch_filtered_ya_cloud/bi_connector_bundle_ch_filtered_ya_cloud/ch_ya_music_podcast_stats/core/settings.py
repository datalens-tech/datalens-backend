from bi_configs.connectors_settings import ConnectorSettingsBase, CHYaMusicPodcastStatsConnectorSettings
from bi_configs.settings_loaders.fallback_cfg_resolver import ObjectLikeConfig
from bi_configs.settings_loaders.meta_definition import required

from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition


def ch_ya_music_podcast_stats_settings_fallback(full_cfg: ObjectLikeConfig) -> dict[str, ConnectorSettingsBase]:
    cfg = full_cfg.CONNECTORS_DATA
    return dict(
        CH_YA_MUSIC_PODCAST_STATS=CHYaMusicPodcastStatsConnectorSettings(  # type: ignore
            HOST=cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_HOST,
            PORT=cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_PORT,
            DB_NAME=cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_DB_MAME,
            USERNAME=cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_USERNAME,
            USE_MANAGED_NETWORK=cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_USE_MANAGED_NETWORK,
            ALLOWED_TABLES=cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_ALLOWED_TABLES,
            SUBSELECT_TEMPLATES=tuple(cfg.CH_YA_MUSIC_PODCAST_STATS.CONN_MUSIC_SUBSELECT_TEMPLATES),
            PASSWORD=required(str),
        )
    )


class CHYaMusicPodcastStatsSettingDefinition(ConnectorSettingsDefinition):
    settings_class = CHYaMusicPodcastStatsConnectorSettings
    fallback = ch_ya_music_podcast_stats_settings_fallback
