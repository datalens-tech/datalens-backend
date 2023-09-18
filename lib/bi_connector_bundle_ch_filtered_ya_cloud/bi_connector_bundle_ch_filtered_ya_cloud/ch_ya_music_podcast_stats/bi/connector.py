from __future__ import annotations

from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.connector import (
    CHYaMusicPodcastStatsCoreConnectionDefinition,
    CHYaMusicPodcastStatsTableCoreSourceDefinition,
    CHYaMusicPodcastStatsCoreConnector,
)

from dl_api_connector.api_schema.source_base import SQLDataSourceSchema, SQLDataSourceTemplateSchema
from dl_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)

from dl_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_bundle_ch_filtered.base.bi.i18n.localizer import CONFIGS as BASE_CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.bi.api_schema.connection import (
    CHYaMusicPodcastStatsConnectionSchema,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.bi.connection_form.form_config import (
    CHYaMusicPodcastStatsConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.bi.connection_info import (
    CHYaMusicPodcastStatsConnectionInfoProvider,
)


class CHYaMusicPodcastStatsBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = CHYaMusicPodcastStatsCoreConnectionDefinition
    api_generic_schema_cls = CHYaMusicPodcastStatsConnectionSchema
    info_provider_cls = CHYaMusicPodcastStatsConnectionInfoProvider
    form_factory_cls = CHYaMusicPodcastStatsConnectionFormFactory


class CHYaMusicPodcastStatsBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHYaMusicPodcastStatsTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHYaMusicPodcastStatsBiApiConnector(BiApiConnector):
    core_connector_cls = CHYaMusicPodcastStatsCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (
        CHYaMusicPodcastStatsBiApiConnectionDefinition,
    )
    source_definitions = (
        CHYaMusicPodcastStatsBiApiSourceDefinition,
    )
    translation_configs = frozenset(BASE_CONFIGS) | frozenset(CONFIGS)
