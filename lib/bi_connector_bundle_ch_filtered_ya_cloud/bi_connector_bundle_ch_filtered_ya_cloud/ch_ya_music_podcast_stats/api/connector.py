from __future__ import annotations

from dl_api_connector.api_schema.source_base import (
    SQLDataSourceSchema,
    SQLDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)
from dl_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_bundle_ch_filtered.base.api.i18n.localizer import CONFIGS as BASE_CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.base.api.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.api.api_schema.connection import (
    CHYaMusicPodcastStatsConnectionSchema,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.api.connection_form.form_config import (
    CHYaMusicPodcastStatsConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.api.connection_info import (
    CHYaMusicPodcastStatsConnectionInfoProvider,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.core.connector import (
    CHYaMusicPodcastStatsCoreConnectionDefinition,
    CHYaMusicPodcastStatsCoreConnector,
    CHYaMusicPodcastStatsTableCoreSourceDefinition,
)


class CHYaMusicPodcastStatsApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = CHYaMusicPodcastStatsCoreConnectionDefinition
    api_generic_schema_cls = CHYaMusicPodcastStatsConnectionSchema
    info_provider_cls = CHYaMusicPodcastStatsConnectionInfoProvider
    form_factory_cls = CHYaMusicPodcastStatsConnectionFormFactory


class CHYaMusicPodcastStatsApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHYaMusicPodcastStatsTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHYaMusicPodcastStatsApiConnector(ApiConnector):
    core_connector_cls = CHYaMusicPodcastStatsCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (CHYaMusicPodcastStatsApiConnectionDefinition,)
    source_definitions = (CHYaMusicPodcastStatsApiSourceDefinition,)
    translation_configs = frozenset(BASE_CONFIGS) | frozenset(CONFIGS)
