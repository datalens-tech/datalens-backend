from __future__ import annotations

from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.connector import (
    CHSchoolbookCoreConnectionDefinition,
    CHSchoolbookCoreSourceDefinition,
    CHSchoolbookCoreConnector,
)

from bi_api_connector.api_schema.source_base import SQLDataSourceSchema, SQLDataSourceTemplateSchema
from bi_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)

from bi_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_bundle_ch_filtered.base.bi.i18n.localizer import CONFIGS as BASE_CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.bi.api_schema.connection import CHSchoolbookConnectionSchema
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.bi.connection_form.form_config import (
    CHSchoolbookConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.bi.connection_info import CHSchoolbookConnectionInfoProvider


class CHSchoolbookBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = CHSchoolbookCoreConnectionDefinition
    api_generic_schema_cls = CHSchoolbookConnectionSchema
    info_provider_cls = CHSchoolbookConnectionInfoProvider
    form_factory_cls = CHSchoolbookConnectionFormFactory


class CHSchoolbookBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHSchoolbookCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHSchoolbookBiApiConnector(BiApiConnector):
    core_connector_cls = CHSchoolbookCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (
        CHSchoolbookBiApiConnectionDefinition,
    )
    source_definitions = (
        CHSchoolbookBiApiSourceDefinition,
    )
    translation_configs = frozenset(BASE_CONFIGS) | frozenset(CONFIGS)
