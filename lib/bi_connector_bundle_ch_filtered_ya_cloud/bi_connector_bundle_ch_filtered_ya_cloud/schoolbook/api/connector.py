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
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.api.api_schema.connection import CHSchoolbookConnectionSchema
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.api.connection_form.form_config import (
    CHSchoolbookConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.api.connection_info import CHSchoolbookConnectionInfoProvider
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.core.connector import (
    CHSchoolbookCoreConnectionDefinition,
    CHSchoolbookCoreConnector,
    CHSchoolbookCoreSourceDefinition,
)


class CHSchoolbookApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = CHSchoolbookCoreConnectionDefinition
    api_generic_schema_cls = CHSchoolbookConnectionSchema
    info_provider_cls = CHSchoolbookConnectionInfoProvider
    form_factory_cls = CHSchoolbookConnectionFormFactory


class CHSchoolbookApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHSchoolbookCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHSchoolbookApiConnector(ApiConnector):
    core_connector_cls = CHSchoolbookCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (CHSchoolbookApiConnectionDefinition,)
    source_definitions = (CHSchoolbookApiSourceDefinition,)
    translation_configs = frozenset(BASE_CONFIGS) | frozenset(CONFIGS)
