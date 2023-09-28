from __future__ import annotations

from dl_api_connector.api_schema.source_base import (
    SimpleDataSourceSchema,
    SimpleDataSourceTemplateSchema,
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)

from bi_connector_yql.api.yq.api_schema.connection import YQConnectionSchema
from bi_connector_yql.api.yq.connection_form.form_config import YQConnectionFormFactory
from bi_connector_yql.api.yq.connection_info import YQConnectionInfoProvider
from bi_connector_yql.api.yql_base.i18n.localizer import CONFIGS
from bi_connector_yql.core.yq.connector import (
    YQCoreConnectionDefinition,
    YQCoreConnector,
    YQSubselectCoreSourceDefinition,
    YQTableCoreSourceDefinition,
)
from bi_connector_yql.formula.constants import DIALECT_NAME_YQ


class YQTableApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = YQTableCoreSourceDefinition
    api_schema_cls = SimpleDataSourceSchema
    template_api_schema_cls = SimpleDataSourceTemplateSchema


class YQSubselectApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = YQSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class YQApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = YQCoreConnectionDefinition
    api_generic_schema_cls = YQConnectionSchema
    form_factory_cls = YQConnectionFormFactory
    info_provider_cls = YQConnectionInfoProvider


class YQApiConnector(ApiConnector):
    core_connector_cls = YQCoreConnector
    formula_dialect_name = DIALECT_NAME_YQ
    connection_definitions = (YQApiConnectionDefinition,)
    source_definitions = (
        YQTableApiSourceDefinition,
        YQSubselectApiSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
