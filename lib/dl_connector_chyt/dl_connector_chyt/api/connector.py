from __future__ import annotations

from dl_api_connector.api_schema.source_base import (
    SQLDataSourceSchema,
    SQLDataSourceTemplateSchema,
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)

from dl_connector_chyt.api.api_schema.connection import CHYTConnectionSchema
from dl_connector_chyt.api.api_schema.source import (
    CHYTTableListDataSourceSchema,
    CHYTTableListDataSourceTemplateSchema,
    CHYTTableRangeDataSourceSchema,
    CHYTTableRangeDataSourceTemplateSchema,
)
from dl_connector_chyt.api.connection_form.form_config import CHYTConnectionFormFactory
from dl_connector_chyt.api.connection_info import CHYTConnectionInfoProvider
from dl_connector_chyt.api.i18n.localizer import CONFIGS
from dl_connector_chyt.core.connector import (
    CHYTCoreConnectionDefinition,
    CHYTCoreConnector,
    CHYTTableCoreSourceDefinition,
    CHYTTableListCoreSourceDefinition,
    CHYTTableRangeCoreSourceDefinition,
    CHYTTableSubselectCoreSourceDefinition,
)
from dl_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE


class CHYTApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = CHYTCoreConnectionDefinition
    api_generic_schema_cls = CHYTConnectionSchema
    info_provider_cls = CHYTConnectionInfoProvider
    form_factory_cls = CHYTConnectionFormFactory


class CHYTTableApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHYTTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHYTTableListApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHYTTableListCoreSourceDefinition
    api_schema_cls = CHYTTableListDataSourceSchema
    template_api_schema_cls = CHYTTableListDataSourceTemplateSchema


class CHYTTableRangeApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHYTTableRangeCoreSourceDefinition
    api_schema_cls = CHYTTableRangeDataSourceSchema
    template_api_schema_cls = CHYTTableRangeDataSourceTemplateSchema


class CHYTSubselectApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHYTTableSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class CHYTApiConnector(ApiConnector):
    core_connector_cls = CHYTCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (CHYTApiConnectionDefinition,)
    source_definitions = (
        CHYTTableApiSourceDefinition,
        CHYTTableListApiSourceDefinition,
        CHYTTableRangeApiSourceDefinition,
        CHYTSubselectApiSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
