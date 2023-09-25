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
from dl_connector_chyt.api.api_schema.source import (
    CHYTTableListDataSourceSchema,
    CHYTTableListDataSourceTemplateSchema,
    CHYTTableRangeDataSourceSchema,
    CHYTTableRangeDataSourceTemplateSchema,
)
from dl_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE

from bi_connector_chyt_internal.bi.api_schema.connection import (
    CHYTConnectionSchema,
    CHYTUserAuthConnectionSchema,
)
from bi_connector_chyt_internal.bi.connection_form.token_auth_form.form_config import (
    CHYTInternalTokenConnectionFormFactory,
)
from bi_connector_chyt_internal.bi.connection_form.user_auth_form.form_config import (
    CHYTInternalUserConnectionFormFactory,
)
from bi_connector_chyt_internal.bi.connection_info import (
    CHYTInternalTokenConnectionInfoProvider,
    CHYTUserAuthConnectionInfoProvider,
)
from bi_connector_chyt_internal.bi.i18n.localizer import CONFIGS
from bi_connector_chyt_internal.core.connector import (
    CHYTInternalCoreConnectionDefinition,
    CHYTInternalCoreConnector,
    CHYTTableCoreSourceDefinition,
    CHYTTableListCoreSourceDefinition,
    CHYTTableRangeCoreSourceDefinition,
    CHYTTableSubselectCoreSourceDefinition,
    CHYTUserAuthCoreConnectionDefinition,
    CHYTUserAuthTableCoreSourceDefinition,
    CHYTUserAuthTableListCoreSourceDefinition,
    CHYTUserAuthTableRangeCoreSourceDefinition,
    CHYTUserAuthTableSubselectCoreSourceDefinition,
)


class CHYTInternalApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = CHYTInternalCoreConnectionDefinition
    api_generic_schema_cls = CHYTConnectionSchema
    info_provider_cls = CHYTInternalTokenConnectionInfoProvider
    form_factory_cls = CHYTInternalTokenConnectionFormFactory


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


class CHYTUserAuthApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = CHYTUserAuthCoreConnectionDefinition
    api_generic_schema_cls = CHYTUserAuthConnectionSchema
    info_provider_cls = CHYTUserAuthConnectionInfoProvider
    form_factory_cls = CHYTInternalUserConnectionFormFactory


class CHYTUserAuthTableApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHYTUserAuthTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHYTUserAuthTableListApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHYTUserAuthTableListCoreSourceDefinition
    api_schema_cls = CHYTTableListDataSourceSchema
    template_api_schema_cls = CHYTTableListDataSourceTemplateSchema


class CHYTUserAuthTableRangeApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHYTUserAuthTableRangeCoreSourceDefinition
    api_schema_cls = CHYTTableRangeDataSourceSchema
    template_api_schema_cls = CHYTTableRangeDataSourceTemplateSchema


class CHYTUserAuthSubselectApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = CHYTUserAuthTableSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class CHYTInternalApiConnector(ApiConnector):
    core_connector_cls = CHYTInternalCoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (
        CHYTInternalApiConnectionDefinition,
        CHYTUserAuthApiConnectionDefinition,
    )
    source_definitions = (
        CHYTTableApiSourceDefinition,
        CHYTTableListApiSourceDefinition,
        CHYTTableRangeApiSourceDefinition,
        CHYTSubselectApiSourceDefinition,
        CHYTUserAuthTableApiSourceDefinition,
        CHYTUserAuthTableListApiSourceDefinition,
        CHYTUserAuthTableRangeApiSourceDefinition,
        CHYTUserAuthSubselectApiSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
