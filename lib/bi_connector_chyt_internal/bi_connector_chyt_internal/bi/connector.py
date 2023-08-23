from __future__ import annotations

from bi_formula.core.dialect import DialectName

from bi_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)
from bi_api_connector.api_schema.source_base import (
    SQLDataSourceSchema,
    SQLDataSourceTemplateSchema,
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)

from bi_connector_chyt.bi.api_schema.source import (
    CHYTTableListDataSourceSchema,
    CHYTTableListDataSourceTemplateSchema,
    CHYTTableRangeDataSourceSchema,
    CHYTTableRangeDataSourceTemplateSchema,
)
from bi_connector_chyt_internal.bi.api_schema.connection import CHYTConnectionSchema, CHYTUserAuthConnectionSchema
from bi_connector_chyt_internal.bi.connection_info import (
    CHYTInternalTokenConnectionInfoProvider, CHYTUserAuthConnectionInfoProvider,
)
from bi_connector_chyt_internal.bi.i18n.localizer import CONFIGS

from bi_connector_chyt_internal.core.connector import (
    CHYTInternalCoreConnectionDefinition,
    CHYTUserAuthCoreConnectionDefinition,
    CHYTTableCoreSourceDefinition,
    CHYTTableListCoreSourceDefinition,
    CHYTTableRangeCoreSourceDefinition,
    CHYTTableSubselectCoreSourceDefinition,
    CHYTUserAuthTableCoreSourceDefinition,
    CHYTUserAuthTableListCoreSourceDefinition,
    CHYTUserAuthTableRangeCoreSourceDefinition,
    CHYTUserAuthTableSubselectCoreSourceDefinition,
    CHYTInternalCoreConnector,
)


class CHYTInternalBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = CHYTInternalCoreConnectionDefinition
    api_generic_schema_cls = CHYTConnectionSchema
    info_provider_cls = CHYTInternalTokenConnectionInfoProvider


class CHYTTableBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHYTTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHYTTableListBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHYTTableListCoreSourceDefinition
    api_schema_cls = CHYTTableListDataSourceSchema
    template_api_schema_cls = CHYTTableListDataSourceTemplateSchema


class CHYTTableRangeBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHYTTableRangeCoreSourceDefinition
    api_schema_cls = CHYTTableRangeDataSourceSchema
    template_api_schema_cls = CHYTTableRangeDataSourceTemplateSchema


class CHYTSubselectBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHYTTableSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class CHYTUserAuthBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = CHYTUserAuthCoreConnectionDefinition
    api_generic_schema_cls = CHYTUserAuthConnectionSchema
    info_provider_cls = CHYTUserAuthConnectionInfoProvider


class CHYTUserAuthTableBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHYTUserAuthTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class CHYTUserAuthTableListBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHYTUserAuthTableListCoreSourceDefinition
    api_schema_cls = CHYTTableListDataSourceSchema
    template_api_schema_cls = CHYTTableListDataSourceTemplateSchema


class CHYTUserAuthTableRangeBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHYTUserAuthTableRangeCoreSourceDefinition
    api_schema_cls = CHYTTableRangeDataSourceSchema
    template_api_schema_cls = CHYTTableRangeDataSourceTemplateSchema


class CHYTUserAuthSubselectBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = CHYTUserAuthTableSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class CHYTInternalBiApiConnector(BiApiConnector):
    core_connector_cls = CHYTInternalCoreConnector
    formula_dialect_name = DialectName.CLICKHOUSE
    connection_definitions = (
        CHYTInternalBiApiConnectionDefinition,
        CHYTUserAuthBiApiConnectionDefinition,
    )
    source_definitions = (
        CHYTTableBiApiSourceDefinition,
        CHYTTableListBiApiSourceDefinition,
        CHYTTableRangeBiApiSourceDefinition,
        CHYTSubselectBiApiSourceDefinition,
        CHYTUserAuthTableBiApiSourceDefinition,
        CHYTUserAuthTableListBiApiSourceDefinition,
        CHYTUserAuthTableRangeBiApiSourceDefinition,
        CHYTUserAuthSubselectBiApiSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
