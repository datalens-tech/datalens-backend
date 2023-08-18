from __future__ import annotations

from bi_connector_bundle_chs3.chs3_base.core.connector import (
    BaseFileS3TableCoreSourceDefinition,
    BaseFileS3CoreConnectionDefinition,
    BaseFileS3CoreConnector,
)

from bi_formula.core.dialect import DialectName

from bi_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)

from bi_api_lib.connectors.chs3_base.schemas import (
    BaseFileS3ConnectionSchema,
    BaseFileS3DataSourceSchema,
    BaseFileS3DataSourceTemplateSchema,
)


class BaseFileS3TableBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = BaseFileS3TableCoreSourceDefinition
    api_schema_cls = BaseFileS3DataSourceSchema
    template_api_schema_cls = BaseFileS3DataSourceTemplateSchema


class BaseFileS3BiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = BaseFileS3CoreConnectionDefinition
    api_generic_schema_cls = BaseFileS3ConnectionSchema


class BaseFileS3BiApiConnector(BiApiConnector):
    core_connector_cls = BaseFileS3CoreConnector
    formula_dialect_name = DialectName.CLICKHOUSE
    connection_definitions = (
        BaseFileS3BiApiConnectionDefinition,
    )
    source_definitions = (
        BaseFileS3TableBiApiSourceDefinition,
    )
