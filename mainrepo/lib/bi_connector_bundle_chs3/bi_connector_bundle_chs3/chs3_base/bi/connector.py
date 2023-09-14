from __future__ import annotations

from bi_api_connector.connector import (
    BiApiConnectionDefinition,
    BiApiConnector,
    BiApiSourceDefinition,
)

from bi_connector_bundle_chs3.chs3_base.bi.api_schema.connection import BaseFileS3ConnectionSchema
from bi_connector_bundle_chs3.chs3_base.bi.api_schema.source import (
    BaseFileS3DataSourceSchema,
    BaseFileS3DataSourceTemplateSchema,
)
from bi_connector_bundle_chs3.chs3_base.bi.i18n.localizer import CONFIGS
from bi_connector_bundle_chs3.chs3_base.core.connector import (
    BaseFileS3CoreConnectionDefinition,
    BaseFileS3CoreConnector,
    BaseFileS3TableCoreSourceDefinition,
)
from bi_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE


class BaseFileS3TableBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = BaseFileS3TableCoreSourceDefinition
    api_schema_cls = BaseFileS3DataSourceSchema
    template_api_schema_cls = BaseFileS3DataSourceTemplateSchema


class BaseFileS3BiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = BaseFileS3CoreConnectionDefinition
    api_generic_schema_cls = BaseFileS3ConnectionSchema


class BaseFileS3BiApiConnector(BiApiConnector):
    core_connector_cls = BaseFileS3CoreConnector
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    connection_definitions = (BaseFileS3BiApiConnectionDefinition,)
    source_definitions = (BaseFileS3TableBiApiSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
