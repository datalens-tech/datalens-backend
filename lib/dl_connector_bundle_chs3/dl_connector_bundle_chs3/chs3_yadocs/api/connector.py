from __future__ import annotations

from dl_connector_bundle_chs3.chs3_base.api.connector import (
    BaseFileS3ApiConnectionDefinition,
    BaseFileS3ApiConnector,
    BaseFileS3TableApiSourceDefinition,
)
from dl_connector_bundle_chs3.chs3_yadocs.api.api_schema.connection import YaDocsFileS3ConnectionSchema
from dl_connector_bundle_chs3.chs3_yadocs.api.connection_info import YaDocsFileS3ConnectionInfoProvider
from dl_connector_bundle_chs3.chs3_yadocs.core.connector import (
    YaDocsFileS3CoreBackendDefinition,
    YaDocsFileS3CoreConnectionDefinition,
    YaDocsFileS3TableCoreSourceDefinition,
)
from dl_connector_bundle_chs3.chs3_yadocs.formula.constants import DIALECT_NAME_YADOCS
from dl_connector_clickhouse.api.connector import ClickHouseApiBackendDefinition


class YaDocsFileS3TableApiSourceDefinition(BaseFileS3TableApiSourceDefinition):
    core_source_def_cls = YaDocsFileS3TableCoreSourceDefinition


class YaDocsFileS3ApiConnectionDefinition(BaseFileS3ApiConnectionDefinition):
    core_conn_def_cls = YaDocsFileS3CoreConnectionDefinition
    api_generic_schema_cls = YaDocsFileS3ConnectionSchema
    info_provider_cls = YaDocsFileS3ConnectionInfoProvider


class YaDocsFileS3ApiBackendDefinition(ClickHouseApiBackendDefinition):
    core_backend_definition = YaDocsFileS3CoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_YADOCS


class YaDocsFileS3ApiConnector(BaseFileS3ApiConnector):
    backend_definition = YaDocsFileS3ApiBackendDefinition
    connection_definitions = (YaDocsFileS3ApiConnectionDefinition,)
    source_definitions = (YaDocsFileS3TableApiSourceDefinition,)
