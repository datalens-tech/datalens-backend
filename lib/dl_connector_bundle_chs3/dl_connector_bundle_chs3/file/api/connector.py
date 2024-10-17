from __future__ import annotations

from dl_connector_bundle_chs3.chs3_base.api.connector import (
    BaseFileS3ApiConnectionDefinition,
    BaseFileS3ApiConnector,
    BaseFileS3TableApiSourceDefinition,
)
from dl_connector_bundle_chs3.file.api.api_schema.connection import FileS3ConnectionSchema
from dl_connector_bundle_chs3.file.api.connection_info import FileS3ConnectionInfoProvider
from dl_connector_bundle_chs3.file.core.connector import (
    FileS3CoreBackendDefinition,
    FileS3CoreConnectionDefinition,
    FileS3TableCoreSourceDefinition,
)
from dl_connector_bundle_chs3.file.formula.constants import DIALECT_NAME_FILE
from dl_connector_clickhouse.api.connector import ClickHouseApiBackendDefinition


class FileS3TableApiSourceDefinition(BaseFileS3TableApiSourceDefinition):
    core_source_def_cls = FileS3TableCoreSourceDefinition


class FileS3ApiConnectionDefinition(BaseFileS3ApiConnectionDefinition):
    core_conn_def_cls = FileS3CoreConnectionDefinition
    api_generic_schema_cls = FileS3ConnectionSchema
    info_provider_cls = FileS3ConnectionInfoProvider


class FileS3ApiBackendDefinition(ClickHouseApiBackendDefinition):
    core_backend_definition = FileS3CoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_FILE


class FileS3ApiConnector(BaseFileS3ApiConnector):
    backend_definition = FileS3ApiBackendDefinition
    connection_definitions = (FileS3ApiConnectionDefinition,)
    source_definitions = (FileS3TableApiSourceDefinition,)
