from __future__ import annotations

from dl_connector_bundle_chs3.chs3_base.api.connector import (
    BaseFileS3ApiConnectionDefinition,
    BaseFileS3ApiConnector,
    BaseFileS3TableApiSourceDefinition,
)
from dl_connector_bundle_chs3.file.api.api_schema.connection import FileS3ConnectionSchema
from dl_connector_bundle_chs3.file.api.connection_info import FileS3ConnectionInfoProvider
from dl_connector_bundle_chs3.file.core.connector import (
    FileS3CoreConnectionDefinition,
    FileS3CoreConnector,
    FileS3TableCoreSourceDefinition,
)


class FileS3TableApiSourceDefinition(BaseFileS3TableApiSourceDefinition):
    core_source_def_cls = FileS3TableCoreSourceDefinition


class FileS3ApiConnectionDefinition(BaseFileS3ApiConnectionDefinition):
    core_conn_def_cls = FileS3CoreConnectionDefinition
    api_generic_schema_cls = FileS3ConnectionSchema
    info_provider_cls = FileS3ConnectionInfoProvider


class FileS3ApiConnector(BaseFileS3ApiConnector):
    connection_definitions = (FileS3ApiConnectionDefinition,)
    source_definitions = (FileS3TableApiSourceDefinition,)
