from __future__ import annotations

from dl_connector_bundle_chs3.chs3_base.api.connector import (
    BaseFileS3BiApiConnectionDefinition,
    BaseFileS3BiApiConnector,
    BaseFileS3TableBiApiSourceDefinition,
)
from dl_connector_bundle_chs3.file.api.api_schema.connection import FileS3ConnectionSchema
from dl_connector_bundle_chs3.file.api.connection_info import FileS3ConnectionInfoProvider
from dl_connector_bundle_chs3.file.core.connector import (
    FileS3CoreConnectionDefinition,
    FileS3CoreConnector,
    FileS3TableCoreSourceDefinition,
)


class FileS3TableBiApiSourceDefinition(BaseFileS3TableBiApiSourceDefinition):
    core_source_def_cls = FileS3TableCoreSourceDefinition


class FileS3BiApiConnectionDefinition(BaseFileS3BiApiConnectionDefinition):
    core_conn_def_cls = FileS3CoreConnectionDefinition
    api_generic_schema_cls = FileS3ConnectionSchema
    info_provider_cls = FileS3ConnectionInfoProvider


class FileS3BiApiConnector(BaseFileS3BiApiConnector):
    core_connector_cls = FileS3CoreConnector
    connection_definitions = (FileS3BiApiConnectionDefinition,)
    source_definitions = (FileS3TableBiApiSourceDefinition,)
