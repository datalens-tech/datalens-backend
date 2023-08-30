from __future__ import annotations

from bi_connector_bundle_chs3.chs3_gsheets.core.connector import (
    GSheetsFileS3TableCoreSourceDefinition,
    GSheetsFileS3CoreConnectionDefinition,
    GSheetsFileS3CoreConnector,
)

from bi_api_lib.connectors.chs3_base.connector import (
    BaseFileS3TableBiApiSourceDefinition,
    BaseFileS3BiApiConnectionDefinition,
    BaseFileS3BiApiConnector,
)
from bi_api_lib.connectors.chs3_gsheets.connection_info import GSheetsFileS3ConnectionInfoProvider
from bi_api_lib.connectors.chs3_gsheets.schemas import GSheetsFileS3ConnectionSchema


class GSheetsFileS3TableBiApiSourceDefinition(BaseFileS3TableBiApiSourceDefinition):
    core_source_def_cls = GSheetsFileS3TableCoreSourceDefinition


class GSheetsFileS3BiApiConnectionDefinition(BaseFileS3BiApiConnectionDefinition):
    core_conn_def_cls = GSheetsFileS3CoreConnectionDefinition
    api_generic_schema_cls = GSheetsFileS3ConnectionSchema
    info_provider_cls = GSheetsFileS3ConnectionInfoProvider


class GSheetsFileS3BiApiConnector(BaseFileS3BiApiConnector):
    core_connector_cls = GSheetsFileS3CoreConnector
    connection_definitions = (
        GSheetsFileS3BiApiConnectionDefinition,
    )
    source_definitions = (
        GSheetsFileS3TableBiApiSourceDefinition,
    )
