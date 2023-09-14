from __future__ import annotations

from bi_connector_bundle_chs3.chs3_base.bi.connector import (
    BaseFileS3BiApiConnectionDefinition,
    BaseFileS3BiApiConnector,
    BaseFileS3TableBiApiSourceDefinition,
)
from bi_connector_bundle_chs3.chs3_gsheets.bi.api_schema.connection import GSheetsFileS3ConnectionSchema
from bi_connector_bundle_chs3.chs3_gsheets.bi.connection_info import GSheetsFileS3ConnectionInfoProvider
from bi_connector_bundle_chs3.chs3_gsheets.core.connector import (
    GSheetsFileS3CoreConnectionDefinition,
    GSheetsFileS3CoreConnector,
    GSheetsFileS3TableCoreSourceDefinition,
)


class GSheetsFileS3TableBiApiSourceDefinition(BaseFileS3TableBiApiSourceDefinition):
    core_source_def_cls = GSheetsFileS3TableCoreSourceDefinition


class GSheetsFileS3BiApiConnectionDefinition(BaseFileS3BiApiConnectionDefinition):
    core_conn_def_cls = GSheetsFileS3CoreConnectionDefinition
    api_generic_schema_cls = GSheetsFileS3ConnectionSchema
    info_provider_cls = GSheetsFileS3ConnectionInfoProvider


class GSheetsFileS3BiApiConnector(BaseFileS3BiApiConnector):
    core_connector_cls = GSheetsFileS3CoreConnector
    connection_definitions = (GSheetsFileS3BiApiConnectionDefinition,)
    source_definitions = (GSheetsFileS3TableBiApiSourceDefinition,)
