from __future__ import annotations

from dl_connector_bundle_chs3.chs3_base.api.connector import (
    BaseFileS3ApiConnectionDefinition,
    BaseFileS3ApiConnector,
    BaseFileS3TableApiSourceDefinition,
)
from dl_connector_bundle_chs3.chs3_gsheets.api.api_schema.connection import GSheetsFileS3ConnectionSchema
from dl_connector_bundle_chs3.chs3_gsheets.api.connection_info import GSheetsFileS3ConnectionInfoProvider
from dl_connector_bundle_chs3.chs3_gsheets.core.connector import (
    GSheetsFileS3CoreBackendDefinition,
    GSheetsFileS3CoreConnectionDefinition,
    GSheetsFileS3TableCoreSourceDefinition,
)
from dl_connector_bundle_chs3.chs3_gsheets.formula.constants import DIALECT_NAME_GSHEETS_V2
from dl_connector_clickhouse.api.connector import ClickHouseApiBackendDefinition


class GSheetsFileS3TableApiSourceDefinition(BaseFileS3TableApiSourceDefinition):
    core_source_def_cls = GSheetsFileS3TableCoreSourceDefinition


class GSheetsFileS3ApiConnectionDefinition(BaseFileS3ApiConnectionDefinition):
    core_conn_def_cls = GSheetsFileS3CoreConnectionDefinition
    api_generic_schema_cls = GSheetsFileS3ConnectionSchema
    info_provider_cls = GSheetsFileS3ConnectionInfoProvider


class GSheetsFileS3ApiBackendDefinition(ClickHouseApiBackendDefinition):
    core_backend_definition = GSheetsFileS3CoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_GSHEETS_V2


class GSheetsFileS3ApiConnector(BaseFileS3ApiConnector):
    backend_definition = GSheetsFileS3ApiBackendDefinition
    connection_definitions = (GSheetsFileS3ApiConnectionDefinition,)
    source_definitions = (GSheetsFileS3TableApiSourceDefinition,)
