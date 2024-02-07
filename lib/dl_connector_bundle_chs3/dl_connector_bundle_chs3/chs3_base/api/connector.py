from __future__ import annotations

from dl_api_connector.connector import (
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)

from dl_connector_bundle_chs3.chs3_base.api.api_schema.connection import BaseFileS3ConnectionSchema
from dl_connector_bundle_chs3.chs3_base.api.api_schema.source import (
    BaseFileS3DataSourceSchema,
    BaseFileS3DataSourceTemplateSchema,
)
from dl_connector_bundle_chs3.chs3_base.api.i18n.localizer import CONFIGS
from dl_connector_bundle_chs3.chs3_base.core.connector import (
    BaseFileS3CoreConnectionDefinition,
    BaseFileS3TableCoreSourceDefinition,
)
from dl_connector_clickhouse.api.connector import ClickHouseApiBackendDefinition


class BaseFileS3TableApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = BaseFileS3TableCoreSourceDefinition
    api_schema_cls = BaseFileS3DataSourceSchema
    template_api_schema_cls = BaseFileS3DataSourceTemplateSchema


class BaseFileS3ApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = BaseFileS3CoreConnectionDefinition
    api_generic_schema_cls = BaseFileS3ConnectionSchema


class BaseFileS3ApiConnector(ApiConnector):
    backend_definition = ClickHouseApiBackendDefinition
    connection_definitions = (BaseFileS3ApiConnectionDefinition,)
    source_definitions = (BaseFileS3TableApiSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
