from __future__ import annotations

from bi_constants.enums import CreateDSFrom

from bi_api_connector.api_schema.source_base import (
    SimpleDataSourceTemplateSchema, SubselectDataSourceSchema, SubselectDataSourceTemplateSchema,
)
from bi_api_connector.api_schema.source import (
    register_source_api_schema, register_source_template_api_schema,
    CHYDBTableDataSourceSchema, CHYDBTableDataSourceTemplateSchema,
)


def register_non_connectorized_source_schemas() -> None:
    # FIXME: Connectorize
    for source_type in (
        CreateDSFrom.CSV,
    ):
        register_source_api_schema(source_type=source_type, schema_cls=SubselectDataSourceSchema)
        register_source_template_api_schema(source_type=source_type, schema_cls=SimpleDataSourceTemplateSchema)

    for source_type in (
            CreateDSFrom.CHYDB_SUBSELECT,
    ):
        register_source_api_schema(source_type=source_type, schema_cls=SubselectDataSourceSchema)
        register_source_template_api_schema(source_type=source_type, schema_cls=SubselectDataSourceTemplateSchema)

    for source_type in (
            CreateDSFrom.CHYDB_TABLE,
    ):
        register_source_api_schema(source_type=source_type, schema_cls=CHYDBTableDataSourceSchema)
        register_source_template_api_schema(source_type=source_type, schema_cls=CHYDBTableDataSourceTemplateSchema)
