from bi_core.connectors.base.connector import CoreSourceDefinition
from bi_core.data_source_spec.sql import (
    StandardSchemaSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from bi_core.us_manager.storage_schemas.data_source_spec_base import (
    SchemaSQLDataSourceSpecStorageSchema,
    SubselectDataSourceSpecStorageSchema,
)


class SQLTableCoreSourceDefinitionBase(CoreSourceDefinition):
    source_spec_cls = StandardSchemaSQLDataSourceSpec
    us_storage_schema_cls = SchemaSQLDataSourceSpecStorageSchema


class SQLSubselectCoreSourceDefinitionBase(CoreSourceDefinition):
    source_spec_cls = SubselectDataSourceSpec
    us_storage_schema_cls = SubselectDataSourceSpecStorageSchema
