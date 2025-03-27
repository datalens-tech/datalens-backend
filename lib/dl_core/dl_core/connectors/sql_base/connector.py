from dl_core.connectors.base.connector import CoreSourceDefinition
from dl_core.data_source_spec.sql import (
    StandardSchemaSQLDataSourceSpec,
    SubselectDataSourceSpec,
)
from dl_core.data_source_spec_mutator.sql import SubselectDataSourceSpecMutator
from dl_core.us_manager.storage_schemas.data_source_spec_base import (
    SchemaSQLDataSourceSpecStorageSchema,
    SubselectDataSourceSpecStorageSchema,
)


class SQLTableCoreSourceDefinitionBase(CoreSourceDefinition):
    source_spec_cls = StandardSchemaSQLDataSourceSpec
    us_storage_schema_cls = SchemaSQLDataSourceSpecStorageSchema


class SQLSubselectCoreSourceDefinitionBase(CoreSourceDefinition):
    source_spec_cls = SubselectDataSourceSpec
    source_spec_mutator_cls = SubselectDataSourceSpecMutator
    us_storage_schema_cls = SubselectDataSourceSpecStorageSchema
