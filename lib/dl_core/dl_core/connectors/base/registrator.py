from typing import (
    TYPE_CHECKING,
)

from dl_constants.enums import SourceBackendType
from dl_core.backend_types import register_connection_backend_type
from dl_core.connection_executors.adapters.common_base import register_dialect_string
from dl_core.connection_executors.remote_query_executor.commons import register_adapter_class
from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)
from dl_core.connectors.base.dashsql import register_custom_dash_sql_key_names
from dl_core.connectors.base.data_source_migration import register_data_source_migrator
from dl_core.connectors.base.export_import import register_export_import_allowed
from dl_core.connectors.settings.registry import register_connector_settings_class
from dl_core.data_processing.query_compiler_registry import register_sa_query_compiler_cls
from dl_core.data_source.type_mapping import register_data_source_class
from dl_core.data_source_spec.type_mapping import register_data_source_spec_class
from dl_core.db_session_utils import (
    register_query_fail_exceptions,
    register_sa_query_cls,
)
from dl_core.reporting.notifications import register_notification
from dl_core.services_registry.conn_executor_factory import (
    register_async_conn_executor_class,
    register_sync_conn_executor_class,
)
from dl_core.us_connection import register_connection_class
from dl_core.us_manager.storage_schemas.connection_schema_registry import register_connection_schema
from dl_core.us_manager.storage_schemas.data_source_spec import register_data_source_schema
from dl_dashsql.registry import register_dash_sql_param_literalizer_cls
from dl_type_transformer.sa_types import register_sa_types
from dl_type_transformer.type_transformer import register_type_transformer_class


if TYPE_CHECKING:
    from dl_core.connections_security.base import ConnSecuritySettings  # noqa: F401


class CoreConnectorRegistrator:
    @classmethod
    def register_source_definition(cls, source_def: type[CoreSourceDefinition]) -> None:
        register_data_source_spec_class(
            source_type=source_def.source_type,
            spec_cls=source_def.source_spec_cls,
        )
        register_data_source_class(
            source_type=source_def.source_type,
            source_cls=source_def.source_cls,
        )
        register_data_source_schema(
            source_type=source_def.source_type,
            schema_cls=source_def.us_storage_schema_cls,
        )

    @classmethod
    def register_connection_definition(
        cls,
        conn_def: type[CoreConnectionDefinition],
        backend_type: SourceBackendType,
    ) -> None:
        register_connection_class(
            new_conn_cls=conn_def.connection_cls,
            allow_ct_override=True,
            conn_type=conn_def.conn_type,
            lifecycle_manager_cls=conn_def.lifecycle_manager_cls,
            schema_migration_cls=conn_def.schema_migration_cls,
        )
        register_connection_backend_type(conn_type=conn_def.conn_type, backend_type=backend_type)
        register_connection_schema(conn_cls=conn_def.connection_cls, schema_cls=conn_def.us_storage_schema_cls)  # type: ignore  # 2024-01-30 # TODO: Argument "schema_cls" to "register_connection_schema" has incompatible type "type[Schema] | None"; expected "type[Schema]"  [arg-type]
        register_type_transformer_class(conn_type=conn_def.conn_type, tt_cls=conn_def.type_transformer_cls)
        if conn_def.sync_conn_executor_cls is not None:
            register_sync_conn_executor_class(
                conn_cls=conn_def.connection_cls, sync_ce_cls=conn_def.sync_conn_executor_cls
            )
        if conn_def.async_conn_executor_cls is not None:
            register_async_conn_executor_class(
                conn_cls=conn_def.connection_cls, async_ce_cls=conn_def.async_conn_executor_cls
            )
        register_dialect_string(conn_type=conn_def.conn_type, dialect_str=conn_def.dialect_string)
        if conn_def.settings_definition is not None:
            register_connector_settings_class(
                conn_type=conn_def.conn_type,
                settings_class=conn_def.settings_definition.settings_class,
                fallback=conn_def.settings_definition.fallback,
            )
        register_data_source_migrator(conn_type=conn_def.conn_type, migrator_cls=conn_def.data_source_migrator_cls)
        register_custom_dash_sql_key_names(conn_type=conn_def.conn_type, key_names=conn_def.custom_dashsql_key_names)
        register_export_import_allowed(conn_type=conn_def.conn_type, value=conn_def.allow_export)

    @classmethod
    def register_backend_definition(cls, backend_def: type[CoreBackendDefinition]) -> None:
        backend_type = backend_def.backend_type
        register_sa_query_cls(backend_type=backend_type, query_cls=backend_def.query_cls)
        register_sa_query_compiler_cls(
            backend_type=backend_type,
            sa_query_compiler_cls=backend_def.compiler_cls,
        )
        register_dash_sql_param_literalizer_cls(
            backend_type=backend_type,
            literalizer_cls=backend_def.dashsql_literalizer_cls,
        )

    @classmethod
    def register_connector(cls, connector: type[CoreConnector]) -> None:
        backend_def = connector.backend_definition
        cls.register_backend_definition(backend_def=backend_def)
        for source_def in connector.source_definitions:
            cls.register_source_definition(source_def=source_def)
        for conn_def in connector.connection_definitions:
            cls.register_connection_definition(conn_def=conn_def, backend_type=backend_def.backend_type)

        register_sa_types(connector.sa_types or {})
        for conn_sec_settings in connector.conn_security:  # type: ConnSecuritySettings
            dto_types = conn_sec_settings.dtos
            conn_sec_settings.security_checker_cls.register_dto_types(dto_types)
        for adapter_cls in connector.rqe_adapter_classes:
            register_adapter_class(adapter_cls=adapter_cls)
        register_query_fail_exceptions(exception_classes=connector.query_fail_exceptions)
        for notification_cls in connector.notification_classes:
            register_notification()(notification_cls)  # it is a parameterized decorator

        connector.registration_hook()  # for custom actions


CONN_REG_CORE = CoreConnectorRegistrator
