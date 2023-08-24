from typing import Type

from bi_core.connectors.base.connector import (
    CoreConnector, CoreSourceDefinition, CoreConnectionDefinition,
)
from bi_core.data_source_spec.type_mapping import register_data_source_spec_class
from bi_core.data_source.type_mapping import register_data_source_class
from bi_core.us_connection import register_connection_class
from bi_core.us_manager.storage_schemas.data_source_spec import register_data_source_schema
from bi_core.us_manager.storage_schemas.connection_schema_registry import register_connection_schema
from bi_core.db.sa_types import register_sa_types
from bi_core.db.conversion_base import register_type_transformer_class
from bi_core.services_registry.conn_executor_factory import (
    register_sync_conn_executor_class, register_async_conn_executor_class,
)
from bi_core.connections_security.base import (
    register_safe_dto_type, register_mdb_dto_type,
)
from bi_core.connection_executors.adapters.common_base import register_dialect_string
from bi_core.connection_executors.remote_query_executor.commons import register_adapter_class
from bi_core.db_session_utils import register_sa_query_cls, register_query_fail_exceptions
from bi_core.reporting.notifications import register_notification
from bi_core.backend_types import register_connection_backend_type
from bi_core.connectors.settings.registry import register_connector_settings_class


class CoreConnectorRegistrator:
    @classmethod
    def register_source_definition(cls, source_def: Type[CoreSourceDefinition]) -> None:
        register_data_source_spec_class(
            source_type=source_def.source_type,  # type: ignore
            spec_cls=source_def.source_spec_cls,
        )
        register_data_source_class(
            source_type=source_def.source_type,  # type: ignore
            source_cls=source_def.source_cls,
        )
        register_data_source_schema(
            source_type=source_def.source_type,  # type: ignore
            schema_cls=source_def.us_storage_schema_cls,
        )

    @classmethod
    def register_connection_definition(
            cls, conn_def: Type[CoreConnectionDefinition], connector: Type[CoreConnector],
    ) -> None:
        register_connection_class(
            new_conn_cls=conn_def.connection_cls,
            allow_ct_override=True,
            conn_type=conn_def.conn_type,
            lifecycle_manager_cls=conn_def.lifecycle_manager_cls,
        )
        register_connection_backend_type(conn_type=conn_def.conn_type, backend_type=connector.backend_type)
        register_connection_schema(conn_cls=conn_def.connection_cls, schema_cls=conn_def.us_storage_schema_cls)  # type: ignore
        register_type_transformer_class(conn_type=conn_def.conn_type, tt_cls=conn_def.type_transformer_cls)  # type: ignore
        if conn_def.sync_conn_executor_cls is not None:
            register_sync_conn_executor_class(
                conn_cls=conn_def.connection_cls, sync_ce_cls=conn_def.sync_conn_executor_cls)
        if conn_def.async_conn_executor_cls is not None:
            register_async_conn_executor_class(
                conn_cls=conn_def.connection_cls, async_ce_cls=conn_def.async_conn_executor_cls)
        register_dialect_string(conn_type=conn_def.conn_type, dialect_str=conn_def.dialect_string)
        if conn_def.settings_class is not None:
            register_connector_settings_class(conn_type=conn_def.conn_type, settings_class=conn_def.settings_class)

    @classmethod
    def register_connector(cls, connector: Type[CoreConnector]) -> None:
        for source_def in connector.source_definitions:
            cls.register_source_definition(source_def=source_def)
        for conn_def in connector.connection_definitions:
            cls.register_connection_definition(conn_def=conn_def, connector=connector)
        register_sa_types(connector.sa_types or {})
        for dto_cls in connector.safe_dto_classes:
            register_safe_dto_type(dto_cls=dto_cls)
        for dto_cls in connector.mdb_dto_classes:
            register_mdb_dto_type(dto_cls=dto_cls)
        for adapter_cls in connector.rqe_adapter_classes:
            register_adapter_class(adapter_cls=adapter_cls)
        register_sa_query_cls(backend_type=connector.backend_type, query_cls=connector.query_cls)
        register_query_fail_exceptions(exception_classes=connector.query_fail_exceptions)
        for notification_cls in connector.notification_classes:
            register_notification()(notification_cls)  # it is a parameterized decorator

        connector.registration_hook()  # for custom actions


CONN_REG_CORE = CoreConnectorRegistrator
