from __future__ import annotations

import abc
from typing import AbstractSet, Callable, ClassVar, Optional, Type, TYPE_CHECKING

from sqlalchemy.orm import Query

from bi_constants.enums import SourceBackendType
from bi_core.connections_security.base import ConnSecuritySettings
from bi_core.connectors.base.query_compiler import QueryCompiler
from bi_core.connectors.settings.primitives import ConnectorSettingsDefinition
from bi_core.data_source.base import DataSource
from bi_core.data_source_spec.base import DataSourceSpec
from bi_core.us_manager.storage_schemas.data_source_spec_base import DataSourceSpecStorageSchema
from bi_core.connectors.base.lifecycle import ConnectionLifecycleManager, DefaultConnectionLifecycleManager
from bi_core.connectors.base.data_source_migration import DataSourceMigrator, DefaultDataSourceMigrator

if TYPE_CHECKING:
    from marshmallow import Schema
    from sqlalchemy.types import TypeEngine
    from bi_constants.enums import ConnectionType, CreateDSFrom
    from bi_core.us_connection_base import ConnectionBase
    from bi_core.db.native_type import GenericNativeType
    from bi_core.db.conversion_base import TypeTransformer
    from bi_core.connection_executors.common_base import ConnExecutorBase
    from bi_core.connection_executors.async_base import AsyncConnExecutorBase
    from bi_core.connection_executors.adapters.common_base import CommonBaseDirectAdapter
    from bi_core.reporting.notifications import BaseNotification


class CoreSourceDefinition(abc.ABC):
    source_type: ClassVar[CreateDSFrom]
    source_cls: ClassVar[Type[DataSource]] = DataSource  # type: ignore
    source_spec_cls: ClassVar[Type[DataSourceSpec]] = DataSourceSpec
    us_storage_schema_cls: ClassVar[Type[DataSourceSpecStorageSchema]] = DataSourceSpecStorageSchema


class CoreConnectionDefinition(abc.ABC):
    conn_type: ClassVar[ConnectionType]
    connection_cls: ClassVar[Type[ConnectionBase]]
    us_storage_schema_cls: ClassVar[Optional[Type[Schema]]] = None
    type_transformer_cls: ClassVar[Type[TypeTransformer]]
    sync_conn_executor_cls: ClassVar[Optional[Type[ConnExecutorBase]]] = None
    async_conn_executor_cls: ClassVar[Optional[Type[AsyncConnExecutorBase]]] = None
    lifecycle_manager_cls: ClassVar[Type[ConnectionLifecycleManager]] = DefaultConnectionLifecycleManager
    dialect_string: ClassVar[str]
    data_source_migrator_cls: ClassVar[Type[DataSourceMigrator]] = DefaultDataSourceMigrator
    settings_definition: ClassVar[Optional[Type[ConnectorSettingsDefinition]]] = None


class CoreConnector(abc.ABC):
    backend_type: ClassVar[SourceBackendType] = SourceBackendType.NONE
    compiler_cls: ClassVar[Type[QueryCompiler]] = QueryCompiler
    query_cls: ClassVar[Type[Query]] = Query
    connection_definitions: ClassVar[tuple[Type[CoreConnectionDefinition], ...]] = ()
    source_definitions: ClassVar[tuple[Type[CoreSourceDefinition], ...]] = ()
    sa_types: ClassVar[Optional[dict[GenericNativeType, Callable[[GenericNativeType], TypeEngine]]]] = None
    rqe_adapter_classes: ClassVar[AbstractSet[Type[CommonBaseDirectAdapter]]] = frozenset()
    conn_security: ClassVar[AbstractSet[ConnSecuritySettings]] = frozenset()
    query_fail_exceptions: frozenset[Type[Exception]] = frozenset()
    notification_classes: ClassVar[tuple[Type[BaseNotification], ...]] = ()

    @classmethod
    def registration_hook(cls) -> None:
        """Do some non-standard stuff here. Think twice before implementing."""
