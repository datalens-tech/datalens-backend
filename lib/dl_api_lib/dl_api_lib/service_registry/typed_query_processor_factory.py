from __future__ import annotations

import abc
from typing import TYPE_CHECKING

import attr

from dl_core.data_processing.typed_query import CEBasedTypedQueryProcessor
from dl_core.us_connection_base import ConnectionBase
from dl_core.utils import FutureRef
from dl_dashsql.typed_query.processor.base import TypedQueryProcessorBase


if TYPE_CHECKING:
    from dl_core.services_registry.top_level import ServicesRegistry  # noqa


@attr.s
class TypedQueryProcessorFactory(abc.ABC):
    _service_registry_ref: FutureRef["ServicesRegistry"] = attr.ib(kw_only=True)

    @property
    def service_registry(self) -> ServicesRegistry:
        return self._service_registry_ref.ref

    @abc.abstractmethod
    def get_typed_query_processor(self, connection: ConnectionBase) -> TypedQueryProcessorBase:
        raise NotImplementedError


class DefaultQueryProcessorFactory(TypedQueryProcessorFactory):
    def get_typed_query_processor(self, connection: ConnectionBase) -> TypedQueryProcessorBase:
        ce_factory = self.service_registry.get_conn_executor_factory()
        conn_executor = ce_factory.get_async_conn_executor(conn=connection)
        tq_processor = CEBasedTypedQueryProcessor(async_conn_executor=conn_executor)
        return tq_processor
