from __future__ import annotations

import datetime
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
)

from dl_constants.enums import UserDataType
from dl_core import exc
from dl_core.data_source.sql import PseudoSQLDataSource
from dl_core.db import (
    SchemaColumn,
    SchemaInfo,
)

from dl_connector_metrica.core.constants import (
    CONNECTION_TYPE_APPMETRICA_API,
    CONNECTION_TYPE_METRICA_API,
)
from dl_connector_metrica.core.us_connection import MetrikaApiConnection


if TYPE_CHECKING:
    from dl_core.connection_executors.async_base import AsyncConnExecutorBase
    from dl_core.connection_executors.sync_base import SyncConnExecutorBase


LOGGER = logging.getLogger(__name__)


class MetrikaApiDataSource(PseudoSQLDataSource):
    store_raw_schema: ClassVar[bool] = False
    preview_enabled: ClassVar[bool] = False
    supports_schema_update: ClassVar[bool] = False

    conn_type = CONNECTION_TYPE_METRICA_API

    def get_schema_info(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> SchemaInfo:
        assert self.saved_raw_schema is not None
        return SchemaInfo.from_schema(self.saved_raw_schema)

    def _check_existence(self, conn_executor_factory: Callable[[], SyncConnExecutorBase]) -> bool:
        return True

    async def _check_existence_async(self, conn_executor_factory: Callable[[], AsyncConnExecutorBase]) -> bool:
        return True

    @property
    def saved_raw_schema(self) -> Optional[list[SchemaColumn]]:
        assert self.conn_type is not None
        db_name = self.db_name
        assert db_name is not None
        conn_cls = self.get_connection_cls()
        assert issubclass(conn_cls, MetrikaApiConnection)
        return [
            sch_column.clone(source_id=self.id)
            for sch_column in conn_cls.get_raw_schema(
                metrica_namespace=db_name,
                actual_conn_type=self.conn_type,
            )
        ]

    def get_expression_value_range(self, col_name: str) -> tuple[Any, Any]:
        """Date/datetime column value ranges are defined as ``(<counter creation>, <now>)``"""
        try:
            assert self.saved_raw_schema is not None
            column = next(col for col in self.saved_raw_schema if col.name == col_name)
        except StopIteration:
            raise exc.InvalidColumnError("Invalid field name") from None

        if column.user_type not in (UserDataType.date, UserDataType.datetime, UserDataType.genericdatetime):
            raise exc.InvalidColumnError("Invalid field for value range")

        creation_date = self.connection.data.counter_creation_date
        now = datetime.datetime.utcnow()
        if column.user_type in (UserDataType.datetime, UserDataType.genericdatetime):
            min_value = datetime.datetime(creation_date.year, creation_date.month, creation_date.day)
            max_value = now
        else:
            min_value = creation_date
            max_value = now.date()  # type: ignore  # TODO: fix

        return min_value, max_value


class AppMetrikaApiDataSource(MetrikaApiDataSource):
    conn_type = CONNECTION_TYPE_APPMETRICA_API
