from __future__ import annotations

from typing import (
    Callable,
    ClassVar,
    Optional,
)

import attr

from dl_core.base_models import ConnCacheableDataModelMixin
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.connection_models.conn_options import ConnectOptions
from dl_core.us_connection_base import (
    ConnectionBase,
    DataSourceTemplate,
)
from dl_core.utils import secrepr
from dl_utils.utils import DataKey

from dl_connector_bitrix_gds.core.constants import (
    DEFAULT_DB,
    SOURCE_TYPE_BITRIX_GDS,
)
from dl_connector_bitrix_gds.core.dto import BitrixGDSConnDTO


@attr.s(frozen=True, hash=True)
class BitrixGDSConnectOptions(ConnectOptions):
    max_execution_time: Optional[int] = attr.ib(default=None)
    connect_timeout: Optional[int] = attr.ib(default=None)
    total_timeout: Optional[int] = attr.ib(default=None)


class BitrixGDSConnection(ConnectionBase):
    allow_cache: ClassVar[bool] = True
    source_type = SOURCE_TYPE_BITRIX_GDS

    @attr.s(kw_only=True)
    class DataModel(ConnCacheableDataModelMixin, ConnectionBase.DataModel):
        portal: str = attr.ib()
        token: str = attr.ib(repr=secrepr)

        @classmethod
        def get_secret_keys(cls) -> set[DataKey]:
            return {
                *super().get_secret_keys(),
                DataKey(parts=("token",)),
            }

    @property
    def cache_ttl_sec_override(self) -> Optional[int]:
        return self.data.cache_ttl_sec

    def get_conn_options(self) -> BitrixGDSConnectOptions:
        return super().get_conn_options().to_subclass(BitrixGDSConnectOptions)

    def get_conn_dto(self) -> BitrixGDSConnDTO:
        return BitrixGDSConnDTO(
            conn_id=self.uuid,
            portal=self.data.portal,
            token=self.data.token,
        )

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
        search_text: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        db_name: str | None = None,
    ) -> list[dict]:
        return [
            dict(db_name=DEFAULT_DB, table_name=item.table_name)
            for item in self.get_tables(
                conn_executor_factory=conn_executor_factory,
                db_name=DEFAULT_DB,
                schema_name=None,
            )
        ]

    def get_data_source_templates(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
        search_text: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        db_name: str | None = None,
    ) -> list[DataSourceTemplate]:
        return [
            DataSourceTemplate(
                title=parameters["table_name"],
                group=[],
                source_type=SOURCE_TYPE_BITRIX_GDS,
                connection_id=self.uuid,  # type: ignore  # TODO: fix
                parameters=parameters,
            )
            for parameters in self.get_parameter_combinations(conn_executor_factory=conn_executor_factory)
        ]

    @property
    def allow_public_usage(self) -> bool:
        return True
