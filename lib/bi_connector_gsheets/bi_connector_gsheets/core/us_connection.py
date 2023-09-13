from __future__ import annotations

from typing import Callable, ClassVar, Optional

import attr

from bi_core.base_models import ConnCacheableDataModelMixin
from bi_core.connection_models.conn_options import ConnectOptions
from bi_core.connection_executors.sync_base import SyncConnExecutorBase
from bi_core.us_connection_base import ConnectionBase, DataSourceTemplate, ExecutorBasedMixin

from bi_connector_gsheets.core.constants import SOURCE_TYPE_GSHEETS
from bi_connector_gsheets.core.dto import GSheetsConnDTO


@attr.s(frozen=True, hash=True)
class GSheetsConnectOptions(ConnectOptions):
    max_execution_time: Optional[int] = attr.ib(default=None)
    connect_timeout: Optional[int] = attr.ib(default=None)
    total_timeout: Optional[int] = attr.ib(default=None)


class GSheetsConnection(ExecutorBasedMixin, ConnectionBase):  # type: ignore  # TODO: fix

    allow_cache: ClassVar[bool] = True
    # Tricky: is_always_user_source: ClassVar[bool] = True
    use_locked_cache: ClassVar[bool] = True

    @attr.s(kw_only=True)
    class DataModel(ConnCacheableDataModelMixin, ConnectionBase.DataModel):
        url: str = attr.ib()

    @property
    def allow_public_usage(self) -> bool:
        return False

    @property
    def cache_ttl_sec_override(self) -> Optional[int]:
        return self.data.cache_ttl_sec

    def get_conn_options(self) -> GSheetsConnectOptions:
        return super().get_conn_options().to_subclass(
            GSheetsConnectOptions,
            # max_execution_time=66,
            # connect_timeout=5,
            # total_timeout=75,
        )

    def get_conn_dto(self) -> GSheetsConnDTO:
        return GSheetsConnDTO(
            conn_id=self.uuid,
            sheets_url=self.data.url,
        )

    def get_data_source_templates(
            self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[DataSourceTemplate]:
        return [
            DataSourceTemplate(
                title='current sheet',  # TODO?: save the current sheet name in the connection or get it on the fly.
                group=[],
                source_type=SOURCE_TYPE_GSHEETS,
                connection_id=self.uuid,  # type: ignore  # TODO: fix
                parameters={},
            ),
        ]
