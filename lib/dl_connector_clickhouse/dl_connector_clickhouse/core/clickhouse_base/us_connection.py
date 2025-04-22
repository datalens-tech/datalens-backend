from __future__ import annotations

from enum import Enum
from typing import (
    Any,
    Callable,
    Optional,
)

import attr

from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.connection_executors.sync_base import SyncConnExecutorBase
from dl_core.connection_models import ConnectOptions
from dl_core.us_connection_base import (
    ClassicConnectionSQL,
    ConnectionBase,
)

from dl_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions
from dl_connector_clickhouse.core.clickhouse_base.dto import ClickHouseConnDTO


@attr.s
class SubselectTemplate:
    title: str = attr.ib()
    sql_query: str = attr.ib()


class SubselectParameterType(Enum):  # TODO: maybe redundant?
    single_value = "single_value"
    list_of_values = "list_of_values"


@attr.s
class SubselectParameter:
    name: str = attr.ib()
    ss_type: SubselectParameterType = attr.ib()
    values: Any = attr.ib()


class ConnectionClickhouseBase(ClassicConnectionSQL):
    MAX_ALLOWED_MAX_EXECUTION_TIME = 70
    # this is max seconds that can be specified as max_execution_time CH option
    # magic number justification:
    #   80s - requests http timeout (calculated below based on this value)
    #   90s - our total request timeout
    # TODO ^ bind this value to the COMMON_TIMEOUT_SEC app setting or create a new one

    @attr.s(kw_only=True)
    class DataModel(ClassicConnectionSQL.DataModel):
        secure: bool = attr.ib(default=False)
        endpoint: str = attr.ib(default="")
        cluster_name: Optional[str] = attr.ib(default=None)
        max_execution_time: Optional[int] = attr.ib(default=None)
        ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)
        readonly: int = attr.ib(kw_only=True, default=2)

    def get_conn_dto(self) -> ClickHouseConnDTO:
        return ClickHouseConnDTO(
            conn_id=self.uuid,
            protocol="https" if self.data.secure else "http",
            host=self.data.host,
            multihosts=self.parse_multihosts(),
            port=self.data.port,
            endpoint=self.data.endpoint,
            cluster_name=self.data.cluster_name,
            db_name=self.data.db_name,
            username=self.data.username,
            password=self.password,  # type: ignore  # 2024-01-24 # TODO: Argument "password" to "ClickHouseConnDTO" has incompatible type "str | None"; expected "str"  [arg-type]
            secure=self.data.secure,
            ssl_ca=self.data.ssl_ca,
        )

    @staticmethod
    def get_effective_conn_options(
        base_conn_opts: ConnectOptions,
        user_max_execution_time: Optional[int],
        max_allowed_max_execution_time: int,
    ) -> CHConnectOptions:
        if user_max_execution_time is None:
            max_execution_time = None
            total_timeout = max_allowed_max_execution_time + 10
        else:
            if 0 < user_max_execution_time <= max_allowed_max_execution_time:
                total_timeout = user_max_execution_time + 10
                max_execution_time = user_max_execution_time
            else:
                total_timeout = max_allowed_max_execution_time + 10
                max_execution_time = max_allowed_max_execution_time

        return base_conn_opts.to_subclass(
            CHConnectOptions,
            total_timeout=total_timeout,
            connect_timeout=1,
            max_execution_time=max_execution_time,
        )

    def get_conn_options(self) -> CHConnectOptions:
        base = super().get_conn_options()
        return self.get_effective_conn_options(
            base_conn_opts=base,
            user_max_execution_time=self.data.max_execution_time,
            max_allowed_max_execution_time=self.MAX_ALLOWED_MAX_EXECUTION_TIME,
        )

    def get_parameter_combinations(
        self,
        conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        ch_system_dbs = (
            "system",
            "mdb_system",
            "_system",
            "information_schema",
        )
        conn_executor = conn_executor_factory(self)
        query = ConnExecutorQuery(query="SELECT `database`, `name` from `system`.`tables`", db_name="system")
        return [
            dict(db_name=db_name, table_name=table_name)
            for db_name, table_name in conn_executor.execute(query=query).get_all()
            if db_name.lower() not in ch_system_dbs
        ]
