from __future__ import annotations

import abc
from typing import Callable, Optional, Tuple, Any
from enum import Enum

import attr

from bi_configs.connectors_settings import CHFrozenConnectorSettings

from bi_core.connection_models import ConnectOptions
from bi_core.connection_executors.common_base import ConnExecutorQuery
from bi_core.connectors.clickhouse_base.conn_options import CHConnectOptions
from bi_core.connection_executors.sync_base import SyncConnExecutorBase
from bi_core.us_connection_base import ConnectionBase, ClassicConnectionSQL, ConnectionHardcodedDataMixin
from bi_core.base_models import ConnMDBMixin
from bi_core.utils import parse_comma_separated_hosts
from bi_core.connectors.clickhouse_base.dto import ClickHouseConnDTO


@attr.s
class SubselectTemplate:
    title: str = attr.ib()
    sql_query: str = attr.ib()


class SubselectParameterType(Enum):  # TODO: maybe redundant?
    single_value = 'single_value'
    list_of_values = 'list_of_values'


@attr.s
class SubselectParameter:
    name: str = attr.ib()
    ss_type: SubselectParameterType = attr.ib()
    values: Any = attr.ib()


class ConnectionClickhouseBase(ClassicConnectionSQL):

    MAX_ALLOWED_MAX_EXECUTION_TIME = 280    # 290 sec - requests http timeout, 300 sec - uwsgi harakiri

    @attr.s(kw_only=True)
    class DataModel(ConnMDBMixin, ClassicConnectionSQL.DataModel):
        secure: bool = attr.ib(default=False)
        endpoint: str = attr.ib(default='')
        cluster_name: Optional[str] = attr.ib(default=None)
        max_execution_time: Optional[int] = attr.ib(default=None)
        ssl_ca: Optional[str] = attr.ib(kw_only=True, default=None)

    def get_conn_dto(self) -> ClickHouseConnDTO:
        return ClickHouseConnDTO(
            conn_id=self.uuid,
            # TODO: remove me after CHARTS-1095
            protocol='https' if self.data.port in (8443, 443) or self.data.secure else 'http',
            host=self.data.host,
            multihosts=self.parse_multihosts(),  # type: ignore  # TODO: fix
            port=self.data.port,
            endpoint=self.data.endpoint,
            cluster_name=self.data.cluster_name,
            db_name=self.data.db_name,
            username=self.data.username,
            password=self.password,
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
            self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        ch_system_dbs = (
            'system',
            'mdb_system',
            '_system',
            'information_schema',   # in DoubleCloud
        )
        conn_executor = conn_executor_factory(self)
        query = ConnExecutorQuery(query='SELECT `database`, `name` from `system`.`tables`', db_name='system')
        return [
            dict(db_name=db_name, table_name=table_name)
            for db_name, table_name in conn_executor.execute(query=query).get_all()
            if db_name.lower() not in ch_system_dbs
        ]


class ConnectionClickhouseFilteredBase(ConnectionClickhouseBase, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def allowed_tables(self) -> list[str]:
        pass

    @property
    def subselect_templates(self) -> Tuple[SubselectTemplate, ...]:
        return tuple()

    def get_subselect_template_by_title(self, title: str) -> SubselectTemplate:
        for sst in self.subselect_templates:
            if sst.title == title:
                return sst
        raise ValueError(f'Unknown subselect template {title}')

    @property
    def subselect_parameters(self) -> list[SubselectParameter]:
        return []

    def get_parameter_combinations(
            self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase],
    ) -> list[dict]:
        all_combinations = super().get_parameter_combinations(conn_executor_factory=conn_executor_factory)
        filtered_combinations = list(filter(
            lambda c: c['db_name'] == self.db_name and c['table_name'] in self.allowed_tables,
            all_combinations,
        ))
        filtered_combinations.extend([
            {'db_name': self.db_name, 'table_name': sst.title}
            for sst in self.subselect_templates
        ])
        return filtered_combinations

    def test(self, conn_executor_factory: Callable[[ConnectionBase], SyncConnExecutorBase]) -> None:
        """
        Don't execute `select 1` on our service databases - it's useless because user can't
        manage it anyway.
        """
        pass


class ConnectionCHFilteredHardcodedDataBase(  # type: ignore  # TODO: fix
    ConnectionHardcodedDataMixin[CHFrozenConnectorSettings],
    ConnectionClickhouseFilteredBase,
    metaclass=abc.ABCMeta,
):
    @property
    def db_name(self) -> Optional[str]:
        return self._connector_settings.DB_NAME

    @property
    def allowed_tables(self) -> list[str]:
        return self._connector_settings.ALLOWED_TABLES

    @property
    def subselect_templates(self) -> tuple[SubselectTemplate, ...]:
        return tuple(
            SubselectTemplate(title=sst['title'], sql_query=sst['sql_query'])  # type: ignore  # TODO: fix
            for sst in self._connector_settings.SUBSELECT_TEMPLATES
        )

    def get_conn_dto(self) -> 'ClickHouseConnDTO':
        cs = self._connector_settings
        conn_dto = attr.evolve(
            super().get_conn_dto(),

            protocol='https' if cs.SECURE else 'http',
            host=cs.HOST,
            multihosts=parse_comma_separated_hosts(cs.HOST),  # type: ignore  # TODO: fix
            port=cs.PORT,
            db_name=cs.DB_NAME,
            username=cs.USERNAME,
            password=cs.PASSWORD,
        )
        return conn_dto

    @property
    def allow_public_usage(self) -> bool:
        return self._connector_settings.ALLOW_PUBLIC_USAGE

    def get_conn_options(self) -> CHConnectOptions:
        return super().get_conn_options().clone(
            use_managed_network=self._connector_settings.USE_MANAGED_NETWORK,
        )
