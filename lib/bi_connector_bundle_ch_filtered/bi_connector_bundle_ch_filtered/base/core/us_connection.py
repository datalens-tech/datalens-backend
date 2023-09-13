from __future__ import annotations

import abc
from typing import Callable, Generic, Optional, TypeVar

import attr

from bi_configs.connectors_settings import CHFrozenConnectorSettings

from bi_core.connection_executors.sync_base import SyncConnExecutorBase
from bi_core.us_connection_base import ConnectionBase, ConnectionHardcodedDataMixin
from bi_core.utils import parse_comma_separated_hosts

from bi_connector_clickhouse.core.clickhouse_base.conn_options import CHConnectOptions
from bi_connector_clickhouse.core.clickhouse_base.dto import ClickHouseConnDTO
from bi_connector_clickhouse.core.clickhouse_base.us_connection import (
    ConnectionClickhouseBase, SubselectParameter, SubselectTemplate,
)


class ConnectionClickhouseFilteredBase(ConnectionClickhouseBase, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def allowed_tables(self) -> list[str]:
        pass

    @property
    def subselect_templates(self) -> tuple[SubselectTemplate, ...]:
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


CH_FILTERED_SETTINGS_TV = TypeVar('CH_FILTERED_SETTINGS_TV', bound=CHFrozenConnectorSettings)


class ConnectionCHFilteredHardcodedDataBase(  # type: ignore  # TODO: fix
    ConnectionHardcodedDataMixin[CH_FILTERED_SETTINGS_TV],
    ConnectionClickhouseFilteredBase,
    Generic[CH_FILTERED_SETTINGS_TV],
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
