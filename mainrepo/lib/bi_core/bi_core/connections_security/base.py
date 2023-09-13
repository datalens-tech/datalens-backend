from __future__ import annotations

import abc
import logging
from typing import FrozenSet, Type

import attr

from bi_core import exc
from bi_core.connection_models import ConnDTO, DefaultSQLDTO
from bi_core.connectors.clickhouse_base.dto import ClickHouseConnDTO

LOGGER = logging.getLogger(__name__)


class ConnectionSecurityManager(metaclass=abc.ABCMeta):
    """
    This class performs security checks against connection executions.
    Assumed that instance of this class will be created on each request.
    Individual checks should be performed in context of instance fields that are representing current request context.
    """

    @abc.abstractmethod
    def is_safe_connection(self, conn_dto: ConnDTO) -> bool:
        """
        Must return False if connection is potentially unsafe and should be executed in isolated environment.
        """

    # TODO FIX: determine if we need dedicated method
    @abc.abstractmethod
    def is_internal_connection(self, conn_dto: ConnDTO) -> bool:
        """
        Mostly costyl for bi_core/data_processing/selectors/utils.py:get_query_type()
        Potentially it's behaviour should be same as .is_safe_connection() (TBD: check it),
         but we need exactly the same data to determine if connection is internal.
        """


class InsecureConnectionSecurityManager(ConnectionSecurityManager):
    def is_safe_connection(self, conn_dto: ConnDTO) -> bool:
        return True

    def is_internal_connection(self, conn_dto: ConnDTO) -> bool:
        return True


@attr.s
class CloudConnectionSecurityManager(ConnectionSecurityManager):
    _samples_ch_hosts: FrozenSet[str] = attr.ib()

    _SAFE_DTO_TYPES: set[Type[ConnDTO]] = set()

    _MDB_SQL_DTO_TYPES: set[Type[ConnDTO]] = set()

    def is_safe_connection(self, conn_dto: ConnDTO) -> bool:
        # TODO FIX: Move on top after moving MDB utils in dedicated module (right now will cause import loop)
        from bi_core.mdb_utils import MDBDomainManager

        mdb_man = MDBDomainManager.from_env()

        LOGGER.info('Checking if ConnDTO %r is safe', type(conn_dto))

        # Hosts are not entered by user
        if type(conn_dto) in self._SAFE_DTO_TYPES:
            LOGGER.info('%r in SAFE_DTO_TYPES', type(conn_dto))
            return True

        # Samples hosts
        if isinstance(conn_dto, ClickHouseConnDTO) and \
                all(host in self._samples_ch_hosts for host in conn_dto.multihosts):
            LOGGER.info('Clickhouse hosts %r are in sample host list', conn_dto.multihosts)
            return True

        if isinstance(conn_dto, DefaultSQLDTO) and type(conn_dto) in self._MDB_SQL_DTO_TYPES:
            # MDB hosts
            hosts = conn_dto.get_all_hosts()
            LOGGER.info('Checking if hosts %r belong to mdb', hosts)
            if all(mdb_man.host_in_mdb(host) for host in hosts):
                LOGGER.info('All hosts (%r) look like mdb hosts', hosts)
                return True
            else:
                LOGGER.info('Hosts (%r) do not belong to mdb (%r)', hosts, mdb_man.settings.mdb_domains)

            if len(set(mdb_man.host_in_mdb(host) for host in hosts)) > 1:
                raise exc.ConnectionConfigurationError(
                    'Internal (MDB) hosts and external hosts can not be mixed in multihost configuration')

        return False

    # TODO FIX: Generalize code with .is_safe_connection() after checking if logic is fully compatible
    def is_internal_connection(self, conn_dto: ConnDTO) -> bool:
        # TODO FIX: Move on top after moving MDB utils in dedicated module (right now will cause import loop)
        from bi_core.mdb_utils import MDBDomainManager

        mdb_man = MDBDomainManager.from_env()

        if isinstance(conn_dto, DefaultSQLDTO):
            all_hosts = conn_dto.get_all_hosts()

            if all(mdb_man.host_in_mdb(host) for host in all_hosts):
                return True

            if all(host in self._samples_ch_hosts for host in all_hosts):
                return True

        return False


@attr.s
class InternalConnectionSecurityManager(ConnectionSecurityManager):
    def is_safe_connection(self, conn_dto: ConnDTO) -> bool:
        return True

    def is_internal_connection(self, conn_dto: ConnDTO) -> bool:
        return True


def register_safe_dto_type(dto_cls: Type[ConnDTO]) -> None:
    CloudConnectionSecurityManager._SAFE_DTO_TYPES.add(dto_cls)  # type: ignore  # TODO: fix


def register_mdb_dto_type(dto_cls: Type[ConnDTO]) -> None:
    CloudConnectionSecurityManager._MDB_SQL_DTO_TYPES.add(dto_cls)
