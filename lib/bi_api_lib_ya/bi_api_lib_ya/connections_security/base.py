from __future__ import annotations

import logging
from typing import (
    ClassVar,
    Type,
)

import aiodns
import attr

from dl_core import exc
from dl_core.connection_models import (
    ConnDTO,
    DefaultSQLDTO,
)
from dl_core.connections_security.base import (
    ConnectionSafetyChecker,
    DBDomainManager,
    GenericConnectionSecurityManager,
)


LOGGER = logging.getLogger(__name__)


@attr.s
class MDBDomainManagerSettings:
    managed_network_enabled: bool = attr.ib(default=True)
    mdb_domains: tuple[str, ...] = attr.ib(factory=tuple)
    mdb_cname_domains: tuple[str, ...] = attr.ib(factory=tuple)
    renaming_map: dict[str, str] = attr.ib(factory=dict)


@attr.s
class MDBDomainManager(DBDomainManager):
    _settings: MDBDomainManagerSettings = attr.ib()

    def _host_is_mdb_cname(self, host: str) -> bool:
        return host.endswith(self._settings.mdb_cname_domains)

    def _get_host_for_managed_network(self, host: str) -> str:
        # This hack replaces an external ("overlay", user-network) A-record
        # domain name with an internal ("underlay") AAAA-record domain name.
        # Relevant puncher access:
        # https://puncher.yandex-team.ru/?source=_DL_EXT_BACK_PROD_NETS_&destination=_CLOUD_MDB_PROD_CLIENT_NETS_
        # Relevant cloud documentation process:
        # https://st.yandex-team.ru/YCDOCS-380
        for domain in self._settings.mdb_domains:
            if host.endswith(domain):
                return host.replace(domain, self._settings.renaming_map[domain])

        raise ValueError(f"Incorrect host: {host}")

    def host_in_mdb(self, host: str) -> bool:
        # WARNING: `'abcd.mdb.yandexcloud.net.'` is also a vaild hostname.
        # There should be a more correct way of doing things like that.
        return host.endswith(self._settings.mdb_domains)

    async def normalize_mdb_host(self, original_host: str) -> str:
        working_host = original_host
        LOGGER.info("Normalizing host: %s", working_host)

        if self._host_is_mdb_cname(working_host):
            LOGGER.info("Host looks like MDB CNAME for master: %s", working_host)
            resolver = aiodns.DNSResolver()
            resp = await resolver.query(original_host, "CNAME")
            working_host = resp.cname
            LOGGER.info("Host transformed: %s", working_host)

        if self.host_in_mdb(working_host):
            LOGGER.info("Host looks like MDB host: %s", working_host)
            working_host = self._get_host_for_managed_network(working_host)
            LOGGER.info("Host transformed: %s", working_host)

        LOGGER.info("Host normalization results: %s", working_host)
        return working_host


@attr.s(kw_only=True)
class CloudConnectionSecurityManager(GenericConnectionSecurityManager):
    _samples_ch_hosts: frozenset[str] = attr.ib()

    # TODO FIX: https://st.yandex-team.ru/BI-2582
    #  Generalize code with .is_safe_connection() after checking if logic is fully compatible
    def is_internal_connection(self, conn_dto: ConnDTO) -> bool:
        if isinstance(conn_dto, DefaultSQLDTO):
            all_hosts = conn_dto.get_all_hosts()

            if all(self.db_domain_manager.host_in_mdb(host) for host in all_hosts):
                return True

            if all(host in self._samples_ch_hosts for host in all_hosts):
                return True

        return False


@attr.s(kw_only=True)
class InternalConnectionSecurityManager(GenericConnectionSecurityManager):
    def is_internal_connection(self, conn_dto: ConnDTO) -> bool:
        return True


@attr.s(kw_only=True)
class MDBConnectionSafetyChecker(ConnectionSafetyChecker):
    """MDB hosts"""

    _db_domain_manager: MDBDomainManager = attr.ib()

    _DTO_TYPES: ClassVar[set[Type[ConnDTO]]] = set()

    def is_safe_connection(self, conn_dto: ConnDTO) -> bool:
        if isinstance(conn_dto, DefaultSQLDTO) and type(conn_dto) in self._DTO_TYPES:
            hosts = conn_dto.get_all_hosts()
            LOGGER.info("Checking if hosts %r belong to mdb", hosts)
            if all(self._db_domain_manager.host_in_mdb(host) for host in hosts):
                LOGGER.info("All hosts (%r) look like mdb hosts", hosts)
                return True
            else:
                LOGGER.info("Hosts (%r) do not belong to MDB", hosts)

            if len(set(self._db_domain_manager.host_in_mdb(host) for host in hosts)) > 1:
                raise exc.ConnectionConfigurationError(
                    "Internal (MDB) hosts and external hosts can not be mixed in multihost configuration"
                )

        return False


@attr.s(kw_only=True)
class SamplesConnectionSafetyChecker(ConnectionSafetyChecker):
    """Samples hosts"""

    _DTO_TYPES: ClassVar[set[Type[ConnDTO]]] = set()

    _samples_hosts: frozenset[str] = attr.ib()

    def is_safe_connection(self, conn_dto: ConnDTO) -> bool:
        if isinstance(conn_dto, DefaultSQLDTO):
            if all(host in self._samples_hosts for host in conn_dto.get_all_hosts()):
                LOGGER.info("Hosts %r are in sample host list", conn_dto.get_all_hosts())
                return True
        return False
