from __future__ import annotations

import attr

from bi_api_lib_ya.connections_security.base import (
    CloudConnectionSecurityManager,
    InternalConnectionSecurityManager,
    MDBConnectionSafetyChecker,
    MDBDomainManager,
    MDBDomainManagerSettings,
    SamplesConnectionSafetyChecker,
)
from dl_core.connections_security.base import (
    ConnectionSecurityManager,
    InsecureConnectionSafetyChecker,
    NonUserInputConnectionSafetyChecker,
)
from dl_core.services_registry.env_manager_factory import DefaultEnvManagerFactory


@attr.s(kw_only=True)
class IntranetEnvManagerFactory(DefaultEnvManagerFactory):
    _mdb_domain_manager_settings: MDBDomainManagerSettings = attr.ib()

    def make_security_manager(self) -> ConnectionSecurityManager:
        db_domain_manager = MDBDomainManager(settings=self._mdb_domain_manager_settings)
        return InternalConnectionSecurityManager(
            conn_sec_checkers=[
                InsecureConnectionSafetyChecker(),
            ],
            db_domain_manager=db_domain_manager,
        )


@attr.s(kw_only=True)
class CloudEnvManagerFactory(DefaultEnvManagerFactory):
    _mdb_domain_manager_settings: MDBDomainManagerSettings = attr.ib()
    _samples_ch_hosts: list[str] = attr.ib()

    def make_security_manager(self) -> ConnectionSecurityManager:
        db_domain_manager = MDBDomainManager(settings=self._mdb_domain_manager_settings)
        samples_ch_hosts = frozenset(self._samples_ch_hosts)
        return CloudConnectionSecurityManager(
            conn_sec_checkers=[
                NonUserInputConnectionSafetyChecker(),
                SamplesConnectionSafetyChecker(samples_hosts=samples_ch_hosts),
                MDBConnectionSafetyChecker(db_domain_manager=db_domain_manager),
            ],
            db_domain_manager=db_domain_manager,
            samples_ch_hosts=samples_ch_hosts,
        )


@attr.s(kw_only=True)
class DataCloudEnvManagerFactory(DefaultEnvManagerFactory):
    _mdb_domain_manager_settings: MDBDomainManagerSettings = attr.ib()
    _samples_ch_hosts: list[str] = attr.ib()

    def make_security_manager(self) -> ConnectionSecurityManager:
        db_domain_manager = MDBDomainManager(settings=self._mdb_domain_manager_settings)
        samples_ch_hosts = frozenset(self._samples_ch_hosts)
        return CloudConnectionSecurityManager(
            conn_sec_checkers=[
                NonUserInputConnectionSafetyChecker(),
                SamplesConnectionSafetyChecker(samples_hosts=samples_ch_hosts),
                MDBConnectionSafetyChecker(db_domain_manager=db_domain_manager),
            ],
            db_domain_manager=db_domain_manager,
            samples_ch_hosts=samples_ch_hosts,
        )
