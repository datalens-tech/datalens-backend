import attr
from typing import Optional

from bi_core.services_registry.inst_specific_sr import (
    InstallationSpecificServiceRegistry,
    InstallationSpecificServiceRegistryFactory,
)
from bi_core.rls import BaseSubjectResolver
from bi_core.utils import FutureRef
from bi_core.services_registry.top_level import ServicesRegistry

from bi_dls_client.dls_client import DLSClient
from bi_dls_client.subject_resolver import DLSSubjectResolver


@attr.s
class YTServiceRegistry(InstallationSpecificServiceRegistry):
    _dls_host: Optional[str] = attr.ib(default=None)
    _dls_api_key: Optional[str] = attr.ib(default=None)

    async def get_subject_resolver(self) -> BaseSubjectResolver:
        assert self._dls_host
        assert self._dls_api_key
        dls_client = DLSClient(
            host=self._dls_host,
            secret_api_key=self._dls_api_key,
            rci=self.service_registry.rci,
        )
        return DLSSubjectResolver(dls_client)


@attr.s
class YTServiceRegistryFactory(InstallationSpecificServiceRegistryFactory):
    _dls_host: Optional[str] = attr.ib(default=None)
    _dls_api_key: Optional[str] = attr.ib(default=None)

    def get_inst_specific_sr(self, sr_ref: FutureRef[ServicesRegistry]) -> YTServiceRegistry:
        return YTServiceRegistry(
            service_registry_ref=sr_ref,
            dls_host=self._dls_host,
            dls_api_key=self._dls_api_key
        )
