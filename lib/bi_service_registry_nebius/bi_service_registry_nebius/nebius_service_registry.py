from typing import Optional

import attr

from bi_cloud_integration.sa_creds import SACredsRetrieverFactory
from bi_cloud_integration.service_registry_cloud import BaseCloudServiceRegistry
from dl_core.services_registry.inst_specific_sr import InstallationSpecificServiceRegistryFactory
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.utils import FutureRef


@attr.s
class NebiusServiceRegistry(BaseCloudServiceRegistry):
    pass

@attr.s
class NebiusServiceRegistryFactory(InstallationSpecificServiceRegistryFactory):
    _yc_billing_host: Optional[str] = attr.ib(default=None)
    _yc_api_endpoint_iam: Optional[str] = attr.ib(default=None)
    _yc_api_endpoint_rm: Optional[str] = attr.ib(default=None)
    _yc_api_endpoint_mdb: Optional[str] = attr.ib(default=None)
    _yc_as_endpoint: Optional[str] = attr.ib(default=None)
    _yc_ts_endpoint: Optional[str] = attr.ib(default=None)
    _sa_creds_retriever_factory: Optional[SACredsRetrieverFactory] = attr.ib(default=None)

    def get_inst_specific_sr(self, sr_ref: FutureRef[ServicesRegistry]) -> NebiusServiceRegistry:
        return NebiusServiceRegistry(
            service_registry_ref=sr_ref,
            yc_billing_host=self._yc_billing_host,
            yc_api_endpoint_iam=self._yc_api_endpoint_iam,
            yc_api_endpoint_rm=self._yc_api_endpoint_rm,
            yc_api_endpoint_mdb=self._yc_api_endpoint_mdb,
            yc_as_endpoint=self._yc_as_endpoint,
            yc_ts_endpoint=self._yc_ts_endpoint,
            sa_creds_retriever_factory=self._sa_creds_retriever_factory,
        )
