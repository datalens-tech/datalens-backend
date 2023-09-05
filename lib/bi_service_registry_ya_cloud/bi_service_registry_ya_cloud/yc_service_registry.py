from typing import Optional

import attr

from bi_blackbox_client.client import BlackboxClient
from bi_cloud_integration.iam_rm_client import DLFolderServiceClient
from bi_cloud_integration.sa_creds import SACredsRetrieverFactory
from bi_cloud_integration.yc_as_client import DLASClient
from bi_cloud_integration.yc_subjects import DLYCMSClient
from bi_cloud_integration.yc_ts_client import DLTSClient
from bi_cloud_integration.yc_billing_client import YCBillingApiClient

from bi_core.rls import BaseSubjectResolver
from bi_core.services_registry.inst_specific_sr import (
    InstallationSpecificServiceRegistry,
    InstallationSpecificServiceRegistryFactory,
)
from bi_core.utils import FutureRef
from bi_core.services_registry.top_level import ServicesRegistry
from bi_api_commons_ya_cloud.cloud_manager import CloudManagerAPI

from bi_service_registry_ya_cloud.iam_subject_resolver import IAMSubjectResolver


@attr.s
class YCServiceRegistry(InstallationSpecificServiceRegistry):
    _yc_billing_host: Optional[str] = attr.ib(default=None)
    _yc_api_endpoint_iam: Optional[str] = attr.ib(default=None)
    _yc_api_endpoint_rm: Optional[str] = attr.ib(default=None)
    _yc_as_endpoint: Optional[str] = attr.ib(default=None)
    _yc_ts_endpoint: Optional[str] = attr.ib(default=None)
    _sa_creds_retriever_factory: Optional[SACredsRetrieverFactory] = attr.ib(default=None)
    _blackbox_client: Optional[BlackboxClient] = attr.ib(default=None)

    async def get_yc_billing_client(self) -> Optional[YCBillingApiClient]:
        if self._yc_billing_host:
            return await YCBillingApiClient.create_with_service_auth(yc_billing_host=self._yc_billing_host)
        return None

    async def get_yc_ms_client(self, bearer_token: Optional[str] = None) -> Optional[DLYCMSClient]:
        if not self._yc_api_endpoint_iam:
            return None
        # TODO: a more persistent client (alternatively, close the created client at consumers)
        return DLYCMSClient.create(endpoint=self._yc_api_endpoint_iam, bearer_token=bearer_token)

    async def get_yc_fs_client(self, bearer_token: Optional[str] = None) -> Optional[DLFolderServiceClient]:
        if not self._yc_api_endpoint_rm:
            return None
        # TODO: a more persistent client (alternatively, close the created client at consumers)
        return DLFolderServiceClient.create(endpoint=self._yc_api_endpoint_rm, bearer_token=bearer_token)

    async def get_yc_as_client(self) -> Optional[DLASClient]:
        if not self._yc_as_endpoint:
            return None
        # TODO: a more persistent client (alternatively, close the created client at consumers)
        return DLASClient.create(endpoint=self._yc_as_endpoint)

    async def get_yc_ts_client(self) -> Optional[DLTSClient]:
        if not self._yc_ts_endpoint:
            return None
        # TODO: a more persistent client (alternatively, close the created client at consumers)
        return DLTSClient.create(endpoint=self._yc_ts_endpoint)

    def get_sa_creds_retriever_factory(self) -> SACredsRetrieverFactory:
        if self._sa_creds_retriever_factory is not None:
            return self._sa_creds_retriever_factory
        raise ValueError('SA creds retriever factory has not been initialized')

    def get_blackbox_client(self) -> Optional[BlackboxClient]:
        return self._blackbox_client

    async def get_subject_resolver(self) -> BaseSubjectResolver:
        token = await self.get_sa_creds_retriever_factory().get_retriever().get_sa_token()
        tenant = self.service_registry.rci.tenant
        fs_client = await self.get_yc_fs_client(bearer_token=token)
        ms_client = await self.get_yc_ms_client(bearer_token=token)
        assert tenant is not None
        assert fs_client is not None
        assert ms_client is not None
        return IAMSubjectResolver(
            cloud_manager=CloudManagerAPI(
                tenant=tenant,
                yc_fs_cli=fs_client,
                yc_ms_cli=ms_client,
            )
        )


@attr.s
class YCServiceRegistryFactory(InstallationSpecificServiceRegistryFactory):
    _yc_billing_host: Optional[str] = attr.ib(default=None)
    _yc_api_endpoint_iam: Optional[str] = attr.ib(default=None)
    _yc_api_endpoint_rm: Optional[str] = attr.ib(default=None)
    _yc_as_endpoint: Optional[str] = attr.ib(default=None)
    _yc_ts_endpoint: Optional[str] = attr.ib(default=None)
    _sa_creds_retriever_factory: Optional[SACredsRetrieverFactory] = attr.ib(default=None)
    _blackbox_name: Optional[str] = attr.ib(default=None)

    def get_inst_specific_sr(self, sr_ref: FutureRef[ServicesRegistry]) -> YCServiceRegistry:
        return YCServiceRegistry(
            service_registry_ref=sr_ref,
            yc_billing_host=self._yc_billing_host,
            yc_api_endpoint_iam=self._yc_api_endpoint_iam,
            yc_api_endpoint_rm=self._yc_api_endpoint_rm,
            yc_as_endpoint=self._yc_as_endpoint,
            yc_ts_endpoint=self._yc_ts_endpoint,
            sa_creds_retriever_factory=self._sa_creds_retriever_factory,
            blackbox_client=BlackboxClient(self._blackbox_name) if self._blackbox_name is not None else None
        )
