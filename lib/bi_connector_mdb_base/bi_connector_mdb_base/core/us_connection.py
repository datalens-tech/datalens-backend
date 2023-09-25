from __future__ import annotations

import logging
import os
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Optional,
    Protocol,
    Type,
)

from bi_api_commons_ya_cloud.models import (
    TenantYCFolder,
    TenantYCOrganization,
)
from bi_api_lib_ya.connections_security.base import MDBDomainManager
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistry
from dl_constants.enums import ConnectionType
from dl_core import exc
from dl_core.connection_models import ConnectOptions

from bi_connector_mdb_base.core.base_models import MDBConnectOptionsMixin
from bi_connector_mdb_base.core.settings import MDBConnectorSettings


if TYPE_CHECKING:
    from bi_cloud_integration.mdb import MDBClusterServiceBaseClient
    from dl_core.services_registry.top_level import ServicesRegistry
    from dl_core.us_connection_base import ConnectionBase

    from bi_connector_mdb_base.core.base_models import ConnMDBDataModelMixin


LOGGER = logging.getLogger(__name__)


class _MDBConnectOptionsProtocol(MDBConnectOptionsMixin, ConnectOptions):
    ...


class _MDBConnectionProtocol(Protocol):
    MDB_CLIENT_CLS: ClassVar[Type[MDBClusterServiceBaseClient]]

    conn_type: ConnectionType

    @property
    def data(self) -> ConnMDBDataModelMixin:
        ...

    def parse_multihosts(self) -> tuple[str, ...]:
        ...

    def get_conn_options(self) -> _MDBConnectOptionsProtocol:
        ...

    async def validate_new_data(
        self,
        services_registry: ServicesRegistry,
        changes: Optional[dict] = None,
        original_version: Optional[ConnectionBase] = None,
    ) -> None:
        ...


class MDBConnectionMixin:
    MDB_CLIENT_CLS: ClassVar[Type[MDBClusterServiceBaseClient]]

    async def validate_new_data(
        self: _MDBConnectionProtocol,
        services_registry: ServicesRegistry,
        changes: Optional[dict] = None,
        original_version: Optional[ConnectionBase] = None,
    ) -> None:
        await super().validate_new_data(  # type: ignore
            services_registry=services_registry,
            changes=changes,
            original_version=original_version,
        )

        cs = services_registry.get_connectors_settings(self.conn_type)
        assert cs is not None and isinstance(cs, MDBConnectorSettings)
        if not cs.MDB_ORG_CHECK_ENABLED:
            self.data.skip_mdb_org_check = True
            return

        if self.data.mdb_cluster_id == "":
            self.data.mdb_cluster_id = None
        if self.data.mdb_cluster_id is None:
            db_domain_manager = services_registry.get_conn_executor_factory().conn_security_manager.db_domain_manager
            assert isinstance(db_domain_manager, MDBDomainManager)
            if any(db_domain_manager.host_in_mdb(host) for host in self.parse_multihosts()):
                raise exc.ConnectionConfigurationError(
                    'Connection to Managed Databases should be created via "Select in folder" scenario.',
                )
            return  # Nothing to check anymore - all connection hosts are not MDB

        issr = services_registry.get_installation_specific_service_registry(YCServiceRegistry)
        cli_mdb = await issr.get_mdb_client(self.MDB_CLIENT_CLS)
        cli_folder_service = await issr.get_yc_folder_service_user_client()
        cli_cloud_service = await issr.get_yc_cloud_service_user_client()

        cluster_folder_id = await cli_mdb.get_cluster_folder_id(self.data.mdb_cluster_id)
        cluster_hosts = await cli_mdb.get_cluster_hosts(self.data.mdb_cluster_id)
        cluster_cloud_id = await cli_folder_service.resolve_folder_id_to_cloud_id(cluster_folder_id)
        cluster_org_id = await cli_cloud_service.resolve_cloud_id_to_org_id(cluster_cloud_id)

        tenant = services_registry.rci.tenant
        current_org_id: str
        if isinstance(tenant, TenantYCOrganization):
            current_org_id = tenant.org_id
        elif isinstance(tenant, TenantYCFolder):
            current_cloud_id = await cli_folder_service.resolve_folder_id_to_cloud_id(tenant.folder_id)
            current_org_id = await cli_cloud_service.resolve_cloud_id_to_org_id(current_cloud_id)
        else:
            raise Exception("Unsupported tenant type")

        if cluster_org_id != current_org_id:
            LOGGER.info(
                f"MDB cluster and DataLens tenant organization id mismatch: "
                f'"{cluster_org_id}" != "{current_org_id}"',
            )
            if not self.data.skip_mdb_org_check:
                raise exc.ConnectionConfigurationError(
                    f"Unable to use Managed Databases cluster from another organization. "
                    f'Current DataLens organization id: "{current_org_id}", '
                    f'database organization id: "{cluster_org_id}".'
                )

        if not all(host in cluster_hosts for host in self.parse_multihosts()):  # TODO: check CNAMEs
            LOGGER.info(
                f"MDB cluster and DataLens tenant organization id mismatch: "
                f'"{cluster_org_id}" != "{current_org_id}"',
            )
            if not self.data.skip_mdb_org_check:
                raise exc.ConnectionConfigurationError("All hosts in connection must belong to specified cluster")

        self.data.is_verified_mdb_org = True
        self.data.mdb_folder_id = cluster_folder_id

    def get_conn_options(self: _MDBConnectionProtocol) -> _MDBConnectOptionsProtocol:
        use_managed_network = False
        if self.data.skip_mdb_org_check:
            LOGGER.info("`skip_mdb_org_check` is True => set `use_managed_network = True`")
            use_managed_network = True
        elif self.data.is_verified_mdb_org:
            LOGGER.info("`is_verified_mdb_org` is True => set `use_managed_network = True`")
            use_managed_network = True
        return super().get_conn_options().clone(use_managed_network=use_managed_network)  # type: ignore
