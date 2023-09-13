from __future__ import annotations

from typing import Optional, ClassVar, Type, Protocol, TYPE_CHECKING
import os
import logging

from bi_core import exc
from bi_api_commons_ya_cloud.models import TenantYCOrganization, TenantYCFolder
from bi_service_registry_ya_cloud.yc_service_registry import YCServiceRegistry

if TYPE_CHECKING:
    from bi_core.us_manager.us_manager import USManagerBase
    from bi_core.services_registry.top_level import ServicesRegistry
    from bi_core.us_connection_base import ConnectionBase
    from bi_core.base_models import ConnMDBDataModelMixin
    from bi_cloud_integration.mdb import MDBClusterServiceBaseClient


LOGGER = logging.getLogger(__name__)


class _MDBConnectionProtocol(Protocol):
    _us_manager: USManagerBase
    _service_registry: ServicesRegistry

    MDB_CLIENT_CLS: ClassVar[Type[MDBClusterServiceBaseClient]]

    @property
    def us_manager(self) -> USManagerBase: ...

    @property
    def data(self) -> ConnMDBDataModelMixin: ...

    def parse_multihosts(self) -> tuple[str, ...]: ...

    async def validate_new_data(
            self,
            changes: Optional[dict] = None,
            original_version: Optional[ConnectionBase] = None,
    ) -> None: ...


class MDBConnectionMixin:
    MDB_CLIENT_CLS: ClassVar[Type[MDBClusterServiceBaseClient]]

    async def validate_new_data(
            self: _MDBConnectionProtocol,
            changes: Optional[dict] = None,
            original_version: Optional[ConnectionBase] = None,
    ) -> None:
        await super().validate_new_data(changes=changes, original_version=original_version)  # type: ignore

        if self.data.mdb_cluster_id is None:
            return

        services_registry = self.us_manager.get_services_registry()
        issr = services_registry.get_installation_specific_service_registry(YCServiceRegistry)
        cli_mdb = await issr.get_mdb_client(self.MDB_CLIENT_CLS)
        cli_folder_service = await issr.get_yc_folder_service_user_client()
        cli_cloud_service = await issr.get_yc_cloud_service_user_client()

        cluster_folder_id = await cli_mdb.get_cluster_folder_id(self.data.mdb_cluster_id)
        cluster_hosts = await cli_mdb.get_cluster_hosts(self.data.mdb_cluster_id)
        cluster_cloud_id = await cli_folder_service.resolve_folder_id_to_cloud_id(cluster_folder_id)
        cluster_org_id = await cli_cloud_service.resolve_cloud_id_to_org_id(cluster_cloud_id)

        tenant = self._us_manager.bi_context.tenant
        current_org_id: str
        if isinstance(tenant, TenantYCOrganization):
            current_org_id = tenant.org_id
        elif isinstance(tenant, TenantYCFolder):
            current_cloud_id = await cli_folder_service.resolve_folder_id_to_cloud_id(tenant.folder_id)
            current_org_id = await cli_cloud_service.resolve_cloud_id_to_org_id(current_cloud_id)
        else:
            raise Exception('Unsupported tenant type')

        # Temporary flag    # TODO: remove raise_on_check_fail flag
        raise_on_check_fail = os.environ.get('MDB_ORG_CHECK_ENABLED', '0') == '1'

        if cluster_org_id != current_org_id:
            LOGGER.info(
                f'MDB cluster and DataLens tenant organization id mismatch: '
                f'"{cluster_org_id}" != "{current_org_id}"',
            )
            if raise_on_check_fail and not self.data.skip_mdb_org_check:    # TODO: remove `raise_on_check_fail` flag
                raise exc.ConnectionConfigurationError(
                    f'Unable to use Managed Databases cluster from another organization. '
                    f'Current DataLens organization id: "{current_org_id}", '
                    f'database organization id: "{cluster_org_id}".'
                )

        if not all(host in cluster_hosts for host in self.parse_multihosts()):
            LOGGER.info(
                f'MDB cluster and DataLens tenant organization id mismatch: '
                f'"{cluster_org_id}" != "{current_org_id}"',
            )
            if raise_on_check_fail and not self.data.skip_mdb_org_check:    # TODO: remove `raise_on_check_fail` flag
                raise exc.ConnectionConfigurationError('All hosts in connection must belong to specified cluster')

        self.data.is_verified_mdb_org = True
        self.data.mdb_folder_id = cluster_folder_id
