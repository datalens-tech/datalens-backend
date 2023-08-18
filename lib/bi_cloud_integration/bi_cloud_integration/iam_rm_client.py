from __future__ import annotations

import datetime
from typing import List, Optional, Sequence, Any

import attr
from grpc import Channel

from yandex.cloud.priv.access.access_pb2 import (
    AccessBinding, AccessBindingAction, AccessBindingDelta,
    ListAccessBindingsRequest, Subject, UpdateAccessBindingsRequest,
)
from yandex.cloud.priv.iam.v1.key_service_pb2 import (
    CreateKeyRequest, DeleteKeyRequest, GetKeyRequest, ListKeysRequest,
)
from yandex.cloud.priv.iam.v1.key_service_pb2_grpc import KeyServiceStub
from yandex.cloud.priv.iam.v1.service_account_pb2 import ServiceAccount
# https://a.yandex-team.ru/arc/trunk/arcadia/cloud/bitbucket/private-api/yandex/cloud/priv/iam/v1/service_account_service.proto
from yandex.cloud.priv.iam.v1.service_account_service_pb2 import (
    CreateServiceAccountRequest, DeleteServiceAccountRequest,
    GetServiceAccountRequest, ListServiceAccountsRequest,
)
from yandex.cloud.priv.iam.v1.service_account_service_pb2_grpc import ServiceAccountServiceStub
from yandex.cloud.priv.resourcemanager.v1.folder_service_pb2 import ResolveFoldersRequest
from yandex.cloud.priv.resourcemanager.v1.folder_service_pb2_grpc import FolderServiceStub
from yandex.cloud.priv.resourcemanager.v1.operation_service_pb2 import GetOperationRequest
from yandex.cloud.priv.resourcemanager.v1.operation_service_pb2_grpc import OperationServiceStub

from bi_cloud_integration import model
from bi_cloud_integration.exc import grpc_exc_handler
from bi_cloud_integration.model import (
    ServiceAccountData,
    ServiceAccountKeyWithPrivateKeyData,
    ServiceAccountKeyData,
    Operation,
)
from bi_cloud_integration.yc_client_base import DLYCSingleServiceClient, DLYCServiceConfig
from bi_cloud_integration.yc_operation_base import (
    NoResponseOpConverter,
    DLGenericOperationService,
    YCOperationFailed,
    YCOperationAwaitTimeout,
)


@attr.s
class DLSAServiceClient(DLYCSingleServiceClient):
    """
    IAM (Identity & Access Management)
    Service Account management client.
    Endpoint example: 'api-adapter.private-api.ycp.cloud-preprod.yandex.net:443'
    """

    service_cls = ServiceAccountServiceStub

    @staticmethod
    def _make_service_account_data(grpc_sa: Any) -> ServiceAccountData:
        return ServiceAccountData(id=grpc_sa.id, name=grpc_sa.name, description=grpc_sa.description)

    def _handle_grpc_error(self, err: Any) -> None:
        """ Disable the handle-by-default here, to specify the `operation_code` explicitly """
        pass

    def get_svc_acct_sync(self, svc_acct_id: str) -> Optional[ServiceAccountData]:
        sa = self.service.Get(GetServiceAccountRequest(service_account_id=svc_acct_id))
        return self._make_service_account_data(sa)

    def list_svc_accts_sync(self, folder_id: str) -> List[ServiceAccountData]:
        rs = self.service.List(ListServiceAccountsRequest(folder_id=folder_id))
        return [
            ServiceAccountData(id=sa.id, name=sa.name, description=sa.description)
            for sa in rs.service_accounts]

    def create_svc_acct_sync(self, folder_id: str, name: str, description: str) -> ServiceAccountData:
        """
        :param folder_id:
        :param name: Name of service account
        :param description: Descritpion of
        :return: Created service account data
        """
        with grpc_exc_handler(operation_code='sa_create'):
            rs = self.service.Create(CreateServiceAccountRequest(
                folder_id=folder_id,
                name=name,
                description=description,
            ))

        if rs.response.Is(ServiceAccount.DESCRIPTOR):  # type: ignore
            sa = ServiceAccount()
            rs.response.Unpack(sa)
            return self._make_service_account_data(sa)
        else:
            raise ValueError(f"Unexpected response class: {rs.response.type_url}")

    def delete_svc_acct_sync(self, svc_acct_id: str) -> None:
        return self.service.Delete(DeleteServiceAccountRequest(service_account_id=svc_acct_id))


@attr.s
class DLKeyServiceClient(DLYCSingleServiceClient):
    """
    IAM (Identity & Access Management)
    Key management client.
    Endpoint example: 'api-adapter.private-api.ycp.cloud-preprod.yandex.net:443'
    """

    service_cls = KeyServiceStub

    @staticmethod
    def _pb_key_to_key_data(pb_key: Any) -> ServiceAccountKeyData:
        return ServiceAccountKeyData(
            id=pb_key.id,
            description=pb_key.description,
            svc_acct_id=pb_key.service_account_id,
            created_at=datetime.datetime.fromtimestamp(pb_key.created_at.seconds, datetime.timezone.utc),
            public_key=pb_key.public_key,
        )

    def create_svc_acct_key_sync(self, service_account_id: str, description: str) -> ServiceAccountKeyWithPrivateKeyData:
        rs = self.service.Create(CreateKeyRequest(
            service_account_id=service_account_id,
            description=description,
        ))
        pb_key = rs.key
        return ServiceAccountKeyWithPrivateKeyData(
            svc_acct_key_data=self._pb_key_to_key_data(pb_key),
            private_key=rs.private_key,
        )

    def list_svc_acct_keys_sync(
            self,
            service_account_id: str,
            _page_size: Optional[int] = None,
    ) -> List[ServiceAccountKeyData]:
        page_size = self._get_page_size(_page_size)
        page_token = None
        result: List[ServiceAccountKeyData] = []

        while True:
            rs = self.service.List(ListKeysRequest(
                service_account_id=service_account_id,
                page_size=page_size,
                page_token=page_token,
            ))
            result.extend(
                self._pb_key_to_key_data(pb_key)
                for pb_key in rs.keys)
            page_token = rs.next_page_token
            if not page_token:
                break

        return result

    def get_svc_acct_key_sync(self, key_id: str) -> ServiceAccountKeyData:
        rs = self.service.Get(GetKeyRequest(key_id=key_id))
        return self._pb_key_to_key_data(rs)

    def delete_svc_acct_key_sync(self, key_id: str) -> None:
        self.service.Delete(DeleteKeyRequest(key_id=key_id))


@attr.s
class DLFolderServiceClient(DLYCSingleServiceClient):
    """
    RM (Resource Management)
    Folder service client.
    Endpoint example: 'api-adapter.private-api.ycp.cloud-preprod.yandex.net:443'
    """

    service_cls = FolderServiceStub

    async def resolve_folder_id_to_cloud_id(self, folder_id: str) -> str:
        req = ResolveFoldersRequest(folder_ids=[folder_id])
        resp = await self.service.Resolve.aio(req)
        resolved = resp.resolved_folders
        if len(resolved) != 1:
            raise Exception(f"Expected one resolved folder, got {resp!r}")
        return resolved[0].cloud_id

    def list_svc_acct_role_ids_on_folder_sync(
            self,
            svc_acct_id: str,
            folder_id: str,
            acct_type: str = 'serviceAccount',
            _page_size: Optional[int] = None,
    ) -> List[str]:
        page_size = self._get_page_size(_page_size)
        page_token = None
        result: List[str] = []

        while True:
            rs = self.service.ListAccessBindings(ListAccessBindingsRequest(
                resource_id=folder_id,
                page_size=page_size,
                page_token=page_token,
                private_call=False,
            ))
            result.extend(
                access_binding.role_id
                for access_binding in rs.access_bindings
                if access_binding.subject.id == svc_acct_id
                and access_binding.subject.type == acct_type
            )
            page_token = rs.next_page_token
            if not page_token:
                break

        return result

    def modify_folder_access_bindings_for_svc_acct_sync(
            self,
            svc_acct_id: str,
            folder_id: str,
            role_ids: Sequence[str],
            action: model.AccessBindingAction,
            acct_type: str = "serviceAccount",
    ) -> Operation:
        rq = UpdateAccessBindingsRequest(
            resource_id=folder_id,
            access_binding_deltas=[
                AccessBindingDelta(
                    action={
                        model.AccessBindingAction.ADD: AccessBindingAction.ADD,  # type: ignore
                        model.AccessBindingAction.REMOVE: AccessBindingAction.REMOVE  # type: ignore
                    }[action],
                    access_binding=AccessBinding(
                        role_id=role_id,
                        subject=Subject(
                            id=svc_acct_id,
                            type=acct_type,
                        ),
                    )
                ) for role_id in role_ids
            ],
            private_call=False,
        )
        rs = self.service.UpdateAccessBindings(rq)
        op_converter = NoResponseOpConverter()
        return op_converter.convert_operation(rs)


class DLRMOperationService(DLGenericOperationService[Operation]):
    service_cls = OperationServiceStub

    def _create_get_operation_request(self, operation_id: str) -> Any:
        return GetOperationRequest(operation_id=operation_id)


@attr.s
class IAMRMClient:
    """
    Facade for IAM/RM gRPC API to manipulate
     - service accounts
     - folder permissions
    """

    _request_id: str = attr.ib()
    _iam_token: str = attr.ib(repr=False)

    _iam_endpoint: str = attr.ib()
    _rm_endpoint: str = attr.ib()

    _iam_channel: Optional[Channel] = attr.ib(default=None)
    _rm_channel: Optional[Channel] = attr.ib(default=None)

    _operation_poll_delay: float = attr.ib(default=0.1)
    _operation_poll_timeout: float = attr.ib(default=90.0)

    _sa_service_cli: DLSAServiceClient = attr.ib(init=False)
    _folder_service_cli: DLFolderServiceClient = attr.ib(init=False)
    _key_service_cli: DLKeyServiceClient = attr.ib(init=False)
    _rm_operation_service: DLRMOperationService = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        iam_service_config = DLYCServiceConfig(endpoint=self._iam_endpoint)
        rm_service_config = DLYCServiceConfig(endpoint=self._rm_endpoint)

        if self._iam_channel is None:
            self._iam_channel = iam_service_config.make_channel()
        if self._rm_channel is None:
            self._rm_channel = rm_service_config.make_channel()

        iam_channel = self._iam_channel
        rm_channel = self._rm_channel

        iam_token = self._iam_token

        # IAM
        self._sa_service_cli = DLSAServiceClient(
            service_config=iam_service_config, channel=iam_channel, bearer_token=iam_token)
        self._key_service_cli = DLKeyServiceClient(
            service_config=iam_service_config, channel=iam_channel, bearer_token=iam_token)

        # RM
        self._folder_service_cli = DLFolderServiceClient(
            service_config=rm_service_config, channel=rm_channel, bearer_token=iam_token)
        self._rm_operation_service = DLRMOperationService(
            service_config=rm_service_config, channel=rm_channel, bearer_token=iam_token)

    def get_svc_acct_sync(self, svc_acct_id: str) -> Optional[ServiceAccountData]:
        with grpc_exc_handler("sa_get"):
            return self._sa_service_cli.get_svc_acct_sync(svc_acct_id=svc_acct_id)

    def list_svc_accts_sync(self, folder_id: str) -> List[ServiceAccountData]:
        return self._sa_service_cli.list_svc_accts_sync(folder_id=folder_id)

    def create_svc_acct_sync(self, folder_id: str, name: str, description: str) -> ServiceAccountData:
        return self._sa_service_cli.create_svc_acct_sync(
            folder_id=folder_id, name=name, description=description,
        )

    def delete_svc_acct_sync(self, svc_acct_id: str) -> None:
        return self._sa_service_cli.delete_svc_acct_sync(svc_acct_id=svc_acct_id)

    def create_svc_acct_key_sync(self, service_account_id: str, description: str) -> ServiceAccountKeyWithPrivateKeyData:
        return self._key_service_cli.create_svc_acct_key_sync(
            service_account_id=service_account_id, description=description,
        )

    def list_svc_acct_keys_sync(
            self,
            service_account_id: str,
            _page_size: Optional[int] = None,
    ) -> List[ServiceAccountKeyData]:
        return self._key_service_cli.list_svc_acct_keys_sync(
            service_account_id=service_account_id, _page_size=_page_size,
        )

    def get_svc_acct_key_sync(self, key_id: str) -> ServiceAccountKeyData:
        return self._key_service_cli.get_svc_acct_key_sync(key_id=key_id)

    def delete_svc_acct_key_sync(self, key_id: str) -> None:
        return self._key_service_cli.delete_svc_acct_key_sync(key_id=key_id)

    def list_svc_acct_role_ids_on_folder_sync(
            self,
            svc_acct_id: str,
            folder_id: str,
            acct_type: str = 'serviceAccount',
            _page_size: Optional[int] = None,
    ) -> List[str]:
        with grpc_exc_handler("sa_list_roles"):
            return self._folder_service_cli.list_svc_acct_role_ids_on_folder_sync(
                svc_acct_id=svc_acct_id, folder_id=folder_id, acct_type=acct_type, _page_size=_page_size,
            )

    def modify_folder_access_bindings_for_svc_acct_sync(
            self,
            svc_acct_id: str,
            folder_id: str,
            role_ids: Sequence[str],
            action: model.AccessBindingAction,
            acct_type: str = "serviceAccount",
    ) -> None:
        initial_op = self._folder_service_cli.modify_folder_access_bindings_for_svc_acct_sync(
            svc_acct_id=svc_acct_id, folder_id=folder_id, role_ids=role_ids,
            action=action, acct_type=acct_type,
        )

        try:
            done_op = self._rm_operation_service.wait_for_operation_sync(
                initial_operation=initial_op,
                poll_interval=self._operation_poll_delay,
                timeout=self._operation_poll_timeout,
            )
        except (YCOperationAwaitTimeout, YCOperationFailed):
            # TODO FIX: BI-2161 wrap in more high-level exceptions
            raise
        else:
            assert done_op.done is True
