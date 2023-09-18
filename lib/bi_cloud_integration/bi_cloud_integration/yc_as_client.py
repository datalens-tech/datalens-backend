from __future__ import annotations

import threading
import logging
from typing import Any, Callable, Optional, Sequence

from yandex.cloud.priv.accessservice.v2 import access_service_pb2, access_service_pb2_grpc, resource_pb2

from dl_utils.aio import await_sync
from bi_cloud_integration.exc import grpc_exc_handler
from bi_cloud_integration.model import IAMServiceAccount, IAMUserAccount, IAMAccount, IAMResource
from bi_cloud_integration.yc_client_base import DLYCSingleServiceClient


LOGGER = logging.getLogger(__name__)


class DLASClient(DLYCSingleServiceClient):
    """ DataLens-specific client for YC Access Service """

    service_cls = access_service_pb2_grpc.AccessServiceStub

    @classmethod
    def _convert_grpc_subject_to_iam_account(cls, grpc_subject: Any) -> IAMAccount:
        grpc_subject_type = grpc_subject.WhichOneof("type")
        if grpc_subject_type == "user_account":
            return IAMUserAccount(
                id=grpc_subject.user_account.id,
                federation_id=grpc_subject.user_account.federation_id,
            )
        if grpc_subject_type == "service_account":
            return IAMServiceAccount(
                id=grpc_subject.service_account.id,
                folder_id=grpc_subject.service_account.folder_id,
            )
        raise ValueError("Unknown GRPC Subject type: {}.".format(grpc_subject_type))

    def _handle_grpc_error(self, err: Any) -> None:
        """ Disable the handle-by-default here, to specify the `operation_code` explicitly """
        pass

    async def authenticate(
            self,
            iam_token: Optional[str] = None, iam_cookie: Optional[str] = None,
            signature: Optional[str] = None, api_key: Optional[str] = None,
            request_id: Optional[str] = None
    ) -> IAMAccount:
        """
        Partial copypaste of
        https://a.yandex-team.ru/svn/trunk/arcadia/cloud/iam/accessservice/client/iam-access-service-client-python/v1/yc_as_client_v2/client.py?rev=r9379675#L76
        """
        _number_of_identity_params = sum(1 for x in (iam_token, iam_cookie, signature, api_key) if x is not None)
        if _number_of_identity_params != 1:
            raise ValueError(
                "Exactly one of `iam_token`, `iam_cookie`, `signature`, `api_key` must be specified."
            )

        if iam_token is not None:
            request = access_service_pb2.AuthenticateRequest(
                iam_token=iam_token,
            )
        elif iam_cookie is not None:
            request = access_service_pb2.AuthenticateRequest(
                iam_cookie=iam_cookie,
            )
        elif signature is not None:
            request = access_service_pb2.AuthenticateRequest(
                signature=signature._to_grpc_message(),  # type: ignore
            )
        else:
            request = access_service_pb2.AuthenticateRequest(
                api_key=api_key,
            )

        with grpc_exc_handler():
            response = await self.service.Authenticate.aio(
                request,
                metadata=(
                    (("x-request-id", request_id),)
                    if request_id is not None else ()),
            )

        return self._convert_grpc_subject_to_iam_account(response.subject)

    def authenticate_sync(
            self,
            iam_token: Optional[str] = None, iam_cookie: Optional[str] = None,
            signature: Optional[str] = None, api_key: Optional[str] = None,
            request_id: Optional[str] = None
    ) -> IAMAccount:
        return await_sync(self.authenticate(
            iam_token=iam_token, iam_cookie=iam_cookie,
            signature=signature, api_key=api_key,
            request_id=request_id,
        ))

    async def authorize(
            self,
            permission: str, resource_path: Sequence[IAMResource],
            iam_token: str,
            request_id: Optional[str] = None,
    ) -> None:
        assert len(resource_path) > 0, "At least one resource is required."

        LOGGER.debug('Checking permission %s on %r', permission, resource_path)
        request = access_service_pb2.AuthorizeRequest(
            iam_token=iam_token,
            permission=permission,
            resource_path=[
                resource_pb2.Resource(
                    id=iam_resource.id,
                    type=iam_resource.type,
                ) for iam_resource in resource_path
            ],
        )

        with grpc_exc_handler():
            await self.service.Authorize.aio(
                request,
                metadata=(
                    (("x-request-id", request_id),)
                    if request_id is not None else ()),
            )

    def authorize_sync(
            self,
            permission: str, resource_path: Sequence[IAMResource],
            iam_token: str,
            request_id: Optional[str] = None,
    ) -> None:
        return await_sync(self.authorize(
            permission=permission, resource_path=resource_path,
            iam_token=iam_token,
            request_id=request_id,
        ))


class DLYCASCLIHolder(threading.local):
    yc_as_cli: Optional[DLASClient] = None

    def __init__(self) -> None:
        # Called on each use in a new thread.
        self.lock = threading.Lock()

    def __call__(self, factory: Callable[[], DLASClient]) -> DLASClient:
        """
        WARNING: does not guarantee correctness with different factory results.
        """
        result = self.yc_as_cli
        if result is not None:
            return result
        with self.lock:
            result = self.yc_as_cli
            if result is not None:
                return result
            result = factory()
            self.yc_as_cli = result
        return result
