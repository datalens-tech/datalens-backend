from __future__ import annotations

from concurrent import futures
import functools
from http import cookies
import threading
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)
import uuid

import attr
import google.protobuf.any_pb2
import grpc
import jwt
import shortuuid
from yandex.cloud.priv.accessservice.v2 import (
    access_service_pb2,
    access_service_pb2_grpc,
    resource_pb2,
)
from yandex.cloud.priv.iam.v1 import (
    iam_token_service_pb2,
    iam_token_service_pb2_grpc,
    key_pb2,
    key_service_pb2_grpc,
    service_account_service_pb2_grpc,
)
from yandex.cloud.priv.iam.v1.key_service_pb2 import (
    CreateKeyRequest,
    CreateKeyResponse,
    DeleteKeyRequest,
)
from yandex.cloud.priv.iam.v1.service_account_pb2 import ServiceAccount
from yandex.cloud.priv.iam.v1.service_account_service_pb2 import (
    CreateServiceAccountRequest,
    DeleteServiceAccountRequest,
)
from yandex.cloud.priv.iam.v1.token import iam_token_pb2
from yandex.cloud.priv.oauth.v1 import (
    session_service_pb2,
    session_service_pb2_grpc,
)
from yandex.cloud.priv.operation.operation_pb2 import Operation
from yandex.cloud.priv.resourcemanager.v1 import (
    folder_pb2,
    folder_service_pb2,
    folder_service_pb2_grpc,
    operation_service_pb2_grpc,
)

from bi_cloud_integration.yc_client_base import DLYCServiceConfig

FAKE_SERVICE_LOCAL_METADATA_IAM_TOKEN = "_fake_service_local_metadata_token_"
FAKE_FOLDER_ID = "fake_test_folder_id"
CLOUD_PREPROD_FOLDER_ID = "aoevv1b69su5144mlro3"
FOLDER_TO_CLOUD_IDS = {
    FAKE_FOLDER_ID: "fake_test_cloud_id",
    CLOUD_PREPROD_FOLDER_ID: "aoee4gvsepbo0ah4i2j6",
}


@attr.s(auto_attribs=True, frozen=True, hash=True)
class IAMMockResource:
    r_type: str
    id: str

    @classmethod
    def folder(cls, folder_id: str) -> IAMMockResource:
        return cls(r_type="resource-manager.folder", id=folder_id)

    @classmethod
    def cloud(cls, cloud_id: str) -> IAMMockResource:
        return cls(r_type="resource-manager.cloud", id=cloud_id)

    @classmethod
    def billing_account(cls, billing_account_id: str) -> IAMMockResource:
        return cls(r_type="billing.account", id=billing_account_id)

    @classmethod
    def service_account(cls, service_account_id: str) -> IAMMockResource:
        return cls(r_type="iam.serviceAccount", id=service_account_id)

    @classmethod
    def from_grpc(cls, msg: resource_pb2.Resource) -> IAMMockResource:
        return cls(r_type=msg.type, id=msg.id)

    def to_grpc(self) -> resource_pb2.Resource:
        return resource_pb2.Resource(
            type=self.r_type,
            id=self.id,
        )


@attr.s(auto_attribs=True, frozen=True)
class IAMMockUser:
    id: str
    iam_tokens: List[str] = attr.ib(factory=list)
    cookies: List[str] = attr.ib(factory=list)
    map_resource_path_permissions: Dict[Tuple[IAMMockResource, ...], FrozenSet[str]] = attr.ib(factory=dict)
    is_service_account: bool = False
    service_account_folder_id: str = FAKE_FOLDER_ID

    def clone(self) -> IAMMockUser:
        return attr.evolve(
            self,
            iam_tokens=list(self.iam_tokens),
            cookies=list(self.cookies),
            map_resource_path_permissions=dict(self.map_resource_path_permissions),
        )

    def to_grpc_subject(self) -> access_service_pb2.Subject:
        if self.is_service_account:
            return access_service_pb2.Subject(
                service_account=access_service_pb2.Subject.ServiceAccount(
                    id=self.id,
                    folder_id=self.service_account_folder_id,
                )
            )
        else:
            return access_service_pb2.Subject(
                user_account=access_service_pb2.Subject.UserAccount(
                    id=self.id,
                    federation_id="the-mock-federation.yc",
                )
            )

    @staticmethod
    def yc_session_to_cookie_header(yc_session_val: str) -> str:
        c = cookies.SimpleCookie()
        c["yc_session"] = yc_session_val
        return c.output(header="").strip()

    @staticmethod
    def normalize_permission_entry(
        resource_path: Union[IAMMockResource, Iterable[IAMMockResource]],
        permissions: Union[str, Iterable[str]],
    ) -> Tuple[Tuple[IAMMockResource, ...], FrozenSet[str]]:
        return (
            ((resource_path,) if isinstance(resource_path, IAMMockResource) else tuple(resource_path)),
            frozenset({permissions}) if isinstance(permissions, str) else frozenset(permissions),
        )

    def with_permissions(
        self,
        resource_path: Union[IAMMockResource, Iterable[IAMMockResource]],
        permissions: Union[str, Iterable[str]],
    ) -> IAMMockUser:
        actual_rp, actual_perms_set = self.normalize_permission_entry(resource_path, permissions)
        assert actual_rp not in self.map_resource_path_permissions
        return attr.evolve(
            self, map_resource_path_permissions={**self.map_resource_path_permissions, actual_rp: actual_perms_set}
        )

    def get_single_iam_token(self) -> str:
        if len(self.iam_tokens) == 1:
            return self.iam_tokens[0]
        raise AssertionError(f"User {self.id!r} tokens count {len(self.iam_tokens)} != 1")

    def get_single_yc_cookie(self) -> str:
        if len(self.cookies) == 1:
            return self.cookies[0]
        raise AssertionError(f"User {self.id!r} cookies count {len(self.cookies)} != 1")


@attr.s(auto_attribs=True, frozen=True, hash=True)
class GRPCReqResp:
    name: str
    request: Any
    context: grpc.ServicerContext
    result: Any = None
    exception: Optional[BaseException] = None


@attr.s
class AuthConfigHolder:
    _users: List[IAMMockUser] = attr.ib(init=False, factory=list)
    # Convenience classvars for tests
    User: ClassVar[Type[IAMMockUser]] = IAMMockUser
    Resource: ClassVar[Type[IAMMockResource]] = IAMMockResource

    request_log: List[GRPCReqResp] = attr.ib(init=False, factory=list)
    request_log_lock: threading.Lock = attr.ib(init=False, factory=threading.Lock)
    request_log_max_size = 10

    def log_a_request(self, reqresp: GRPCReqResp) -> None:
        self.request_log.append(reqresp)
        if len(self.request_log) > self.request_log_max_size:
            self.request_log.pop(0)

    def _get_all_iam_tokens(self) -> Set[str]:
        ret: Set[str] = set()
        for user in self._users:
            ret.update(user.iam_tokens)
        return ret

    def add_users(self, obj: Union[IAMMockUser, Iterable[IAMMockUser]]) -> None:
        users_to_add = [obj] if isinstance(obj, IAMMockUser) else obj
        all_iam_tokens = self._get_all_iam_tokens()
        for new_user in users_to_add:
            user_tokens = set(new_user.iam_tokens)
            token_cross_section = user_tokens & all_iam_tokens
            if token_cross_section:
                raise ValueError(f"Duplicated IAM tokens for new IAMMockUser {new_user.id}: {token_cross_section!r}")
        self._users.extend(users_to_add)

    def make_new_user(self, *permissions) -> IAMMockUser:
        idx = len(self._users)
        user = IAMMockUser(
            id=f"fake_auto_user_{idx}",
            iam_tokens=[f"fake_iam_token_{idx}"],
            cookies=[f"fake_user_cookie_{idx}"],
        )
        for r_path, perm in permissions:
            user = user.with_permissions(r_path, perm)
        self.add_users((user,))
        return user

    def find_user_by_id(self, user_id: str) -> Optional[IAMMockUser]:
        return next((u for u in self._users if u.id == user_id), None)

    def add_iam_token_to_user(self, user: Union[str, IAMMockUser], iam_token: str) -> None:
        all_iam_tokens = self._get_all_iam_tokens()
        user_id = user if isinstance(user, str) else user.id

        if iam_token in all_iam_tokens:
            raise ValueError(f"Duplicated IAM tokens for new IAMMockUser {user_id}: {iam_token!r}")

        local_user = self.find_user_by_id(user_id)
        if local_user is None:
            raise ValueError(f"Can not fount user ID to add IAM token: {user_id}")
        local_user.iam_tokens.append(iam_token)

    def find_user_by_iam_token(self, iam_token: str) -> Optional[IAMMockUser]:
        user = next((u for u in self._users if iam_token in u.iam_tokens), None)
        return None if user is None else user.clone()

    def find_user_by_cookie(self, cookie: str) -> Optional[IAMMockUser]:
        user = next((u for u in self._users if cookie in u.cookies), None)
        return None if user is None else user.clone()

    @staticmethod
    def has_user_permission(user: IAMMockUser, rp: Sequence[IAMMockResource], permission: str) -> bool:
        return permission in user.map_resource_path_permissions.get(tuple(rp), ())


def with_request_log(func: Callable):
    @functools.wraps(func)
    def with_request_log_wrapped(
        self: Any,
        request: Any,
        context: grpc.ServicerContext,
        *args: Any,
        **kwargs: Any,
    ):
        auth_config_holder = getattr(self, "auth_config_holder", None)
        log_a_request = (
            auth_config_holder.log_a_request if isinstance(auth_config_holder, AuthConfigHolder) else lambda rr: None
        )
        name = self.__class__.__name__ + "." + func.__name__

        try:
            result = func(self, request, context, *args, **kwargs)
        except Exception as err:
            log_a_request(GRPCReqResp(name=name, request=request, context=context, exception=err))
            raise
        log_a_request(GRPCReqResp(name=name, request=request, context=context, result=result))
        return result

    return with_request_log_wrapped


@attr.s
class ServiceMockBase:
    auth_config_holder: AuthConfigHolder = attr.ib()


class AccessServiceMock(access_service_pb2_grpc.AccessServiceServicer, ServiceMockBase):
    @with_request_log
    def Authenticate(
        self,
        request: access_service_pb2.AuthenticateRequest,
        context: grpc.ServicerContext,
    ) -> access_service_pb2.AuthenticateResponse:
        # from celery.contrib import rdb; rdb.set_trace()
        iam_token = request.iam_token
        user = self.auth_config_holder.find_user_by_iam_token(iam_token)

        if user is None:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Bad IAM token")

        return access_service_pb2.AuthenticateResponse(
            subject=user.to_grpc_subject(),
        )

    @with_request_log
    def Authorize(
        self,
        request: access_service_pb2.AuthorizeRequest,
        context: grpc.ServicerContext,
    ) -> access_service_pb2.AuthorizeResponse:
        # from celery.contrib import rdb; rdb.set_trace()

        if not request.HasField("iam_token"):
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "Only IAM authorization implemented in mock")

        iam_token = request.iam_token
        user = self.auth_config_holder.find_user_by_iam_token(iam_token)

        if user is None:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Bad IAM token!")

        rp = [IAMMockResource.from_grpc(grpc_r) for grpc_r in request.resource_path]

        if self.auth_config_holder.has_user_permission(user, rp, request.permission):
            return access_service_pb2.AuthorizeResponse(
                subject=user.to_grpc_subject(), resource_path=[r.to_grpc() for r in rp]
            )

        context.abort(grpc.StatusCode.PERMISSION_DENIED, "Permission denied!")


@attr.s
class SessionServiceMock(session_service_pb2_grpc.SessionServiceServicer, ServiceMockBase):
    @with_request_log
    def Check(
        self,
        request: session_service_pb2.CheckSessionRequest,
        context: grpc.ServicerContext,
    ) -> session_service_pb2.CreateSessionResponse:
        parser_cookies = cookies.SimpleCookie()
        parser_cookies.load(request.cookie_header)
        try:
            yc_session = parser_cookies["yc_session"].value
        except KeyError:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "No yc_session in cookie")
            raise

        user = self.auth_config_holder.find_user_by_cookie(yc_session)
        if user is None:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "yc_session cookie is invalid")
            raise Exception()

        new_iam_token = f"gen-by-session-service-{shortuuid.uuid()}"
        self.auth_config_holder.add_iam_token_to_user(user, new_iam_token)

        return session_service_pb2.CheckSessionResponse(
            subject_claims=None,
            expires_at=None,
            cloud_user_info=None,
            iam_token=iam_token_pb2.IamToken(
                iam_token=new_iam_token,
                expires_at=None,
            ),
            passport_session=None,  # session_service_pb2.PassportSession(users=[])
        )


@attr.s
class IamTokenServiceMock(iam_token_service_pb2_grpc.IamTokenServiceServicer, ServiceMockBase):
    @with_request_log
    def Create(
        self,
        request: iam_token_service_pb2.CreateIamTokenRequest,
        context: grpc.ServicerContext,
    ) -> iam_token_service_pb2.CreateIamTokenResponse:
        assert request.jwt
        service_account_id = jwt.decode(request.jwt, options=dict(verify_signature=False))["iss"]
        user = self.auth_config_holder.find_user_by_id(service_account_id)
        iam_token = user.iam_tokens[0]
        return iam_token_service_pb2.CreateIamTokenResponse(iam_token=iam_token)

    @with_request_log
    def CreateForServiceAccount(
        self,
        request: iam_token_service_pb2.CreateIamTokenForServiceAccountRequest,
        context: grpc.ServicerContext,
    ) -> iam_token_service_pb2.CreateIamTokenResponse:
        headers = {obj.key: obj.value for obj in context.invocation_metadata()}
        auth = headers.get("authorization")
        if auth != "Bearer " + FAKE_SERVICE_LOCAL_METADATA_IAM_TOKEN:
            context.abort(
                grpc.StatusCode.UNAUTHENTICATED, f"Must have a mockupped service-local-metadata IAM token; {headers=!r}"
            )

        return iam_token_service_pb2.CreateIamTokenResponse(
            iam_token=f"fake_impersonation_iam_token_for_service_account_{request.service_account_id}",
        )


@attr.s
class FolderServiceMock(folder_service_pb2_grpc.FolderServiceServicer, ServiceMockBase):
    @with_request_log
    def Resolve(
        self,
        request: folder_service_pb2.ResolveFoldersRequest,
        context: grpc.ServicerContext,
    ) -> folder_service_pb2.ResolveFoldersResponse:
        if any([folder_id not in FOLDER_TO_CLOUD_IDS.keys() for folder_id in request.folder_ids]):
            context.abort(
                grpc.StatusCode.UNIMPLEMENTED,
                f"Only folder_id from ({', '.join(FOLDER_TO_CLOUD_IDS.keys())}) are allowed here",
            )

        return folder_service_pb2.ResolveFoldersResponse(
            resolved_folders=[
                folder_pb2.ResolvedFolder(cloud_id=FOLDER_TO_CLOUD_IDS[folder_id]) for folder_id in request.folder_ids
            ],
        )

    def UpdateAccessBindings(self, request, context):
        return Operation()


@attr.s
class ServiceAccountServiceMock(service_account_service_pb2_grpc.ServiceAccountServiceServicer, ServiceMockBase):
    @with_request_log
    def Create(self, request: CreateServiceAccountRequest, context: grpc.ServicerContext) -> Operation:
        sa = google.protobuf.any_pb2.Any()
        sa_id = str(uuid.uuid4())
        sa_name = request.name
        sa.Pack(ServiceAccount(id=sa_id, name=sa_name))
        return Operation(response=sa)

    @with_request_log
    def Delete(self, request: DeleteServiceAccountRequest, context: grpc.ServicerContext) -> Operation:
        return Operation(done=True)


@attr.s
class KeyServiceMock(key_service_pb2_grpc.KeyServiceServicer, ServiceMockBase):
    @with_request_log
    def Create(self, request: CreateKeyRequest, context: grpc.ServicerContext) -> CreateKeyResponse:
        key = key_pb2.Key(
            id=str(uuid.uuid4()), service_account_id=request.service_account_id, public_key="pub", key_algorithm=0
        )
        response = CreateKeyResponse(key=key, private_key="priv")
        return response

    @with_request_log
    def Delete(self, request: DeleteKeyRequest, context: grpc.ServicerContext) -> Operation:
        return Operation(done=True)


@attr.s
class OperationServiceMock(operation_service_pb2_grpc.OperationServiceServicer, ServiceMockBase):
    def Get(self, request, context):
        return Operation(done=True)


@attr.s
class IAMServicesMockFacade:
    data_holder: AuthConfigHolder = attr.ib()
    service_config: DLYCServiceConfig = attr.ib()

    @classmethod
    def create_threadpool_grpc_server(cls, auth_config_holder: AuthConfigHolder, max_workers: int = 3):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
        access_service_pb2_grpc.add_AccessServiceServicer_to_server(AccessServiceMock(auth_config_holder), server)
        session_service_pb2_grpc.add_SessionServiceServicer_to_server(SessionServiceMock(auth_config_holder), server)
        iam_token_service_pb2_grpc.add_IamTokenServiceServicer_to_server(
            IamTokenServiceMock(auth_config_holder), server
        )
        folder_service_pb2_grpc.add_FolderServiceServicer_to_server(FolderServiceMock(auth_config_holder), server)
        service_account_service_pb2_grpc.add_ServiceAccountServiceServicer_to_server(
            ServiceAccountServiceMock(auth_config_holder), server
        )
        key_service_pb2_grpc.add_KeyServiceServicer_to_server(KeyServiceMock(auth_config_holder), server)
        operation_service_pb2_grpc.add_OperationServiceServicer_to_server(
            OperationServiceMock(auth_config_holder), server
        )
        return server
