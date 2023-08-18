import abc
import asyncio
import logging
from typing import Optional, Sequence, Dict, Union, overload, Literal, Callable

import attr
import jwt
import grpc

from bi_cloud_integration.exc import YCBadRequest, YCUnauthenticated, YCPermissionDenied
from bi_cloud_integration.model import IAMAccount, IAMUserAccount, IAMServiceAccount, IAMResource
from bi_cloud_integration.yc_as_client import DLASClient
from bi_cloud_integration.yc_ss_client import DLSSClient
from bi_constants.api_constants import DLHeadersCommon, DLHeaders, YcTokenHeaderMode
from bi_api_commons.access_control_common import AuthFailureError
from bi_api_commons.error_messages import UserErrorMessages
from bi_api_commons.logging import RequestLoggingContextController
from bi_api_commons.yc_access_control_model import (
    AuthorizationMode,
    AuthorizationModeYandexCloud,
    AuthorizationModeDataCloud,
)
from bi_api_commons.base_models import IAMAuthData, RequestContextInfo, EmbedAuthData, TenantCommon
from bi_api_commons.base_models import TenantYCFolder, TenantYCOrganization, TenantDCProject
from bi_api_commons.base_models import TenantDef
from bi_app_tools.profiling_base import generic_profiler_async
from bi_utils.aio import await_sync

LOGGER = logging.getLogger(__name__)


class _MissingHeader(Exception):
    pass


class YCContext:
    pass


@attr.s(frozen=True, auto_attribs=True)
class YCAuthContext(YCContext):
    iam_account: IAMAccount
    auth_data: IAMAuthData
    # For some DC cases (e.g. public API) tenant should be defined based on request body. So now it is optional.
    tenant: Optional[TenantDef]


@attr.s(frozen=True, auto_attribs=True)
class YCEmbedContext(YCContext):
    embed_id: str
    auth_data: EmbedAuthData
    tenant: Optional[TenantDef]


@attr.s
class YCBaseController(metaclass=abc.ABCMeta):
    _authorization_mode: AuthorizationMode = attr.ib()

    _request_headers: Dict[str, str] = attr.ib(repr=False)
    _request_id: str = attr.ib()  # Assumed that here will be full current request ID

    def __attrs_post_init__(self) -> None:
        self._request_headers = {
            name.lower(): value
            for name, value in self._request_headers.items()
        }

    @staticmethod
    def _normalize_header_name(name: Union[str, DLHeaders]) -> str:
        return (name.value if isinstance(name, DLHeaders) else name).lower()

    @overload
    def get_request_header(self, name: Union[str, DLHeaders], required: Literal[True]) -> str:
        pass

    @overload
    def get_request_header(self, name: Union[str, DLHeaders], required: Literal[False]) -> Optional[str]:
        pass

    def get_request_header(self, name: Union[str, DLHeaders], required: bool = False) -> Optional[str]:
        secret_header_value = self._request_headers.get(self._normalize_header_name(name))
        if required and secret_header_value is None:
            raise _MissingHeader(name)
        return secret_header_value

    def has_request_header(self, name: Union[str, DLHeaders]) -> bool:
        return self._normalize_header_name(name) in self._request_headers

    @abc.abstractmethod
    async def perform_base_security_checks(self) -> YCContext:
        raise NotImplementedError()


@attr.s
class YCAccessController(YCBaseController):
    """
    Light-weight web-framework-agnostic facade that implements logic of base authentication/authorization
     of HTTP-request in YC/DataCloud environments.
    Assumed that this class will be instantiated per-request.
     It also will be useful to authorize some additional actions in views.
    This class is not responsible for gRPC clients/channels lifecycle management,
     web-framework middleware should care about it.
    Typical lifecycle of this object is:
     * Instantiate within middleware
     * Call perform_base_security_checks[_sync].
        If user authenticated & authorized for base object YCAuthContext will be returned & set inside for further
        auxiliary authorization checks
     * Set instance to request context/unwrap resolved YCAuthContext to request/logging context
     * May be call perform_auxiliary_authorization[_sync]
    """
    _as_client: DLASClient = attr.ib()
    # Expected that DLSSClient will be with appropriate IAM-token
    _ss_client: Optional[DLSSClient] = attr.ib()

    _cookie_auth_enabled: bool = attr.ib(default=False)
    _yc_token_header_mode: YcTokenHeaderMode = attr.ib(default=YcTokenHeaderMode.INTERNAL)
    _send_inbound_req_id_to_access_service: bool = attr.ib(default=False)
    _req_logging_ctx_ctrl: Optional[RequestLoggingContextController] = attr.ib(default=None)
    _skip_tenant_resolution: bool = attr.ib(default=False)

    # Internals
    _request_id_normalized_for_iam: str = attr.ib(init=False, default=None)
    _auth_ctx_resolution_lock: asyncio.Lock = attr.ib(init=False, factory=asyncio.Lock)
    _resolved_auth_ctx: Optional[YCAuthContext] = attr.ib(init=False, default=None)

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()

        initial_ss_client = self._ss_client

        req_id_normalized_for_iam = DLSSClient.normalize_request_id(self._request_id) or ''
        self._request_id_normalized_for_iam = req_id_normalized_for_iam

        if initial_ss_client and initial_ss_client.request_id != req_id_normalized_for_iam:
            self._ss_client = initial_ss_client.clone(request_id=req_id_normalized_for_iam)
        LOGGER.debug('YC SS request_id: %r', req_id_normalized_for_iam)

    def _get_request_id_for_access_service(self) -> str:
        # Access-service is tolerant to any format of request
        if self._send_inbound_req_id_to_access_service:
            # Preserving previous behaviour of yc_auth for Flask (maybe it is not desired)
            req_id_from_headers = self.get_request_header("x-request-id", required=False)
            return req_id_from_headers if req_id_from_headers is not None else self._request_id
        return self._request_id

    @generic_profiler_async("yc-auth-aio-check-cookie")  # type: ignore  # TODO: fix
    async def _resolve_iam_token_by_cookie(self, secret_cookie_header_value: str, host_header_value: str) -> str:
        ss_client = self._ss_client
        assert ss_client is not None, "Can not perform cookie auth. Session service client is not provided."
        try:
            ss_resp = await ss_client.check(cookie_header=secret_cookie_header_value, host=host_header_value)
        except (YCBadRequest, YCUnauthenticated) as err:
            msg = err.info.internal_details or ''
            code = 401 if isinstance(err, YCUnauthenticated) or msg.lower().startswith('unauthenticated') else None
            raise AuthFailureError(msg, response_code=code)
        except grpc.RpcError as err:
            details = err.details()  # type: ignore
            raise AuthFailureError(details)

        token = ss_resp.iam_token.iam_token
        # subject_id = ss_resp.subject_claims.sub
        # subject_title = ss_resp.subject_claims.name
        # subject_email = ss_resp.subject_claims.email
        # subject_federation = ss_resp.subject_claims.federation.id

        return token

    def _get_iam_token_from_header(self) -> Optional[str]:
        def get_internal_yc_token() -> Optional[str]:
            return self.get_request_header(DLHeadersCommon.IAM_TOKEN, required=False)

        def get_external_yc_token() -> Optional[str]:
            secret_header_value = self.get_request_header(DLHeadersCommon.AUTHORIZATION_TOKEN, required=False)
            if secret_header_value is None:
                return None

            header_value_prefix = "Bearer "
            if secret_header_value.startswith(header_value_prefix):
                return secret_header_value.removeprefix(header_value_prefix)
            else:
                raise AuthFailureError(
                    internal_message=f"{DLHeadersCommon.AUTHORIZATION_TOKEN!r} header does not starts with 'Bearer '",
                    user_message=f"Invalid {DLHeadersCommon.AUTHORIZATION_TOKEN.value!r} header format",
                    response_code=403,
                )

        map_mode_to_token_extractor_order: Dict[YcTokenHeaderMode, Sequence[Callable[[], Optional[str]]]] = {
            YcTokenHeaderMode.EXTERNAL: (get_external_yc_token,),
            YcTokenHeaderMode.INTERNAL: (get_internal_yc_token,),
            YcTokenHeaderMode.UNIVERSAL: (get_external_yc_token, get_internal_yc_token,),
        }
        extractor_order = map_mode_to_token_extractor_order[self._yc_token_header_mode]

        for extractor in extractor_order:
            secret_iam_token = extractor()
            if secret_iam_token is not None:
                return secret_iam_token

        return None

    async def _resolve_iam_token(self) -> str:
        secret_iam_token_from_headers = self._get_iam_token_from_header()
        if secret_iam_token_from_headers is not None:
            return secret_iam_token_from_headers

        # ENABLE_COOKIES_AUTH is `True` only in bi-converter - to *directly* process
        # heavy data file upload requests to upload.datalens.yandex.ru/api/v1/upload
        # (without frontend gateway).
        if self._cookie_auth_enabled and self.has_request_header("cookie"):
            try:
                secret_cookies_header_value = self.get_request_header("cookie", required=True)
            except _MissingHeader:
                raise AuthFailureError(
                    "No cookie provided",
                    user_message=UserErrorMessages.no_authentication_data_provided.value,
                    response_code=401,
                )
            try:
                host_header_value = self.get_request_header("host", required=True)
            except _MissingHeader:
                raise AuthFailureError(
                    "No host header provided",
                    user_message=UserErrorMessages.no_authentication_data_provided.value,
                    response_code=401,
                )
            return await self._resolve_iam_token_by_cookie(
                secret_cookie_header_value=secret_cookies_header_value,
                host_header_value=host_header_value
            )
        else:
            raise AuthFailureError(
                "No IAM token provided",
                user_message=UserErrorMessages.no_authentication_data_provided.value,
                response_code=401,
            )

    async def _resolve_tenant(self) -> TenantDef:
        """
        This method resolves tenant using HTTP request info and authorization mode
         * YC: folder ID / organization ID
         * DataCloud: project ID
        """
        auth_mode = self._authorization_mode

        if isinstance(auth_mode, AuthorizationModeYandexCloud):
            hdr_folder_id = self.get_request_header(DLHeadersCommon.FOLDER_ID, required=False)
            hdr_org_id = self.get_request_header(DLHeadersCommon.ORG_ID, required=False)
            hdr_dl_tenant_id = self.get_request_header(DLHeadersCommon.TENANT_ID, required=False)

            if hdr_dl_tenant_id is not None:
                if hdr_folder_id is not None or hdr_org_id is not None:
                    raise AuthFailureError(
                        internal_message=UserErrorMessages.org_or_folder_mixed_with_dl_tenant.value,
                        user_message=UserErrorMessages.org_or_folder_mixed_with_dl_tenant.value,
                        response_code=401,
                    )

                org_prefix = TenantYCOrganization.tenant_id_prefix

                if hdr_dl_tenant_id.startswith(org_prefix):
                    return TenantYCOrganization(
                        org_id=hdr_dl_tenant_id.removeprefix(org_prefix)
                    )

                else:
                    return TenantYCFolder(folder_id=hdr_dl_tenant_id)

            elif hdr_folder_id is not None and hdr_org_id is not None:
                raise AuthFailureError(
                    internal_message=UserErrorMessages.both_folder_and_org_specified.value,
                    user_message=UserErrorMessages.both_folder_and_org_specified.value,
                    response_code=401,
                )
            elif hdr_folder_id is not None:
                return TenantYCFolder(folder_id=hdr_folder_id)
            elif hdr_org_id is not None:
                return TenantYCOrganization(org_id=hdr_org_id)
            else:
                raise AuthFailureError(
                    internal_message=UserErrorMessages.no_tenant_specified.value,
                    user_message=UserErrorMessages.no_tenant_specified.value,
                    response_code=401,
                )

        elif isinstance(auth_mode, AuthorizationModeDataCloud):
            project_id = self.get_request_header(DLHeadersCommon.PROJECT_ID, required=False)
            if project_id is None:
                raise AuthFailureError(
                    internal_message=UserErrorMessages.no_dc_tenant_specified.value,
                    user_message=UserErrorMessages.no_dc_tenant_specified.value,
                    response_code=401,
                )

            return TenantDCProject(project_id=project_id)

        raise AssertionError(f"Unknown authorization mode: {auth_mode!r}", AuthorizationMode)

    @generic_profiler_async("yc-auth-aio-authenticate")  # type: ignore  # TODO: fix
    async def _validate_iam_token(
            self, iam_token: str
    ) -> IAMAccount:
        try:
            user = await self._as_client.authenticate(
                iam_token,
                request_id=self._get_request_id_for_access_service()
            )
        except YCBadRequest as err:
            LOGGER.info('yc_as BadRequestException: %r', err)
            msg = str(err)
            raise AuthFailureError(
                internal_message=msg,
                user_message=UserErrorMessages.user_unauthenticated.value,
                response_code=401,
            )
        except YCUnauthenticated as err:
            LOGGER.info('yc_as UnauthenticatedException: %r', err)
            msg = str(err)
            raise AuthFailureError(
                internal_message=msg,
                user_message=UserErrorMessages.user_unauthenticated.value,
                response_code=401,
            )
        else:
            assert isinstance(user, (IAMUserAccount, IAMServiceAccount))
            return user

    @generic_profiler_async("yc-auth-aio-authorize")  # type: ignore  # TODO: fix
    async def _authorize_for_tenant(self, iam_token: str, tenant: TenantDef) -> None:
        auth_mode = self._authorization_mode

        iam_resource_path: Sequence[IAMResource]
        permission_to_check: str

        if isinstance(auth_mode, AuthorizationModeYandexCloud):
            assert isinstance(tenant, (TenantYCFolder, TenantYCOrganization))
            if isinstance(tenant, TenantYCFolder):
                iam_resource_path = [IAMResource.folder(tenant.folder_id)]
                permission_to_check = auth_mode.folder_permission_to_check
            elif isinstance(tenant, TenantYCOrganization):
                iam_resource_path = [IAMResource.organization(tenant.org_id)]
                permission_to_check = auth_mode.organization_permission_to_check
        elif isinstance(auth_mode, AuthorizationModeDataCloud):
            assert isinstance(tenant, TenantDCProject)
            # as in https://nda.ya.ru/t/0hsM703U4uXzfk
            iam_resource_path = [IAMResource.cloud(tenant.project_id)]
            permission_to_check = auth_mode.project_permission_to_check
        else:
            raise AssertionError(
                f"Unknown type of authorization mode: {type(auth_mode)}",
                auth_mode,
            )

        try:
            await self._as_client.authorize(
                iam_token=iam_token,
                # 'datalens.instance.use', normally:
                permission=permission_to_check,
                resource_path=iam_resource_path,
                request_id=self._get_request_id_for_access_service(),
            )
        except YCBadRequest as err:
            LOGGER.info('yc_as authorize: BadRequestException: %r', err)
            msg = str(err)
            raise AuthFailureError(
                internal_message=msg,
                user_message=UserErrorMessages.user_unauthorized.value,
                response_code=403,
            )
        except YCPermissionDenied as err:
            LOGGER.info('yc_as authorize: PermissionDeniedException: %r', err)
            msg = str(err)
            raise AuthFailureError(
                internal_message=msg,
                user_message=UserErrorMessages.user_unauthorized.value,
                response_code=403,
            )

    # TODO CONSIDER: BI-2717 May be fill each key as soon as we resolve it?
    def _update_logging_ctx(self, auth_ctx: YCAuthContext) -> None:
        log_ctx_controller = self._req_logging_ctx_ctrl
        if log_ctx_controller is None:
            return

        tenant = auth_ctx.tenant
        user_id: str = auth_ctx.iam_account.id
        user_name: Optional[str] = None

        if isinstance(tenant, TenantYCFolder):
            log_ctx_controller.put_to_context("folder_id", tenant.folder_id)
        elif isinstance(tenant, TenantYCOrganization):
            log_ctx_controller.put_to_context("org_id", tenant.org_id)
        elif isinstance(tenant, TenantDCProject):
            log_ctx_controller.put_to_context("project_id", tenant.project_id)
        elif tenant is None:
            assert self._skip_tenant_resolution, "'skip_tenant_resolution' flag is not set but tenant was not resolved."
        else:
            raise AssertionError(f"Can not set tenant key to logging context. Unexpected type of tenant: {tenant!r}")

        log_ctx_controller.put_to_context("user_id", user_id)
        log_ctx_controller.put_to_context("user_name", user_name)

    @generic_profiler_async("yc-auth-aio-full")  # type: ignore  # TODO: fix
    async def perform_base_security_checks(self) -> YCAuthContext:
        async with self._auth_ctx_resolution_lock:
            if self._resolved_auth_ctx is not None:
                raise AssertionError(f"Auth context already resolved for request {self._request_id!r}")

            iam_token = await self._resolve_iam_token()
            iam_account = await self._validate_iam_token(iam_token)

            tenant: Optional[TenantDef] = None

            if not self._skip_tenant_resolution:
                tenant = await self._resolve_tenant()
                tenant = self._authorization_mode.ensure_tenant(
                    tenant
                )
                # MyPy can not figure out that tenant is not None here
                assert tenant is not None
                await self._authorize_for_tenant(iam_token, tenant)

            auth_ctx = YCAuthContext(
                iam_account=iam_account,
                auth_data=IAMAuthData(iam_token),
                tenant=tenant,
            )
            self._resolved_auth_ctx = auth_ctx
            self._update_logging_ctx(auth_ctx)

        return auth_ctx

    def perform_base_security_checks_sync(self) -> YCAuthContext:
        return await_sync(self.perform_base_security_checks())

    @generic_profiler_async("yc-auth-aio-aux-authorize")  # type: ignore  # TODO: fix
    async def perform_auxiliary_authorization(self, permission: str, resource_path: Sequence[IAMResource]) -> None:
        auth_context = self._resolved_auth_ctx
        assert auth_context is not None, (
            f"Can not perform authorization for {resource_path!r}."
            f" Auth context was not resolved for request {self._request_id!r}"
        )
        try:
            await self._as_client.authorize(
                permission=permission,
                resource_path=resource_path,
                iam_token=auth_context.auth_data.iam_token,
                request_id=self._get_request_id_for_access_service(),
            )
        except (YCBadRequest, YCPermissionDenied) as err:
            LOGGER.info('yc_as auxiliary authorize: %r %r %r', permission, resource_path, err)
            raise AuthFailureError(
                internal_message=str(err),
            )

    async def perform_auxiliary_authorization_sync(self, permission: str, resource_path: Sequence[IAMResource]) -> None:
        return await_sync(self.perform_auxiliary_authorization(permission, resource_path))

    @classmethod
    def update_rci_with_auth_ctx(cls, rci: RequestContextInfo, auth_ctx: YCAuthContext) -> RequestContextInfo:
        return rci.clone(
            user_id=auth_ctx.iam_account.id,
            auth_data=auth_ctx.auth_data,
            tenant=auth_ctx.tenant,
        )


@attr.s
class YCEmbedAccessController(YCBaseController):
    _req_logging_ctx_ctrl: Optional[RequestLoggingContextController] = attr.ib(default=None)
    # Internals
    _resolved_auth_ctx: Optional[YCEmbedContext] = attr.ib(init=False, default=None)

    def _get_embed_token_from_header(self) -> Optional[str]:
        return self.get_request_header(DLHeadersCommon.EMBED_TOKEN, required=True)

    def _resolve_tenant(self) -> TenantDef:
        auth_mode = self._authorization_mode

        if isinstance(auth_mode, AuthorizationModeDataCloud):
            project_id = self.get_request_header(DLHeadersCommon.PROJECT_ID, required=False)
            if project_id is not None:
                return TenantDCProject(project_id=project_id)
        elif isinstance(auth_mode, AuthorizationModeYandexCloud):
            hdr_folder_id = self.get_request_header(DLHeadersCommon.FOLDER_ID, required=False)
            hdr_org_id = self.get_request_header(DLHeadersCommon.ORG_ID, required=False)
            hdr_dl_tenant_id = self.get_request_header(DLHeadersCommon.TENANT_ID, required=False)

            if hdr_dl_tenant_id is not None:
                if hdr_folder_id is not None or hdr_org_id is not None:
                    return TenantCommon()

                org_prefix = TenantYCOrganization.tenant_id_prefix

                if hdr_dl_tenant_id.startswith(org_prefix):
                    return TenantYCOrganization(org_id=hdr_dl_tenant_id.removeprefix(org_prefix))
                else:
                    return TenantYCFolder(folder_id=hdr_dl_tenant_id)

            elif hdr_folder_id is not None:
                return TenantYCFolder(folder_id=hdr_folder_id)
            elif hdr_org_id is not None:
                return TenantYCOrganization(org_id=hdr_org_id)

        return TenantCommon()

    def _update_logging_ctx(self, auth_ctx: YCEmbedContext) -> None:
        log_ctx_controller = self._req_logging_ctx_ctrl
        if log_ctx_controller is None:
            return

        tenant = auth_ctx.tenant
        user_id: str = auth_ctx.embed_id

        if isinstance(tenant, TenantDCProject):
            log_ctx_controller.put_to_context("project_id", tenant.project_id)
        if isinstance(tenant, TenantYCFolder):
            log_ctx_controller.put_to_context("folder_id", tenant.folder_id)
        elif isinstance(tenant, TenantYCOrganization):
            log_ctx_controller.put_to_context("org_id", tenant.org_id)

        log_ctx_controller.put_to_context("user_id", user_id)

    @staticmethod
    def _drop_prefix(token: str) -> str:
        known_prefix_list = [
            "dl_ru_token_",
            "dl_dc_token_",
            "dl_il_token_",
        ]
        for prefix in known_prefix_list:
            if token.startswith(prefix):
                return token[len(prefix):]
        return token

    @generic_profiler_async("yc-auth-aio-full")  # type: ignore  # TODO: fix
    async def perform_base_security_checks(self) -> YCEmbedContext:
        if self._resolved_auth_ctx is not None:
            raise AssertionError(f"Auth context already resolved for request {self._request_id!r}")

        embed_token = self._get_embed_token_from_header()
        if embed_token is None:
            raise AuthFailureError(
                "No embed token provided",
                user_message=UserErrorMessages.no_authentication_data_provided.value,
                response_code=401,
            )
        tenant = self._resolve_tenant()

        jwt_token = self._drop_prefix(embed_token)
        try:
            jwt_payload = jwt.decode(jwt_token, options={'verify_signature': False})
        except Exception:  # noqa
            raise AuthFailureError(
                "Failed to decode jwt token",
                user_message=UserErrorMessages.no_authentication_data_provided.value,
                response_code=401,
            )

        if embed_token is None:
            raise AuthFailureError(
                "No embed token provided",
                user_message=UserErrorMessages.no_authentication_data_provided.value,
                response_code=401,
            )
        embed_id = jwt_payload.get('embedId')

        if embed_id is None:
            raise AuthFailureError(
                "Embed id not found in jwt token",
                user_message=UserErrorMessages.no_authentication_data_provided.value,
                response_code=401,
            )

        auth_ctx = YCEmbedContext(
            embed_id=embed_id,
            auth_data=EmbedAuthData(embed_token),
            tenant=tenant,
        )
        self._resolved_auth_ctx = auth_ctx
        self._update_logging_ctx(auth_ctx)

        return auth_ctx

    @classmethod
    def update_rci_with_auth_ctx(cls, rci: RequestContextInfo, auth_ctx: YCEmbedContext) -> RequestContextInfo:
        return rci.clone(
            user_id=auth_ctx.embed_id,
            auth_data=auth_ctx.auth_data,
            tenant=auth_ctx.tenant,
        )
