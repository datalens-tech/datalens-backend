from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional, Type

import attr
from aiohttp import web

from bi_api_commons.access_control_common import match_path_prefix, AuthFailureError
from bi_api_commons.yc_access_control import YCAccessController, YCAuthContext, YCEmbedContext, YCEmbedAccessController
from bi_api_commons.yc_access_control_model import AuthorizationMode
from bi_cloud_integration.yc_as_client import DLASClient, DLYCASCLIHolder
from bi_cloud_integration.yc_client_base import DLYCServiceConfig
from bi_cloud_integration.yc_ss_client import DLSSClient
from bi_constants.api_constants import YcTokenHeaderMode
from bi_api_commons.aiohttp import aiohttp_wrappers

if TYPE_CHECKING:
    from aiohttp.typedefs import Handler
    from bi_api_commons.aiohttp.aiohttp_wrappers import DLRequestBase

LOGGER = logging.getLogger(__name__)


@attr.s()
class YCAuthService:
    _authorization_mode: AuthorizationMode = attr.ib()
    _enable_cookie_auth: bool = attr.ib()
    _access_service_cfg: DLYCServiceConfig = attr.ib()
    _session_service_cfg: Optional[DLYCServiceConfig] = attr.ib(default=None)
    _allowed_folder_ids: Optional[set[str]] = attr.ib(default=None)
    _skip_path_list: tuple[str, ...] = attr.ib(default=())
    _yc_token_header_mode: YcTokenHeaderMode = attr.ib(default=YcTokenHeaderMode.INTERNAL)
    _ss_sa_key_data: Optional[dict[str, str]] = attr.ib(default=None, repr=False)
    _yc_ts_endpoint: Optional[str] = attr.ib(default=None)
    # Use with caution:
    #  - Authorization for tenant will not be performed
    #  - A lot of our code relies on resolved tenant in RCI
    #  At the moment of commit only external API requires to postpone tenant resolution.
    #  Reason: do not obligate user to send project ID in headers.
    _skip_tenant_resolution: bool = attr.ib(default=False)
    # Internals
    _yc_as_cli_holder: DLYCASCLIHolder = attr.ib(init=False, factory=DLYCASCLIHolder)
    _base_ss_client: Optional[DLSSClient] = attr.ib(init=False, default=None)

    def __attrs_post_init__(self) -> None:
        if self._enable_cookie_auth:
            assert self._session_service_cfg is not None, "To enable cookie auth session_service_cfg must be provided"
            if self._ss_sa_key_data is not None:
                assert self._yc_ts_endpoint is not None

    def should_skip_auth(self, requested_path: str) -> bool:
        return match_path_prefix(
            prefix_list=self._skip_path_list,
            path=requested_path,
        )

    def make_yc_as_cli(self) -> DLASClient:
        return DLASClient(service_config=self._access_service_cfg)

    @property
    def yc_as_cli(self) -> DLASClient:
        return self._yc_as_cli_holder(factory=self.make_yc_as_cli)

    async def _create_base_ss_client(self) -> DLSSClient:
        assert self._session_service_cfg is not None
        yc_ss_cli = DLSSClient(self._session_service_cfg)
        return yc_ss_cli

    async def get_ss_client_for_current_request(self) -> Optional[DLSSClient]:
        """
        Session service client stores IAM token of SA in it's fields.
        So we have to create new instance for each request to re-fetch SA IAM token
         from machine metadata due to it has a limited lifetime.
        Request ID will be actualized in YCAccessController
        """
        if self._base_ss_client is None:
            actual_client = await self._create_base_ss_client()
            self._base_ss_client = actual_client
        else:
            actual_client = self._base_ss_client

        # Note that this obtains a service user token from yc container metadata,
        # and it likely needs a special permission
        # ( `internal.oauth.client` for now, to be changed later: https://st.yandex-team.ru/CLOUD-41544 )
        # https://bb.yandex-team.ru/projects/CLOUD/repos/iam-sync-configs/commits/65879c374752922149efebf6d6bfd63200d0bf63
        # https://bb.yandex-team.ru/projects/CLOUD/repos/iam-sync-configs/pull-requests/1738/commits/c31956a22d1d094eef7204c3f925b35190bd1a52

        if self._ss_sa_key_data is not None:
            actual_client = await actual_client.ensure_fresh_token(
                sa_key_data=self._ss_sa_key_data,
                ts_endpoint=self._yc_ts_endpoint,
            )
        else:
            actual_client = await actual_client.ensure_fresh_token()

        return actual_client

    def ensure_allowed_folder_id(self, folder_id: str) -> None:
        allowed_folder_ids = self._allowed_folder_ids
        if allowed_folder_ids is None:
            return
        LOGGER.info("Folder ID check-up will be performed")
        if folder_id not in allowed_folder_ids:
            LOGGER.info(f"Folder ID check-up failed for {folder_id}")
            # TODO FIX: https://st.yandex-team.ru/BI-2708 move exception handling to common error-response formatter
            raise web.HTTPForbidden(reason="Not allowed for this folder ID")
        LOGGER.info("Folder ID check-up passed")

    async def process_request(
            self,
            app_request: DLRequestBase,
            update_ctx: bool = False,
    ) -> YCAuthContext:
        yc_as_cli = self.yc_as_cli
        request_id = app_request.temp_rci.request_id
        assert request_id is not None, "Request ID is not set in RCI"

        yc_as_cli = yc_as_cli.clone(request_id=request_id)

        ss_client: Optional[DLSSClient] = (
            await self.get_ss_client_for_current_request() if self._enable_cookie_auth
            else None
        )

        access_ctrl = YCAccessController(
            authorization_mode=self._authorization_mode,
            cookie_auth_enabled=self._enable_cookie_auth,
            as_client=yc_as_cli,
            ss_client=ss_client,
            yc_token_header_mode=self._yc_token_header_mode,
            request_headers=dict(app_request.request.headers),
            request_id=request_id,
            req_logging_ctx_ctrl=app_request.log_ctx_controller if update_ctx else None,
            skip_tenant_resolution=self._skip_tenant_resolution,
        )

        try:
            auth_ctx = await access_ctrl.perform_base_security_checks()
        except Exception as err:
            # TODO FIX: https://st.yandex-team.ru/BI-2708 move exception handling to common error-response formatter
            if isinstance(err, AuthFailureError) and err.response_code in (401, 403):
                exc_cls: Type[web.HTTPException] = {
                    401: web.HTTPUnauthorized,
                    403: web.HTTPForbidden,
                }[err.response_code]
                raise exc_cls(reason=err.user_message)
            else:
                LOGGER.exception("yc auth error: %r", err)
                raise

        if update_ctx:
            enriched_rci = access_ctrl.update_rci_with_auth_ctx(
                app_request.temp_rci,
                auth_ctx,
            )
            app_request.replace_temp_rci(enriched_rci)

        LOGGER.info("YC authentication and authorization successfully passed")
        return auth_ctx

    @web.middleware
    @aiohttp_wrappers.DLRequestBase.use_dl_request_on_method
    async def middleware(self, app_request: DLRequestBase, handler: Handler) -> web.StreamResponse:
        request_path = app_request.request.path
        if self.should_skip_auth(request_path):
            LOGGER.info(f"Skipping request auth because of configured skip prefix list. {request_path=!r}")
            return await handler(app_request.request)

        if aiohttp_wrappers.RequiredResourceCommon.SKIP_AUTH in app_request.required_resources:
            LOGGER.info("Auth was skipped due to SKIP_AUTH flag in target view")
            return await handler(app_request.request)

        await self.process_request(app_request, update_ctx=True)
        return await handler(app_request.request)


@attr.s()
class YCEmbedAuthService:
    _authorization_mode: AuthorizationMode = attr.ib()

    async def process_request(
            self,
            app_request: DLRequestBase,
            update_ctx: bool = False,
    ) -> YCEmbedContext:
        request_id = app_request.temp_rci.request_id
        assert request_id is not None, "Request ID is not set in RCI"

        access_ctrl = YCEmbedAccessController(
            authorization_mode=self._authorization_mode,
            request_headers=dict(app_request.request.headers),
            request_id=request_id,
            req_logging_ctx_ctrl=app_request.log_ctx_controller if update_ctx else None,
        )

        try:
            auth_ctx = await access_ctrl.perform_base_security_checks()
        except Exception as err:
            LOGGER.exception("yc auth error: %r", err)
            raise

        if update_ctx:
            enriched_rci = access_ctrl.update_rci_with_auth_ctx(
                app_request.temp_rci,
                auth_ctx,
            )
            app_request.replace_temp_rci(enriched_rci)

        LOGGER.info("YC embed check successfully passed")
        return auth_ctx

    @web.middleware
    @aiohttp_wrappers.DLRequestBase.use_dl_request_on_method
    async def middleware(self, app_request: DLRequestBase, handler: Handler) -> web.StreamResponse:
        if aiohttp_wrappers.RequiredResourceCommon.SKIP_AUTH in app_request.required_resources:
            LOGGER.info("Auth was skipped due to SKIP_AUTH flag in target view")
            return await handler(app_request.request)

        await self.process_request(app_request, update_ctx=True)
        return await handler(app_request.request)
