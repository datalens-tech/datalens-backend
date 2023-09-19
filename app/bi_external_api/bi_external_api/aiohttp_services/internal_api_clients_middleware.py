from __future__ import annotations

import contextlib
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Optional,
)

from aiohttp import (
    ClientSession,
    TCPConnector,
    web,
)
import attr

from dl_api_commons.base_models import (
    AuthData,
    TenantCommon,
    TenantDef,
)
from dl_constants.api_constants import (
    DLHeaders,
    DLHeadersCommon,
)

from ..internal_api_clients.charts_api import APIClientCharts
from ..internal_api_clients.dash_api import APIClientDashboard
from ..internal_api_clients.dataset_api import APIClientBIBackControlPlane
from ..internal_api_clients.main import InternalAPIClients
from ..internal_api_clients.united_storage import (
    MiniUSClient,
    USMasterAuthData,
)
from .base import (
    ExtAPIRequest,
    ExtAPIRequiredResource,
    InternalAPIClientsFactory,
)


if TYPE_CHECKING:
    from aiohttp.typedefs import Handler


@attr.s()
class InternalAPIClientsDefaultFactory(InternalAPIClientsFactory):
    dataset_api_base_url: str = attr.ib()
    us_base_url: str = attr.ib()
    us_use_workbook_api: bool = attr.ib()
    # TODO FIX: BI-3266 Make required when charts & dash api will be available in docker-compose testing env
    dash_api_base_url: Optional[str] = attr.ib()
    charts_api_base_url: Optional[str] = attr.ib()

    session: ClientSession = attr.ib()
    req_id: str = attr.ib()
    extra_headers: dict[DLHeaders, str] = attr.ib()
    client_auth_data: AuthData = attr.ib()

    us_master_token: Optional[str] = attr.ib(repr=False)

    def get_internal_api_clients(self, tenant: TenantDef) -> InternalAPIClients:
        charts_api_base_url = self.charts_api_base_url
        dash_api_base_url = self.dash_api_base_url

        return InternalAPIClients(
            datasets_cp=APIClientBIBackControlPlane(
                session=self.session,
                base_url=self.dataset_api_base_url,
                tenant=tenant,
                auth_data=self.client_auth_data,
                req_id=self.req_id,
                extra_headers=self.extra_headers,
                use_workbooks_api=self.us_use_workbook_api,
            ),
            charts=APIClientCharts(
                session=self.session,
                base_url=charts_api_base_url,
                tenant=tenant,
                auth_data=self.client_auth_data,
                req_id=self.req_id,
                extra_headers=self.extra_headers,
                use_workbooks_api=self.us_use_workbook_api,
            )
            if charts_api_base_url is not None
            else None,
            dash=APIClientDashboard(
                session=self.session,
                base_url=dash_api_base_url,
                tenant=tenant,
                auth_data=self.client_auth_data,
                req_id=self.req_id,
                extra_headers=self.extra_headers,
                use_workbooks_api=self.us_use_workbook_api,
            )
            if dash_api_base_url is not None
            else None,
            us=MiniUSClient(
                session=self.session,
                base_url=self.us_base_url,
                tenant=tenant,
                auth_data=self.client_auth_data,
                req_id=self.req_id,
                extra_headers=self.extra_headers,
                use_workbooks_api=self.us_use_workbook_api,
            ),
        )

    def get_super_user_us_client(self) -> MiniUSClient:
        secret_us_master_token = self.us_master_token
        assert secret_us_master_token, "Could not create super user US client. Master token was not provided"

        return MiniUSClient(
            session=self.session,
            base_url=self.us_base_url,
            tenant=TenantCommon(),
            auth_data=USMasterAuthData(secret_us_master_token),
            req_id=self.req_id,
            extra_headers=self.extra_headers,
            use_workbooks_api=self.us_use_workbook_api,
        )


@attr.s()
class InternalAPIClientsMiddleware:
    dataset_api_base_url: str = attr.ib()
    us_base_url: str = attr.ib()
    us_use_workbook_api: bool = attr.ib()
    # TODO FIX: BI-3266 Make required when charts & dash api will be available in docker-compose testing env
    dash_api_base_url: Optional[str] = attr.ib()
    charts_api_base_url: Optional[str] = attr.ib()
    force_close_http_conn: bool = attr.ib()

    us_master_token: Optional[str] = attr.ib(repr=False)

    @contextlib.asynccontextmanager
    async def associate_int_api_clients_with_app_request(self, app_request: ExtAPIRequest) -> AsyncIterator[None]:
        rci = app_request.rci
        user_id = rci.user_id
        auth_data = rci.auth_data
        req_id = rci.request_id

        assert user_id is not None, "User ID was not set in RCI"
        assert auth_data is not None, "Auth data was not set in RCI"
        assert req_id is not None, "Request ID was not set in RCI"

        session: ClientSession

        if self.force_close_http_conn:
            session = ClientSession(connector=TCPConnector(force_close=True))
        else:
            session = ClientSession()

        extra_headers: dict[DLHeaders, str] = {
            header_name: header_value
            for header_name, header_value in {
                dl_header: app_request.rci.plain_headers.get(dl_header.value)
                for dl_header in (DLHeadersCommon.ALLOW_SUPERUSER, DLHeadersCommon.SUDO)
            }.items()
            if header_value is not None
        }

        app_request.set_internal_api_clients_factory(
            InternalAPIClientsDefaultFactory(
                dataset_api_base_url=self.dataset_api_base_url,
                us_base_url=self.us_base_url,
                us_use_workbook_api=self.us_use_workbook_api,
                dash_api_base_url=self.dash_api_base_url,
                charts_api_base_url=self.charts_api_base_url,
                session=session,
                req_id=req_id,
                extra_headers=extra_headers,
                client_auth_data=auth_data,
                us_master_token=self.us_master_token,
            )
        )

        yield

        await session.close()

    # TODO FIX: Check after MyPy upgrade if `error: Missing return statement` will not fires on func def
    @web.middleware
    @ExtAPIRequest.use_dl_request_on_method
    async def middleware(self, app_request: ExtAPIRequest, handler: Handler) -> web.StreamResponse:  # type: ignore
        async with contextlib.AsyncExitStack() as exit_stack:
            if ExtAPIRequiredResource.INT_API_CLIENTS in app_request.required_resources:
                await exit_stack.enter_async_context(self.associate_int_api_clients_with_app_request(app_request))

            return await handler(app_request.request)
