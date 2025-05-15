from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
)

from aiohttp import web

from dl_api_commons.aiohttp import aiohttp_wrappers
from dl_api_commons.base_models import (
    AuthData,
    NoAuthData,
    TenantCommon,
    TenantDef,
)


if TYPE_CHECKING:
    from aiohttp.typedefs import (
        Handler,
        Middleware,
    )


def auth_trust_middleware(
    fake_user_id: Optional[str] = None,
    fake_user_name: Optional[str] = None,
    fake_tenant: Optional[TenantDef] = None,
    fake_auth: Optional[AuthData] = None,
) -> Middleware:
    @web.middleware
    @aiohttp_wrappers.DLRequestBase.use_dl_request
    async def actual_auth_trust_middleware(
        app_request: aiohttp_wrappers.DLRequestBase, handler: Handler
    ) -> web.StreamResponse:
        if aiohttp_wrappers.RequiredResourceCommon.SKIP_AUTH in app_request.required_resources:
            # LOGGER.info("Auth was skipped due to SKIP_AUTH flag in target view")

            # TODO BI-6257 move tenant resolution into a separate middleware
            updated_rci = app_request.temp_rci.clone(
                tenant=TenantCommon() if fake_tenant is None else fake_tenant,
            )
            app_request.replace_temp_rci(updated_rci)
        elif app_request.request.headers.get("x-unauthorized"):
            raise web.HTTPUnauthorized()
        else:
            updated_rci = app_request.temp_rci.clone(
                auth_data=NoAuthData() if fake_auth is None else fake_auth,
                tenant=TenantCommon() if fake_tenant is None else fake_tenant,
            )
            if fake_user_id is not None:
                updated_rci = updated_rci.clone(user_id=fake_user_id)
            if fake_user_name is not None:
                updated_rci = updated_rci.clone(user_name=fake_user_name)
            app_request.replace_temp_rci(updated_rci)

        return await handler(app_request.request)

    return actual_auth_trust_middleware
