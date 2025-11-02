from aiohttp import web
from aiohttp.typedefs import (
    Handler,
    Middleware,
)

import dl_api_commons
from dl_api_commons.aiohttp import aiohttp_wrappers
import dl_auth


def auth_trust_middleware(
    fake_user_id: str | None = None,
    fake_user_name: str | None = None,
    fake_tenant: dl_api_commons.TenantDef | None = None,
    fake_auth: dl_auth.AuthData | None = None,
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
                tenant=dl_api_commons.TenantCommon() if fake_tenant is None else fake_tenant,
            )
            app_request.replace_temp_rci(updated_rci)
        elif app_request.request.headers.get("x-unauthorized"):
            raise web.HTTPUnauthorized()
        else:
            updated_rci = app_request.temp_rci.clone(
                auth_data=dl_auth.NoAuthData() if fake_auth is None else fake_auth,
                tenant=dl_api_commons.TenantCommon() if fake_tenant is None else fake_tenant,
            )
            if fake_user_id is not None:
                updated_rci = updated_rci.clone(user_id=fake_user_id)
            if fake_user_name is not None:
                updated_rci = updated_rci.clone(user_name=fake_user_name)
            app_request.replace_temp_rci(updated_rci)

        return await handler(app_request.request)

    return actual_auth_trust_middleware
