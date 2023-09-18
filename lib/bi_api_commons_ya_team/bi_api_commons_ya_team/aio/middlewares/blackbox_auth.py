from __future__ import annotations

import json
import logging
from typing import Optional

import aiohttp
from aiohttp import (
    hdrs,
    web,
)
from aiohttp.typedefs import Handler
import attr

from bi_api_commons_ya_team.constants import DLCookiesYT
from bi_api_commons_ya_team.models import YaTeamAuthData
from bi_blackbox_client.authenticate import authenticate_async
from bi_blackbox_client.exc import InsufficientAuthData
from dl_api_commons.access_control_common import (
    AuthTokenType,
    BadHeaderPrefixError,
    get_token_from_authorization_header,
)
from dl_api_commons.aio.typing import AIOHTTPMiddleware
from dl_api_commons.aiohttp import aiohttp_wrappers
from dl_api_commons.base_models import TenantCommon
from dl_constants.api_constants import DLHeadersCommon

LOGGER = logging.getLogger(__name__)


def blackbox_auth_middleware(
    client_session: Optional[aiohttp.ClientSession] = None,
    tvm_info: Optional[str] = None,
) -> AIOHTTPMiddleware:
    @web.middleware
    @aiohttp_wrappers.DLRequestBase.use_dl_request
    async def actual_blackbox_auth_middleware(
        app_request: aiohttp_wrappers.DLRequestBase, handler: Handler
    ) -> web.StreamResponse:
        if aiohttp_wrappers.RequiredResourceCommon.SKIP_AUTH in app_request.required_resources:
            LOGGER.info("Auth was skipped due to SKIP_AUTH flag in target view")
            return await handler(app_request.request)

        req = app_request.request

        client_host: str

        x_forwarded_for = app_request.get_single_header(hdrs.X_FORWARDED_FOR, required=False)

        if x_forwarded_for is not None:
            # Preserving logic from Flask version and use last forwarder instead of first
            ip_list = [ip.strip() for ip in x_forwarded_for.split(",")]
            if len(ip_list) > 1:
                client_host = ip_list[-2]
            else:
                # unlikely to happen
                client_host = ip_list[0]
        else:
            # No X-Forward-For -> use source IP address of request
            assert req.remote is not None
            client_host = req.remote

        secret_session_id_cookie = req.cookies.get(DLCookiesYT.YA_TEAM_SESSION_ID.value)
        secret_sessionid2_cookie = req.cookies.get(DLCookiesYT.YA_TEAM_SESSION_ID_2.value)
        secret_authorization_header = app_request.get_single_header(DLHeadersCommon.AUTHORIZATION_TOKEN, required=False)

        try:
            oauth_token = get_token_from_authorization_header(secret_authorization_header, AuthTokenType.oauth)
        except BadHeaderPrefixError as err:
            LOGGER.exception("auth error: %r", err)
            raise web.HTTPUnauthorized(reason=err.user_message)

        try:
            auth_results = await authenticate_async(
                aiohttp_client_session=client_session,
                tvm_info=tvm_info,
                userip=client_host,
                # in case of tests 127.0.0.1:${RANDOM_PORT} will be sent
                host=req.host,
                session_id_cookie=secret_session_id_cookie,
                sessionid2_cookie=secret_sessionid2_cookie,
                authorization_header=secret_authorization_header,
                statbox_id=app_request.temp_rci.request_id,
            )
        except InsufficientAuthData:
            raise web.HTTPForbidden()

        user_id = auth_results.get("user_id")

        if user_id is None:
            LOGGER.info(
                "Blackbox auth was not passed. Blackbox resp: %s", json.dumps(auth_results.get("blackbox_response"))
            )
            raise web.HTTPForbidden()

        user_name = auth_results.get("username")

        if app_request.log_ctx_controller:
            app_request.log_ctx_controller.put_to_context("user_id", user_id)
            app_request.log_ctx_controller.put_to_context("user_name", user_name)

        app_request.replace_temp_rci(
            attr.evolve(
                app_request.temp_rci,
                user_id=user_id,
                user_name=user_name,
                tenant=TenantCommon(),
                auth_data=YaTeamAuthData(
                    oauth_token=oauth_token,
                    cookie_session_id=secret_session_id_cookie,
                    cookie_sessionid2=secret_sessionid2_cookie,
                ),
            )
        )
        return await handler(app_request.request)

    return actual_blackbox_auth_middleware
