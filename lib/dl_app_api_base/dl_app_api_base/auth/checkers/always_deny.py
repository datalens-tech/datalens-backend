from typing import NoReturn

import aiohttp.web
import attr

import dl_app_api_base.auth.checkers.base as auth_checkers_base
import dl_app_api_base.auth.exc as auth_exc


@attr.define(frozen=True, kw_only=True)
class AlwaysDenyAuthChecker(auth_checkers_base.BaseRequestAuthChecker):
    async def check(self, request: aiohttp.web.Request) -> NoReturn:
        raise auth_exc.AuthFailureError("Always deny auth")
