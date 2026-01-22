import aiohttp.web
import attr

import dl_app_api_base.auth.checkers.base as auth_checkers_base


class NoAuthResult(auth_checkers_base.BaseRequestAuthResult):
    ...


@attr.define(frozen=True, kw_only=True)
class NoAuthChecker(auth_checkers_base.BaseRequestAuthChecker):
    async def check(self, request: aiohttp.web.Request) -> NoAuthResult:
        return NoAuthResult()
