import logging
from typing import Sequence

import attr

import dl_app_api_base.auth.checkers as auth_checkers
import dl_app_api_base.auth.exc as auth_exc
import dl_app_api_base.request_context as request_context
import dl_app_base


LOGGER = logging.getLogger(__name__)


@attr.define(frozen=True, kw_only=True)
class AuthRequestContextDependenciesMixin(request_context.BaseRequestContextDependencies):
    request_auth_checkers: Sequence[auth_checkers.RequestAuthCheckerProtocol]


class AuthRequestContextMixin(request_context.BaseRequestContext):
    _dependencies: AuthRequestContextDependenciesMixin

    @dl_app_base.singleton_class_method_result
    async def get_auth_user(self) -> auth_checkers.BaseRequestAuthResult:
        for auth_checker in self._dependencies.request_auth_checkers:
            if not await auth_checker.is_applicable(self._aiohttp_request):
                continue

            return await auth_checker.check(self._aiohttp_request)

        raise auth_exc.NoApplicableAuthCheckersError()
