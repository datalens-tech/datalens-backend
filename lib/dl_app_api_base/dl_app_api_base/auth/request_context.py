import logging
from typing import (
    Protocol,
    Sequence,
)

import attr
import attrs

import dl_app_api_base.auth.checkers as auth_checkers
import dl_app_api_base.auth.exc as auth_exc
import dl_app_api_base.request_context as request_context
import dl_app_base
import dl_auth


LOGGER = logging.getLogger(__name__)


class UserAuthProviderFactory(Protocol):
    def create(self, auth_result: auth_checkers.BaseRequestAuthResult) -> dl_auth.AuthProviderProtocol:
        """
        :raises dl_app_api_base.UserAuthProviderFactoryError: if the auth result type is not supported
        """
        ...


@attrs.define(frozen=True, kw_only=True)
class DirectUserAuthProviderFactory:
    """
    Used when we want to use a static AuthProvider as UserAuthProvider
    Don't use for private(service2service authorization) clients as they should get AuthProvider directly.
    """

    auth_provider: dl_auth.BaseAuthProvider

    def create(self, auth_result: auth_checkers.BaseRequestAuthResult) -> dl_auth.AuthProviderProtocol:
        return self.auth_provider


@attr.define(frozen=True, kw_only=True)
class AuthRequestContextDependenciesMixin(request_context.BaseRequestContextDependencies):
    request_auth_checkers: Sequence[auth_checkers.RequestAuthCheckerProtocol]
    user_auth_provider_factories: dict[dl_auth.AuthTarget, UserAuthProviderFactory] = attrs.Factory(dict)


class AuthRequestContextMixin(request_context.BaseRequestContext):
    _dependencies: AuthRequestContextDependenciesMixin

    @dl_app_base.singleton_class_method_result
    async def get_auth_user(self) -> auth_checkers.BaseRequestAuthResult:
        for auth_checker in self._dependencies.request_auth_checkers:
            if not await auth_checker.is_applicable():
                continue
            LOGGER.info("Found applicable auth checker: %s", auth_checker.name)
            return await auth_checker.check()

        raise auth_exc.NoApplicableAuthCheckersError()

    async def get_user_auth_provider(
        self,
        target: dl_auth.AuthTarget,
    ) -> dl_auth.AuthProviderProtocol:
        auth_result = await self.get_auth_user()
        factory = self._dependencies.user_auth_provider_factories[target]
        return factory.create(auth_result)
