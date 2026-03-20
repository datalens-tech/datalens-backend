import attrs

import dl_auth


@attrs.define(frozen=True, kw_only=True)
class AuthDataUSAuthProvider:
    """Adapts AuthData to AuthProviderProtocol targeting UNITED_STORAGE."""

    auth_data: dl_auth.AuthData

    def get_headers(self) -> dict[str, str]:
        return {k.value: v for k, v in self.auth_data.get_headers(dl_auth.AuthTarget.UNITED_STORAGE).items()}

    def get_cookies(self) -> dict[str, str]:
        return {k.value: v for k, v in self.auth_data.get_cookies(dl_auth.AuthTarget.UNITED_STORAGE).items()}

    async def get_headers_async(self) -> dict[str, str]:
        return self.get_headers()

    async def get_cookies_async(self) -> dict[str, str]:
        return self.get_cookies()
