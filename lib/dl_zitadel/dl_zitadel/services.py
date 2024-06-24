import datetime
import logging
import typing

import dl_zitadel.clients as dl_zitadel_clients


LOGGER = logging.getLogger(__name__)


class ZitadelBaseTokenStorage:
    def __init__(
        self,
        token_refresh_timeout: datetime.timedelta = datetime.timedelta(seconds=60),
    ):
        self._token_refresh_timeout = token_refresh_timeout
        self._token: dl_zitadel_clients.Token | None = None

    def _is_token_expired(self) -> bool:
        if self._token is None:
            LOGGER.debug("Token is not set")
            return True

        if (self._token.expiration_datetime - self._token_refresh_timeout) < datetime.datetime.now():
            LOGGER.debug("Token is expired %s", self._token.expiration_datetime)
            return True

        return False


class SyncClientProtocol(typing.Protocol):
    def get_token(self) -> dl_zitadel_clients.Token:
        ...


class ZitadelSyncTokenStorage(ZitadelBaseTokenStorage):
    def __init__(
        self,
        client: SyncClientProtocol,
        token_refresh_timeout: datetime.timedelta = datetime.timedelta(seconds=60),
    ):
        self._client = client

        super().__init__(token_refresh_timeout)

    def get_token(self) -> str:
        if self._is_token_expired():
            self._token = self._client.get_token()

        return self._token.access_token


class AsyncClientProtocol(typing.Protocol):
    async def get_token(self) -> dl_zitadel_clients.Token:
        ...


# TODO: add soft refresh timeout to avoid multiple requests on timeout
class ZitadelAsyncTokenStorage(ZitadelBaseTokenStorage):
    def __init__(
        self,
        client: AsyncClientProtocol,
        token_refresh_timeout: datetime.timedelta = datetime.timedelta(seconds=60),
    ):
        self._client = client

        super().__init__(token_refresh_timeout)

    async def get_token(self) -> str:
        if self._is_token_expired():
            self._token = await self._client.get_token()

        return self._token.access_token


__all__ = [
    "SyncClientProtocol",
    "AsyncClientProtocol",
    "ZitadelSyncTokenStorage",
    "ZitadelAsyncTokenStorage",
]
