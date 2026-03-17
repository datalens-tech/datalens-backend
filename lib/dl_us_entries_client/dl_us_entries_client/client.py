import http
import logging
from typing import ClassVar

import attrs
import httpx
from typing_extensions import Self

import dl_auth
import dl_httpx
import dl_settings
import dl_us_entries_client.models as models


LOGGER = logging.getLogger(__name__)


US_ENTRIES_AUTH_TARGET = dl_auth.AuthTarget.declare("US_ENTRIES")


class USEntriesClientSettings(dl_settings.BaseSettings):
    BASE_URL: str
    USER_AUTH_PROVIDER: dl_settings.TypedAnnotation[dl_auth.AuthProviderSettings]


class UsEntriesClientException(Exception):
    pass


class EntryNotFoundError(UsEntriesClientException):
    pass


@attrs.define(kw_only=True)
class USEntriesAsyncClient:
    AUTH_TARGET: ClassVar[dl_auth.AuthTarget] = US_ENTRIES_AUTH_TARGET
    _base_client: dl_httpx.HttpxAsyncClient

    @classmethod
    def from_dependencies(cls, dependencies: dl_httpx.HttpxClientDependencies) -> Self:
        return cls(
            base_client=dl_httpx.HttpxAsyncClient.from_dependencies(
                dependencies=dl_httpx.HttpxClientDependencies(
                    base_url=dependencies.base_url,
                    base_cookies=dependencies.base_cookies,
                    base_headers=dependencies.base_headers,
                    ssl_context=dependencies.ssl_context,
                    retry_policy_factory=dependencies.retry_policy_factory,
                    auth_provider=dependencies.auth_provider,
                    logger=LOGGER,
                    debug_logging=dependencies.debug_logging,
                ),
            ),
        )

    async def close(self) -> None:
        await self._base_client.close()

    async def _send(self, request: httpx.Request) -> httpx.Response:
        async with self._base_client.send(request=request) as response:
            return response

    async def check_readiness(self) -> bool:
        try:
            await self.ping(models.PingRequest())
            return True
        except Exception:
            LOGGER.exception("USEntriesAsyncClient.check_readiness failed")
            return False

    async def ping(self, request: models.PingRequest) -> models.PingResponse:
        prepared = await self._base_client.prepare_request_async(request=request)
        response = await self._send(prepared)
        return models.PingResponse.model_validate(response.json())

    async def get_entry(self, request: models.EntryGetRequest) -> models.EntryGetResponse:
        prepared = await self._base_client.prepare_request_async(request=request)
        try:
            response = await self._send(prepared)
        except dl_httpx.HttpStatusHttpxClientException as e:
            if e.response.status_code == http.HTTPStatus.NOT_FOUND:
                raise EntryNotFoundError() from e
            raise
        return models.EntryGetResponse.model_validate(response.json())

    async def post_entry(self, request: models.EntryPostRequest) -> models.EntryPostResponse:
        prepared = await self._base_client.prepare_request_async(request=request)
        response = await self._send(prepared)
        return models.EntryPostResponse.model_validate(response.json())

    async def delete_entry(self, request: models.EntryDeleteRequest) -> None:
        prepared = await self._base_client.prepare_request_async(request=request)
        await self._send(prepared)
