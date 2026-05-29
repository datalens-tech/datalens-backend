import logging
from typing import (
    ClassVar,
    Self,
)

import attrs
import httpx

import dl_auth
import dl_httpx
import dl_settings
import dl_us_entries_client.clients.private.models as private_models
import dl_us_entries_client.exceptions as exceptions
import dl_us_entries_client.models as models

LOGGER = logging.getLogger(__name__)


US_ENTRIES_PRIVATE_AUTH_TARGET = dl_auth.AuthTarget.declare("US_ENTRIES_PRIVATE")


class USEntriesPrivateClientSettings(dl_settings.BaseSettings):
    BASE_URL: str
    SERVICE_AUTH_PROVIDER: dl_settings.TypedAnnotation[dl_auth.AuthProviderSettings]


@attrs.define(kw_only=True)
class USEntriesPrivateAsyncClient:
    AUTH_TARGET: ClassVar[dl_auth.AuthTarget] = US_ENTRIES_PRIVATE_AUTH_TARGET
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

    async def _send(
        self,
        request: httpx.Request,
        *,
        error_transformer: dl_httpx.ErrorTransformerProtocol = dl_httpx.NULL_ERROR_TRANSFORMER,
    ) -> httpx.Response:
        async with self._base_client.send(request=request, error_transformer=error_transformer) as response:
            return response

    async def check_readiness(self) -> bool:
        try:
            await self.ping(models.PingRequest())
            return True
        except Exception:
            LOGGER.exception("USEntriesPrivateAsyncClient.check_readiness failed")
            return False

    async def ping(self, request: models.PingRequest) -> None:
        prepared = await self._base_client.prepare_request(request=request)
        await self._send(prepared)

    async def get_entry(self, request: private_models.PrivateEntryGetRequest) -> private_models.PrivateEntryGetResponse:
        prepared = await self._base_client.prepare_request(request=request)
        response = await self._send(prepared, error_transformer=request.error_transformer)
        result = private_models.PrivateEntryGetResponse.model_validate(response.json())
        if request.include_permissions_info and result.permissions is None:
            raise exceptions.UsEntriesClientException("Permissions requested but not returned by US")
        return result

    async def post_entry(
        self, request: private_models.PrivateEntryPostRequest
    ) -> private_models.PrivateEntryPostResponse:
        prepared = await self._base_client.prepare_request(request=request)
        response = await self._send(prepared)
        return private_models.PrivateEntryPostResponse.model_validate(response.json())

    async def delete_entry(self, request: private_models.PrivateEntryDeleteRequest) -> None:
        prepared = await self._base_client.prepare_request(request=request)
        await self._send(prepared)

    async def post_unversioned_data(
        self, request: private_models.PrivateEntryUnversionedDataPostRequest
    ) -> private_models.PrivateEntryUnversionedDataPostResponse:
        prepared = await self._base_client.prepare_request(request=request)
        response = await self._send(prepared)
        return private_models.PrivateEntryUnversionedDataPostResponse.model_validate(response.json())
