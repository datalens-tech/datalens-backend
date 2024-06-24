import datetime
import logging
import typing

import attr
import httpx
import pydantic


LOGGER = logging.getLogger(__name__)


@attr.s(auto_attribs=True)
class IntrospectResult:
    active: bool
    username: str | None = None
    sub: str | None = None


@attr.s(auto_attribs=True)
class Token:
    access_token: str
    expires_in: int
    request_datetime: datetime.datetime

    @property
    def expiration_datetime(self) -> datetime.datetime:
        return self.request_datetime + datetime.timedelta(seconds=self.expires_in)


class IntrospectPostResponse(pydantic.BaseModel):
    active: bool
    username: str | None = None
    sub: str | None = None

    def to_dataclass(self) -> IntrospectResult:
        return IntrospectResult(
            active=self.active,
            username=self.username,
            sub=self.sub,
        )


class TokenPostResponse(pydantic.BaseModel):
    access_token: str
    token_type: str
    expires_in: int

    def to_dataclass(self) -> Token:
        return Token(
            access_token=self.access_token,
            expires_in=self.expires_in,
            request_datetime=datetime.datetime.now(),
        )


@attr.s(auto_attribs=True)
class ZitadelBaseClient:
    _base_client: httpx.Client | httpx.AsyncClient
    _base_url: str
    _project_id: str
    _client_id: str
    _client_secret: str
    _app_client_id: str
    _app_client_secret: str

    def _log_request(self, request: httpx.Request) -> None:
        LOGGER.debug(
            "Sending request:\nmethod: %s\ncontent: %s",
            request.method,
            request.content.decode() if request.content else None,
        )

    def _log_response(self, response: httpx.Response) -> None:
        LOGGER.debug(
            "Received response:\ncode: %s\ndata: %s",
            response.status_code,
            response.text,
        )

    def _prepare_url(self, path: str) -> str:
        return f"{self._base_url}{path}"

    def _prepare_auth(self) -> httpx.BasicAuth:
        return httpx.BasicAuth(username=self._client_id, password=self._client_secret)

    def _prepare_app_auth(self) -> httpx.BasicAuth:
        return httpx.BasicAuth(username=self._app_client_id, password=self._app_client_secret)

    def _process_response(self, response: httpx.Response) -> dict[str, typing.Any]:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError:
            LOGGER.error("Error while processing response code: %s, data: %s", response.status_code, response.text)
            raise

        return response.json()

    def _get_token_build_request(self) -> httpx.Request:
        data = {
            "grant_type": "client_credentials",
            "scope": f"openid profile urn:zitadel:iam:org:project:id:{self._project_id}:aud",
        }
        return self._base_client.build_request(
            method="POST",
            url=self._prepare_url("/oauth/v2/token"),
            data=data,
        )

    def _get_token_process_response(self, response: httpx.Response) -> Token:
        data = self._process_response(response)
        return TokenPostResponse(**data).to_dataclass()

    def _introspect_build_request(self, token: str) -> httpx.Request:
        return self._base_client.build_request(
            method="POST",
            url=self._prepare_url("/oauth/v2/introspect"),
            data={
                "token": token,
            },
        )

    def _introspect_process_response(self, response: httpx.Response) -> IntrospectResult:
        data = self._process_response(response)
        return IntrospectPostResponse(**data).to_dataclass()


@attr.s(auto_attribs=True)
class ZitadelSyncClient(ZitadelBaseClient):
    _base_client: httpx.Client

    def _send(
        self,
        request: httpx.Request,
        auth: httpx.Auth,
    ) -> httpx.Response:
        self._log_request(request)
        response = self._base_client.send(
            request=request,
            auth=auth,
        )
        self._log_response(response)

        return response

    def get_token(self) -> Token:
        response = self._send(
            request=self._get_token_build_request(),
            auth=self._prepare_auth(),
        )
        return self._get_token_process_response(response)

    def introspect(self, token: str) -> IntrospectResult:
        response = self._send(
            request=self._introspect_build_request(token),
            auth=self._prepare_app_auth(),
        )
        return self._introspect_process_response(response)


@attr.s(auto_attribs=True)
class ZitadelAsyncClient(ZitadelBaseClient):
    _base_client: httpx.AsyncClient

    async def _send(
        self,
        request: httpx.Request,
        auth: httpx.Auth,
    ) -> httpx.Response:
        self._log_request(request)
        response = await self._base_client.send(
            request=request,
            auth=auth,
        )
        self._log_response(response)

        return response

    async def get_token(self) -> Token:
        response = await self._send(
            request=self._get_token_build_request(),
            auth=self._prepare_auth(),
        )
        return self._get_token_process_response(response)

    async def introspect(self, token: str) -> IntrospectResult:
        response = await self._send(
            request=self._introspect_build_request(token),
            auth=self._prepare_app_auth(),
        )
        return self._introspect_process_response(response)


__all__ = [
    "ZitadelAsyncClient",
    "ZitadelSyncClient",
    "Token",
]
