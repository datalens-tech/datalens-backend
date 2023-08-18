from __future__ import annotations

import json
import uuid
import ssl
from asyncio.events import AbstractEventLoop
from typing import Optional, Dict, Any, Union, AsyncIterable, Mapping

import aiohttp.test_utils
import attr
from aiohttp import MultipartWriter

from bi_api_commons.base_models import TenantDef
from bi_api_commons.tracing import get_current_tracing_headers
from bi_constants.api_constants import DLHeadersCommon
from bi_utils.aio import await_sync
from bi_configs.utils import get_root_certificates_path

from .cloud_tokens import AccountCredentials


@attr.s(frozen=True)
class Req:
    method: str = attr.ib()
    url: str = attr.ib()
    params: Dict[str, Union[str, int]] = attr.ib(default=None)
    data_json: Optional[Dict[str, Any]] = attr.ib(default=None)

    require_ok: bool = attr.ib(default=True, kw_only=True)
    extra_headers: Optional[Dict[str, str]] = attr.ib(default=None, kw_only=True)
    add_common_headers: bool = attr.ib(default=True, kw_only=True)
    data: Union[bytes, str, AsyncIterable[bytes], MultipartWriter, None] = attr.ib(default=None, kw_only=True)


@attr.s(frozen=True, auto_attribs=True)
class Resp:
    status: int
    content: bytes
    content_type: Optional[str]
    json: dict
    req_id: str
    headers: Mapping


@attr.s(auto_attribs=True)
class HTTPClientWrapper:
    session: aiohttp.ClientSession
    base_url: str

    def _make_url(self, url: str):
        return f"{self.base_url.rstrip('/')}/{url.lstrip('/')}"

    async def request(
        self, method: str, url: str,
        params: Optional[Mapping[str, str]] = None,
        data: Any = None,
        json: Any = None,
        headers: Optional[Mapping[str, str]] = None,
        **kwargs,
    ):
        return await self.session.request(
            method, self._make_url(url),
            data=data,
            params=params,
            json=json,
            headers=headers,
            **kwargs,
        )


class RequestExecutionException(Exception):
    def __init__(
        self,
        msg: str,
        content: bytes,
        status: int,
        request_id: str
    ):
        super().__init__(msg, content)
        self.status = status
        self.request_id = request_id


@attr.s(auto_attribs=True)
class APIClient:
    web_app: Union[aiohttp.test_utils.TestClient, HTTPClientWrapper]
    folder_id: str
    account_credentials: Optional[AccountCredentials] = attr.ib(default=None)
    public_api_key: Optional[str] = attr.ib(default=None)
    req_id: Optional[str] = attr.ib(default=None)
    tenant: Optional[TenantDef] = None

    @staticmethod
    def common_headers(*, folder_id: str, req_id: Optional[str] = None, token: Optional[str] = None,
                       public_api_key: Optional[str] = None, is_intranet_user: bool = False,
                       tenant: Optional[TenantDef] = None):
        if public_api_key is not None and token is not None:
            raise ValueError("Public API key and access token (IAM or OAuth) can not be provided simultaneously.")

        if public_api_key is None and token is None:
            raise ValueError("Either public API key or access token (IAM or OAuth) must be provided.")

        headers = {
            DLHeadersCommon.REQUEST_ID.value: req_id if req_id is not None else str(uuid.uuid4()),
        }

        if tenant is not None:
            headers[DLHeadersCommon.TENANT_ID.value] = tenant.get_tenant_id()
        else:
            headers[DLHeadersCommon.FOLDER_ID.value] = folder_id

        if token is not None:
            if is_intranet_user:
                headers[DLHeadersCommon.AUTHORIZATION_TOKEN.value] = f"OAuth {token}"
            else:
                headers[DLHeadersCommon.IAM_TOKEN.value] = token

        if public_api_key is not None:
            headers[DLHeadersCommon.PUBLIC_API_KEY.value] = public_api_key

        tracing_headers = get_current_tracing_headers()

        return {**headers, **tracing_headers}

    def get_local_common_headers(self, req_id):
        if self.account_credentials:
            token = self.account_credentials.token
            is_intranet_user = self.account_credentials.is_intranet_user
            return self.common_headers(folder_id=self.folder_id, req_id=req_id,
                                       token=token, is_intranet_user=is_intranet_user,
                                       public_api_key=self.public_api_key,
                                       tenant=self.tenant)

        return self.common_headers(folder_id=self.folder_id, req_id=req_id, public_api_key=self.public_api_key,
                                   tenant=self.tenant)

    async def make_request(self, rq: Req) -> Resp:
        if rq.data is not None and rq.data_json is not None:
            raise ValueError("data_json and data can not be provided simultaneously")

        req_id = uuid.uuid4().hex if self.req_id is None else self.req_id
        resp = await self.web_app.request(
            rq.method, rq.url,
            headers={
                **(self.get_local_common_headers(req_id=req_id) if rq.add_common_headers else {}),
                **(rq.extra_headers if rq.extra_headers else {}),
            },
            params=rq.params,
            data=rq.data,
            json=rq.data_json,
            ssl_context=ssl.create_default_context(cafile=get_root_certificates_path())
        )
        content = await resp.read()

        if resp.content_type == 'application/json':
            resp_json = await resp.json()
        else:
            resp_json = None

        if rq.require_ok:
            if resp.status >= 400:
                raise RequestExecutionException("Non-ok response", content, status=resp.status, request_id=req_id)

        return Resp(
            req_id=req_id,
            status=resp.status,
            json=resp_json,
            content=content,
            content_type=resp.content_type,
            headers=resp.headers,
        )


@attr.s
class TestClientConverterAiohttpToFlask:
    _loop: AbstractEventLoop = attr.ib()
    _aio_client: aiohttp.test_utils.TestClient = attr.ib()
    _extra_headers: Optional[dict[str, str]] = attr.ib(default=None)
    _cookies: Optional[dict[str, str]] = attr.ib(default=None)

    @attr.s(frozen=True, auto_attribs=True)
    class Resp:
        status_code: int
        data: bytes
        json: dict

    def set_cookie(self, server_name: str, key: str, value: str = "", **kwargs: Any) -> None:
        if self._cookies is None:
            self._cookies = {}
        self._cookies.update({key: value})

    def delete_cookie(self, server_name: str, key: str, **kwargs: Any) -> None:
        if self._cookies is not None:
            self._cookies.pop(key, None)

    def post(self, url: str, **kwargs):
        return self.open(url, method='post', **kwargs)

    def get(self, url: str, **kwargs):
        return self.open(url, method='get', **kwargs)

    def open(
            self,
            url: str, *,
            method: str,
            data: Optional[str] = None,
            content_type: Optional[str] = None,
            headers: dict = None,
    ) -> Resp:
        headers = dict(headers) if headers else {}
        if self._extra_headers:
            headers.update(self._extra_headers)

        if content_type is not None:
            headers['Content-Type'] = content_type
        resp = await_sync(self._aio_client.request(
            method, url, data=data, headers=headers, cookies=self._cookies,
        ), loop=self._loop)
        data = await_sync(resp.read())
        try:
            js = json.loads(data)
        except json.JSONDecodeError:
            js = None
        return self.Resp(status_code=resp.status, data=data, json=js)
