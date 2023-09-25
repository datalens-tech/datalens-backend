from __future__ import annotations

from asyncio import AbstractEventLoop
import json
from typing import (
    Any,
    Optional,
)

import aiohttp.test_utils
import attr
from werkzeug.test import Client as WClient

from dl_api_client.dsmaker.api.http_sync_base import (
    ClientResponse,
    SyncHttpClientBase,
)
from dl_utils.aio import await_sync


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
        _json: Optional[dict]

        @property
        def json(self) -> dict:
            assert self._json is not None
            return self._json

    def set_cookie(self, server_name: str, key: str, value: str = "", **kwargs: Any) -> None:
        if self._cookies is None:
            self._cookies = {}
        self._cookies.update({key: value})

    def delete_cookie(self, server_name: str, key: str, **kwargs: Any) -> None:
        if self._cookies is not None:
            self._cookies.pop(key, None)

    def post(self, url: str, **kwargs: Any) -> Resp:
        return self.open(url, method="post", **kwargs)

    def get(self, url: str, **kwargs: Any) -> Resp:
        return self.open(url, method="get", **kwargs)

    def open(
        self,
        url: str,
        *,
        method: str,
        data: Optional[str] = None,
        content_type: Optional[str] = None,
        headers: Optional[dict] = None,
    ) -> Resp:
        headers = dict(headers) if headers else {}
        if self._extra_headers:
            headers.update(self._extra_headers)

        if content_type is not None:
            headers["Content-Type"] = content_type
        resp = await_sync(
            self._aio_client.request(
                method,
                url,
                data=data,
                headers=headers,
                cookies=self._cookies,
            ),
            loop=self._loop,
        )
        resp_data = await_sync(resp.read())

        js: Optional[dict] = None
        if resp_data is not None:
            try:
                js = json.loads(resp_data)
            except json.JSONDecodeError:
                pass

        return self.Resp(status_code=resp.status, data=resp_data, json=js)


@attr.s
class WrappedAioSyncApiClient(SyncHttpClientBase):
    _int_wrapped_client: TestClientConverterAiohttpToFlask = attr.ib()

    def __attrs_post_init__(self) -> None:
        assert isinstance(self._int_wrapped_client, TestClientConverterAiohttpToFlask)

    def open(
        self,
        url: str,
        method: str,
        headers: dict,
        data: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> ClientResponse:
        method = method.lower()
        int_response = self._int_wrapped_client.open(
            url=url,
            method=method,
            headers=headers,
            data=data,
            content_type=content_type,
        )
        return ClientResponse(
            status_code=int_response.status_code,
            data=int_response.data,
            json=int_response.json,
        )


@attr.s
class FlaskSyncApiClient(SyncHttpClientBase):
    _int_wclient: WClient = attr.ib()

    def __attrs_post_init__(self) -> None:
        assert isinstance(self._int_wclient, WClient)

    def open(
        self,
        url: str,
        method: str,
        headers: dict,
        data: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> ClientResponse:
        method = method.lower()
        int_response = self._int_wclient.open(
            url,
            method=method,
            headers=headers,
            data=data,
            content_type=content_type,
        )
        return ClientResponse(
            status_code=int_response.status_code,
            data=int_response.data,
            json=int_response.json,
        )
