from __future__ import annotations

from typing import Optional

import attr
from werkzeug.test import Client as WClient

from bi_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase, ClientResponse
from bi_testing_ya.api_wrappers import TestClientConverterAiohttpToFlask


@attr.s
class WrappedAioSyncApiClient(SyncHttpClientBase):
    _int_wrapped_client: TestClientConverterAiohttpToFlask = attr.ib()

    def __attrs_post_init__(self) -> None:
        assert isinstance(self._int_wrapped_client, TestClientConverterAiohttpToFlask)

    def open(
            self, url: str, method: str,
            headers: dict, data: Optional[str] = None,
            content_type: Optional[str] = None,
    ) -> ClientResponse:

        method = method.lower()
        int_response = self._int_wrapped_client.open(
            url=url, method=method, headers=headers,
            data=data, content_type=content_type,
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
            self, url: str, method: str,
            headers: dict, data: Optional[str] = None,
            content_type: Optional[str] = None,
    ) -> ClientResponse:

        method = method.lower()
        int_response = self._int_wclient.open(
            url, method=method, headers=headers,
            data=data, content_type=content_type,
        )
        return ClientResponse(
            status_code=int_response.status_code,
            data=int_response.data,
            json=int_response.json,
        )
