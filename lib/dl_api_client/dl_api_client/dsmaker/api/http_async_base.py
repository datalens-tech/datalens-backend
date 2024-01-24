from __future__ import annotations

import abc
import asyncio
from http import HTTPStatus
import json
import time
from typing import (
    Mapping,
    Optional,
)

import attr

from dl_api_client.dsmaker.api.base import ApiBase
from dl_api_client.dsmaker.api.http_sync_base import ClientResponse
from dl_api_client.dsmaker.api.serialization_base import BaseApiV1SerializationAdapter
from dl_api_client.dsmaker.primitives import Dataset


class AsyncHttpClientBase(abc.ABC):
    @abc.abstractmethod
    async def open(
        self,
        url: str,
        method: str,
        headers: dict,
        data: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> ClientResponse:
        raise NotImplementedError


@attr.s
class AsyncHttpApiBase(ApiBase):
    client: AsyncHttpClientBase = attr.ib()
    headers: Mapping[str, str] = attr.ib(factory=dict)

    def __attrs_post_init__(self) -> None:
        self.headers = dict(self.headers)

    async def _request(
        self,
        url: str,
        method: str,
        data: Optional[dict] = None,
        headers: Optional[dict] = None,
        lock_timeout: int = None,  # type: ignore  # 2024-01-24 # TODO: Incompatible default for argument "lock_timeout" (default has type "None", argument has type "int")  [assignment]
    ) -> ClientResponse:
        data_str: Optional[str] = None
        content_type: Optional[str] = None
        if method not in ("get", "delete") and data is not None:
            content_type = "application/json"
            data_str = json.dumps(data)

        headers = headers or {}
        assert isinstance(headers, dict)
        headers = {**self.headers, **headers}

        started = time.monotonic()

        async def send_request() -> ClientResponse:
            return await self.client.open(
                url=url,
                method=method,
                headers=headers,
                data=data_str,
                content_type=content_type,
            )

        response = await send_request()

        while response.status_code == HTTPStatus.LOCKED and lock_timeout and time.monotonic() - started < lock_timeout:
            await asyncio.sleep(0.3)
            response = send_request()  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "Coroutine[Any, Any, ClientResponse]", variable has type "ClientResponse")  [assignment]

        return response


@attr.s
class AsyncHttpApiV1Base(AsyncHttpApiBase):
    serial_adapter: BaseApiV1SerializationAdapter = attr.ib(init=False, factory=BaseApiV1SerializationAdapter)

    def dump_dataset_to_request_body(self, dataset: Dataset) -> dict:
        return self.serial_adapter.dump_dataset(dataset)
