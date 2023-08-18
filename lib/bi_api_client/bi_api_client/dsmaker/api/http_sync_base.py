from __future__ import annotations

import abc
import json
import time
from http import HTTPStatus
from typing import Mapping, Optional

import attr

from bi_api_client.dsmaker.primitives import Dataset
from bi_api_client.dsmaker.api.base import ApiBase
from bi_api_client.dsmaker.api.serialization_base import BaseApiV1SerializationAdapter


@attr.s(frozen=True)
class ClientResponse:
    status_code: int = attr.ib(kw_only=True)
    data: bytes = attr.ib(kw_only=True)
    _json: Optional[dict] = attr.ib(kw_only=True)

    @property
    def json(self) -> dict:
        assert self._json is not None
        return self._json


class SyncHttpClientBase(abc.ABC):
    @abc.abstractmethod
    def open(
            self, url: str, method: str,
            headers: dict, data: Optional[str] = None,
            content_type: Optional[str] = None,
    ) -> ClientResponse:
        raise NotImplementedError

    def _strtict_dict(self, d: Optional[dict]) -> dict:
        if d is None:
            d = {}
        assert d is not None
        return d

    def post(
            self, url: str,
            headers: Optional[dict] = None, data: Optional[str] = None,
            content_type: Optional[str] = None,
    ) -> ClientResponse:
        return self.open(
            url=url, method='post', headers=self._strtict_dict(headers),
            data=data, content_type=content_type,
        )

    def put(
            self, url: str,
            headers: Optional[dict] = None, data: Optional[str] = None,
            content_type: Optional[str] = None,
    ) -> ClientResponse:
        return self.open(
            url=url, method='put', headers=self._strtict_dict(headers),
            data=data, content_type=content_type,
        )

    def get(self, url: str, headers: Optional[dict] = None) -> ClientResponse:
        return self.open(url=url, method='get', headers=self._strtict_dict(headers))

    def delete(self, url: str, headers: Optional[dict] = None) -> ClientResponse:
        return self.open(url=url, method='delete', headers=self._strtict_dict(headers))


@attr.s
class SyncHttpApiBase(ApiBase):
    client: SyncHttpClientBase = attr.ib()
    headers: Mapping[str, str] = attr.ib(factory=dict)

    def __attrs_post_init__(self) -> None:
        self.headers = dict(self.headers)

    def _request(
            self,
            url: str, method: str,
            data: Optional[dict] = None,
            headers: Optional[dict] = None,
            lock_timeout: int = None,
    ) -> ClientResponse:

        data_str: Optional[str] = None
        content_type: Optional[str] = None
        if method not in ('get', 'delete') and data is not None:
            content_type = 'application/json'
            data_str = json.dumps(data)

        headers = headers or {}
        assert isinstance(headers, dict)
        headers = {**self.headers, **headers}

        started = time.monotonic()

        def send_request() -> ClientResponse:
            return self.client.open(
                url=url, method=method, headers=headers,
                data=data_str, content_type=content_type,
            )

        response = send_request()

        while (
                response.status_code == HTTPStatus.LOCKED
                and lock_timeout and time.monotonic() - started < lock_timeout
        ):
            time.sleep(0.3)
            response = send_request()

        return response


@attr.s
class SyncHttpApiV1Base(SyncHttpApiBase):
    serial_adapter: BaseApiV1SerializationAdapter = attr.ib(init=False, factory=BaseApiV1SerializationAdapter)

    def dump_dataset_to_request_body(self, dataset: Dataset) -> dict:
        return self.serial_adapter.dump_dataset(dataset)
