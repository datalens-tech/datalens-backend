import logging
from typing import (
    Any,
    AsyncIterable,
    Mapping,
)
import uuid

import aiohttp
import attr

import dl_api_commons
import dl_auth
import dl_constants


LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class Req:
    method: str = attr.ib()
    url: str = attr.ib()
    params: dict[str, str | int] = attr.ib(default=None)
    data_json: dict[str, Any] = attr.ib(default=None)
    data: bytes | str | AsyncIterable[bytes] | aiohttp.MultipartWriter | None = attr.ib(default=None, kw_only=True)

    require_ok: bool = attr.ib(default=True, kw_only=True)
    extra_headers: dict[dl_constants.DLHeaders, str] | None = attr.ib(default=None, kw_only=True)
    add_common_headers: bool = attr.ib(default=True, kw_only=True)


@attr.s(frozen=True, auto_attribs=True)
class Resp:
    status: int
    content: bytes
    content_type: str | None
    json: dict
    req_id: str
    headers: Mapping


class RequestExecutionException(Exception):
    def __init__(self, msg: str, content: bytes, status: int, request_id: str):
        super().__init__(msg, content)
        self.status = status
        self.request_id = request_id


@attr.s(auto_attribs=True)
class DLCommonAPIClient:
    _base_url: str = attr.ib()
    _tenant: dl_api_commons.TenantDef = attr.ib()
    _auth_data: dl_auth.AuthData = attr.ib()
    _session: aiohttp.ClientSession = attr.ib()
    _req_id: str | None = attr.ib(default=None)

    _extra_headers: dict[dl_constants.DLHeaders, str] | None = attr.ib(default=None)

    @staticmethod
    def update_dl_headers(
        acc: dict[dl_constants.DLHeaders, str],
        update: dict[dl_constants.DLHeaders, str],
        stage: str,
    ) -> None:
        headers_intersection = acc.keys() & update.keys()
        if headers_intersection:
            LOGGER.warning(f"Got headers intersection on update on stage {stage!r}: {headers_intersection}")
        acc.update(update)

    # TODO FIX: Check if no overrides (take in account that headers are CI)
    @staticmethod
    def dl_headers_to_plain(
        dl_headers: dict[dl_constants.DLHeaders, str], extra_plain_headers: dict[str, str]
    ) -> dict[str, str]:
        result: dict[str, str] = {dl_header.value: value for dl_header, value in dl_headers.items()}
        result.update(extra_plain_headers)
        return result

    def get_common_headers(self, req_id: str) -> dict[dl_constants.DLHeaders, str]:
        result: dict[dl_constants.DLHeaders, str] = {}

        self.update_dl_headers(result, {dl_constants.DLHeadersCommon.REQUEST_ID: req_id}, "req_id")
        self.update_dl_headers(result, self._tenant.get_outbound_tenancy_headers(), "tenancy")
        self.update_dl_headers(result, self._auth_data.get_headers(), "auth")

        return result

    def get_effective_headers_for_request(self, rq: Req, req_id: str) -> dict[str, str]:
        tracing_headers = dl_api_commons.get_current_tracing_headers()

        result: dict[dl_constants.DLHeaders, str] = {}

        if rq.add_common_headers:
            self.update_dl_headers(result, self.get_common_headers(req_id), "common_headers")

        if rq.extra_headers:
            self.update_dl_headers(result, rq.extra_headers, "request_extra_headers")

        if self._extra_headers:
            self.update_dl_headers(result, self._extra_headers, "common_extra_headers")

        return self.dl_headers_to_plain(result, tracing_headers)

    # TODO FIX: Sanitize path
    def make_full_url(self, path: str) -> str:
        return f"{self._base_url.rstrip('/')}/{path.lstrip('/')}"

    async def make_request(self, rq: Req) -> Resp:
        if rq.data is not None and rq.data_json is not None:
            raise ValueError("data_json and data can not be provided simultaneously")

        req_id = uuid.uuid4().hex if self._req_id is None else self._req_id
        resp = await self._session.request(
            rq.method,
            self.make_full_url(rq.url),
            headers=self.get_effective_headers_for_request(rq, req_id),
            params=rq.params,
            json=rq.data_json,
            data=rq.data,
        )
        content = await resp.read()

        if resp.content_type == "application/json":
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

    async def close(self) -> None:
        await self._session.close()
