from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import (
    AsyncGenerator,
    Iterable,
    NoReturn,
    Optional,
    Union,
)

import aiohttp

from dl_api_commons.aiohttp.aiohttp_client import (
    BIAioHTTPClient,
    PredefinedIntervalsRetrier,
)
from dl_api_commons.tracing import get_current_tracing_headers
from dl_app_tools.profiling_base import GenericProfiler
from dl_core.base_models import EntryLocation
from dl_core.exc import (
    USLockUnacquiredException,
    USReqException,
)
from dl_core.united_storage_client import (
    USAuthContextBase,
    USClientHTTPExceptionWrapper,
    UStorageClientBase,
)


LOGGER = logging.getLogger(__name__)


class UStorageClientAIO(UStorageClientBase):
    class ResponseAdapter(UStorageClientBase.ResponseAdapter):
        def __init__(
            self,
            response: aiohttp.ClientResponse,
            content: bytes,
            request_data: UStorageClientBase.RequestData,
            elapsed_seconds: float,
        ):
            self._response = response
            self._content = content
            self._request_data = request_data
            self._elapsed_seconds = elapsed_seconds

            self._parsed_json_data = None
            self._request_adapter = UStorageClientAIO.RequestAdapter(response.request_info, request_data)

        @property
        def status_code(self) -> int:
            return self._response.status

        def get_header(self, name: str) -> Optional[str]:
            return self._response.headers.get(name)

        @property
        def elapsed_seconds(self) -> float:  # type: ignore  # TODO: fix
            return self._elapsed_seconds

        @property
        def content(self) -> bytes:
            return self._content

        @property
        def request(self) -> "UStorageClientBase.RequestAdapter":
            return self._request_adapter

        def raise_for_status(self) -> None:
            try:
                self._response.raise_for_status()
            except aiohttp.ClientResponseError as err:
                if err.status is not None:
                    raise USClientHTTPExceptionWrapper(str(err)) from err
                raise

        def json(self) -> dict:
            if self._parsed_json_data is None:
                self._parsed_json_data = json.loads(self._content.decode("utf-8"))
            return self._parsed_json_data  # type: ignore  # TODO: fix

    class RequestAdapter(UStorageClientBase.RequestAdapter):
        def __init__(self, request: aiohttp.RequestInfo, request_data: UStorageClientBase.RequestData):
            self._request = request
            self._request_data = request_data

        @property
        def relative_url(self) -> str:
            return self._request_data.relative_url

        @property
        def method(self) -> str:
            return self._request_data.method

        def get_header(self, name: str) -> Optional[str]:
            return self._request.headers.get(name)

        @property
        def json(self) -> dict:
            return self._request_data.json  # type: ignore  # TODO: fix

    _bi_http_client: BIAioHTTPClient

    def __init__(
        self,
        host: str,
        prefix: Optional[str],
        auth_ctx: USAuthContextBase,
        ca_data: bytes,
        context_request_id: Optional[str] = None,
        context_forwarded_for: Optional[str] = None,
        context_workbook_id: Optional[str] = None,
        timeout: int = 30,
    ):
        super().__init__(
            host=host,
            auth_ctx=auth_ctx,
            prefix=prefix,
            timeout=timeout,
            context_request_id=context_request_id,
            context_forwarded_for=context_forwarded_for,
            context_workbook_id=context_workbook_id,
        )

        self._retry_intervals = (0.5, 1.0, 1.1, 2.0, 2.2)
        self._retry_codes = {408, 429, 500, 502, 503, 504}

        self._bi_http_client = BIAioHTTPClient(
            base_url="/".join([self.host, self.prefix]),
            retrier=PredefinedIntervalsRetrier(
                retry_intervals=self._retry_intervals,
                retry_codes=self._retry_codes,
                retry_methods={"GET", "POST", "PUT", "DELETE"},  # TODO: really retry all of them?..
            ),
            headers=self._default_headers,
            cookies=self._cookies,
            raise_for_status=False,
            ca_data=ca_data,
        )

    async def _request(self, request_data: UStorageClientBase.RequestData):  # type: ignore  # TODO: fix
        self._raise_for_disabled_interactions()
        self._log_request_start(request_data)
        tracing_headers = get_current_tracing_headers()
        start = time.monotonic()

        with GenericProfiler("us-client-request"):
            async with self._bi_http_client.request(
                method=request_data.method,
                path=request_data.relative_url,
                params=request_data.params,
                json_data=request_data.json,
                read_timeout_sec=self.timeout,  # TODO: total timeout
                conn_timeout_sec=1,
                headers={
                    **self._extra_headers,
                    **tracing_headers,
                },
            ) as response:
                content = await response.read()

        end = time.monotonic()

        response_adapter = self.ResponseAdapter(
            response=response,
            content=content,
            request_data=request_data,
            elapsed_seconds=(end - start),
        )
        return self._get_us_json_from_response(response_adapter)

    async def get_entry(self, entry_id: str) -> dict:
        return await self._request(self._req_data_get_entry(entry_id=entry_id))

    async def create_entry(  # type: ignore  # TODO: fix
        self,
        key: EntryLocation,
        scope: str,
        meta=None,
        data=None,
        unversioned_data=None,
        type_=None,
        hidden=None,
        links=None,
        **kwargs,
    ):
        rq_data = self._req_data_create_entry(
            key=key,
            scope=scope,
            meta=meta,
            data=data,
            unversioned_data=unversioned_data,
            type_=type_,
            hidden=hidden,
            links=links,
            **kwargs,
        )
        return await self._request(rq_data)

    async def update_entry(  # type: ignore  # TODO: fix
        self, entry_id, data=None, unversioned_data=None, meta=None, lock=None, hidden=None, links=None
    ):
        return await self._request(
            self._req_data_update_entry(
                entry_id,
                data=data,
                unversioned_data=unversioned_data,
                meta=meta,
                lock=lock,
                hidden=hidden,
                links=links,
            )
        )

    async def delete_entry(self, entry_id, lock=None) -> NoReturn:  # type: ignore  # TODO: fix
        await self._request(self._req_data_delete_entry(entry_id, lock=lock))

    async def entries_iterator(
        self,
        scope: str,
        entry_type: Optional[str] = None,
        meta: Optional[dict] = None,
        all_tenants: bool = False,
        include_data: bool = False,
        ids: Optional[Iterable[str]] = None,
        creation_time: Optional[dict[str, Union[str, int, None]]] = None,
    ) -> AsyncGenerator[dict, None]:
        page = 0
        while True:
            resp = await self._request(
                self._req_data_iter_entries(
                    scope,
                    entry_type=entry_type,
                    meta=meta,
                    all_tenants=all_tenants,
                    page=page,
                    include_data=include_data,
                    ids=ids,
                    creation_time=creation_time,
                )
            )
            if resp.get("entries"):
                page_entries = resp["entries"]
            else:
                break

            for entr in page_entries:
                yield entr

            if resp.get("nextPageToken"):
                page = resp["nextPageToken"]
            else:
                break

    async def list_all_entries(
        self, scope: str, entry_type: Optional[str] = None, meta: Optional[dict] = None, all_tenants: bool = False
    ) -> list:
        ret = []
        async for e in self.entries_iterator(scope, entry_type, meta, all_tenants, include_data=False):
            ret.append(e)

        return ret

    async def acquire_lock(
        self,
        entry_id: str,
        duration: Optional[int] = None,
        wait_timeout: Optional[int] = None,
        force: Optional[bool] = None,
    ) -> str:
        """
        :param entry_id: US entry ID to lock
        :param duration: Duration of acquired lock
        :param wait_timeout: Acquire attempts timeout in seconds
        :param force: Force acquire lock without wait
        :return: Lock token
        """
        req_data = self._req_data_acquire_lock(entry_id, duration=duration, force=force)
        start_ts = time.time()
        while True:
            try:
                resp = await self._request(req_data)
                lock = resp["lockToken"]
                LOGGER.info('Acquired lock "%s" for object "%s"', lock, entry_id)
                return lock
            except USLockUnacquiredException:
                if wait_timeout and time.time() - start_ts < wait_timeout:
                    await asyncio.sleep(0.25)
                else:
                    raise

    async def release_lock(self, entry_id, lock):  # type: ignore  # TODO: fix
        try:
            await self._request(self._req_data_release_lock(entry_id, lock=lock))
        except USReqException:
            LOGGER.exception('Unable to release lock "%s"', lock)

    async def close(self):  # type: ignore  # TODO: fix
        await self._bi_http_client.close()
