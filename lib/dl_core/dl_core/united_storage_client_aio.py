from __future__ import annotations

import asyncio
from datetime import (
    datetime,
    timedelta,
)
import json
import logging
import time
from typing import (
    Any,
    AsyncGenerator,
    Iterable,
    Optional,
    Union,
)

import aiohttp

from dl_api_commons.aiohttp.aiohttp_client import BIAioHTTPClient
from dl_api_commons.retrier.aiohttp import AiohttpPolicyRetrier
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
import dl_retrier


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
        def elapsed_seconds(self) -> float:
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
            assert self._parsed_json_data is not None
            return self._parsed_json_data

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
        def json(self) -> Optional[dict]:
            return self._request_data.json

    _bi_http_client: BIAioHTTPClient

    def __init__(
        self,
        host: str,
        prefix: Optional[str],
        auth_ctx: USAuthContextBase,
        ca_data: bytes,
        retry_policy_factory: dl_retrier.BaseRetryPolicyFactory,
        context_request_id: Optional[str] = None,
        context_forwarded_for: Optional[str] = None,
        context_workbook_id: Optional[str] = None,
    ):
        super().__init__(
            host=host,
            auth_ctx=auth_ctx,
            prefix=prefix,
            retry_policy_factory=retry_policy_factory,
            context_request_id=context_request_id,
            context_forwarded_for=context_forwarded_for,
            context_workbook_id=context_workbook_id,
        )

        self._bi_http_client = BIAioHTTPClient(
            base_url="/".join([self.host, self.prefix]),
            headers=self._default_headers,
            cookies=self._cookies,
            raise_for_status=False,
            ca_data=ca_data,
        )

    async def _request(
        self,
        request_data: UStorageClientBase.RequestData,
        retry_policy_name: Optional[str] = None,
    ) -> dict:
        self._raise_for_disabled_interactions()
        self._log_request_start(request_data)
        tracing_headers = get_current_tracing_headers()
        start = time.monotonic()

        with GenericProfiler("us-client-request"):
            retry_policy = self._retry_policy_factory.get_policy(retry_policy_name)
            async with self._bi_http_client.request(
                method=request_data.method,
                path=request_data.relative_url,
                params=request_data.params,
                json_data=request_data.json,
                headers={
                    **self._extra_headers,
                    **tracing_headers,
                },
                retrier=AiohttpPolicyRetrier(
                    retry_policy=retry_policy,
                ),
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

    async def get_entry(
        self,
        entry_id: str,
        params: Optional[dict[str, str]] = None,
        include_permissions: bool = True,
        include_links: bool = True,
        include_favorite: bool = False,
    ) -> dict:
        return await self._request(
            self._req_data_get_entry(
                entry_id=entry_id,
                params=params,
                include_permissions=include_permissions,
                include_links=include_links,
                include_favorite=include_favorite,
            ),
            retry_policy_name="get_entry",
        )

    async def create_entry(
        self,
        key: EntryLocation,
        scope: str,
        meta: Optional[dict[str, str]] = None,
        data: Optional[dict[str, Any]] = None,
        unversioned_data: Optional[dict[str, Any]] = None,
        type_: Optional[str] = None,
        hidden: Optional[bool] = None,
        links: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
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
        return await self._request(
            rq_data,
            retry_policy_name="create_entry",
        )

    async def update_entry(
        self,
        entry_id: str,
        data: Optional[dict[str, Any]] = None,
        unversioned_data: Optional[dict[str, Any]] = None,
        meta: Optional[dict[str, str]] = None,
        lock: Optional[str] = None,
        hidden: Optional[bool] = None,
        links: Optional[dict[str, Any]] = None,
        update_revision: Optional[bool] = None,
    ) -> dict[str, Any]:
        return await self._request(
            self._req_data_update_entry(
                entry_id,
                data=data,
                unversioned_data=unversioned_data,
                meta=meta,
                lock=lock,
                hidden=hidden,
                links=links,
                update_revision=update_revision,
            ),
            retry_policy_name="update_entry",
        )

    async def delete_entry(self, entry_id: str, lock: Optional[str] = None) -> None:
        await self._request(
            self._req_data_delete_entry(entry_id, lock=lock),
            retry_policy_name="delete_entry",
        )

    async def entries_iterator(
        self,
        scope: str,
        entry_type: Optional[str] = None,
        meta: Optional[dict] = None,
        all_tenants: bool = False,
        include_data: bool = False,
        ids: Optional[Iterable[str]] = None,
        creation_time: Optional[dict[str, Union[str, int, None]]] = None,
        limit: Optional[int] = None,
    ) -> AsyncGenerator[dict, None]:
        """
        implements 2-in-1 pagination:
        - by page number (in this case entries are returned from the US along with a nextPageToken)
        - by creation time (entries are returned as a list ordered by creation time)

        :param scope:
        :param entry_type:
        :param meta: Filter entries by "meta" section values.
        :param all_tenants: Look up across all tenants. False by default.
        :param include_data: Return full US entry data. False by default.
        :param ids: Filter entries by uuid.
        :param creation_time: Filter entries by creation_time. Available filters: eq, ne, gt, gte, lt, lte
        :return:
        """
        created_at_from: datetime = datetime(1970, 1, 1)  # for creation time pagination
        previous_page_entry_ids = set()  # for deduplication
        page: int = 0  # for page number pagination

        done = False
        while not done:
            # 1. Prepare and make request
            created_at_from_ts = created_at_from.timestamp()
            unseen_entry_ids = set()
            resp = await self._request(
                self._req_data_iter_entries(
                    scope,
                    entry_type=entry_type,
                    meta=meta,
                    all_tenants=all_tenants,
                    include_data=include_data,
                    ids=ids,
                    creation_time=creation_time,
                    page=page,
                    created_at_from=created_at_from_ts,
                    limit=limit,
                ),
                retry_policy_name="entries_iterator",
            )

            # 2. Deal with pagination
            page_entries: list
            if isinstance(resp, list):
                page_entries = resp
                if page_entries:
                    created_at_from = self.parse_datetime(page_entries[-1]["createdAt"]) - timedelta(milliseconds=1)
                    # minus 1 ms to account for cases where entries, created during a single millisecond, happen to
                    # return on the border of two batches (one in batch 1 and the other in batch 2),
                    # hence the deduplication
                else:
                    LOGGER.info("Got an empty entries list in the US response, the listing is completed")
                    done = True
            else:
                page_entries = resp.get("entries", [])
                if resp.get("nextPageToken"):
                    page = resp["nextPageToken"]
                else:
                    LOGGER.info("Got no nextPageToken in the US response, the listing is completed")
                    done = True

            # 3. Yield results
            for entry in page_entries:
                if entry["entryId"] not in previous_page_entry_ids:
                    unseen_entry_ids.add(entry["entryId"])
                    yield entry

            # 4. Stop if got no nextPageToken or unseen entries
            previous_page_entry_ids = unseen_entry_ids.copy()
            if not unseen_entry_ids:
                LOGGER.info("US response is not empty, but we got no unseen entries, assuming the listing is completed")
                done = True

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
                resp = await self._request(
                    req_data,
                    retry_policy_name="acquire_lock",
                )
                lock = resp["lockToken"]
                LOGGER.info('Acquired lock "%s" for object "%s"', lock, entry_id)
                return lock
            except USLockUnacquiredException:
                if wait_timeout and time.time() - start_ts < wait_timeout:
                    await asyncio.sleep(0.25)
                else:
                    raise

    async def release_lock(self, entry_id: str, lock: str) -> None:
        try:
            await self._request(
                self._req_data_release_lock(entry_id, lock=lock),
                retry_policy_name="release_lock",
            )
        except USReqException:
            LOGGER.exception('Unable to release lock "%s"', lock)

    async def close(self) -> None:
        await self._bi_http_client.close()

    async def get_entry_revisions(self, entry_id: str) -> list[dict[str, Any]]:
        resp = await self._request(
            self._req_data_entry_revisions(entry_id),
            retry_policy_name="get_entry_revisions",
        )
        return resp["entries"]
