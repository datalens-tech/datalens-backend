from __future__ import annotations

import asyncio
import logging
import time
from typing import (
    Any,
    AsyncIterable,
    Optional,
    Sequence,
    Tuple,
)
import uuid

import attr
from ydb.convert import ResultSet
from ydb.dbapi.cursor import get_column_type
from ydb.public.api.grpc.draft.fq_v1_pb2_grpc import FederatedQueryServiceStub
import ydb.public.api.protos.draft.fq_pb2 as yqpb
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.table import TableClientSettings

from bi_cloud_integration.yc_client_base import (
    DLYCServiceConfig,
    DLYCSingleServiceClient,
)
from bi_cloud_integration.yc_ts_client import get_yc_service_token
from bi_sqlalchemy_yq.errors import YQError
from dl_utils.aio import await_sync

TColumnDesc = Tuple[str, str]  # name, type_name
TColumns = Tuple[TColumnDesc, ...]
TResultData = Tuple[TColumns, Sequence[dict]]

LOGGER = logging.getLogger(__name__)


@attr.s(frozen=False)
class DLYQClient(DLYCSingleServiceClient):
    database_name: str = attr.ib(kw_only=True)
    cloud_id: str = attr.ib(kw_only=True)
    folder_id: str = attr.ib(kw_only=True)

    query_poll_pause: float = attr.ib(default=0.678)
    query_total_timeout: float = attr.ib(default=60.0)

    service_cls = FederatedQueryServiceStub

    @classmethod
    def create(  # type: ignore  # `Signature of "create" incompatible with supertype "DLYCClientCommon"`
        cls,
        endpoint: str,
        request_id: Optional[str] = None,
        svc_kwargs: Optional[dict] = None,
        **kwargs: Any,
    ) -> DLYQClient:
        svc_kwargs = dict(svc_kwargs or {})
        svc_kwargs.setdefault("tls", None)
        service_config = DLYCServiceConfig(endpoint=endpoint, **svc_kwargs)
        return cls(
            service_config=service_config,
            request_id=request_id,
            **kwargs,
        )

    @property
    def _default_metadata(self):
        bearer_token = self.bearer_token
        if not bearer_token:
            raise Exception("Need to `.ensure_fresh_token()` first")

        # Note: this also includes the `authorization: Bearer {bearer_token}` metadata.
        # Maybe will have want to remove it here.
        sup = super()._default_metadata

        return sup + (
            ("x-ydb-auth-ticket", bearer_token),
            ("x-ydb-database", self.database_name),
            ("x-ydb-fq-project", f"yandexcloud://{self.folder_id}"),
        )

    async def ensure_fresh_token(self, yc_ts_endpoint: str, key_data: dict) -> DLYQClient:
        token = await get_yc_service_token(
            key_data=key_data,
            yc_ts_endpoint=yc_ts_endpoint,
        )
        return attr.evolve(self, bearer_token=token)

    def _raise_for_status(self, resp: Any) -> None:
        if resp.operation.status != StatusIds.SUCCESS:  # type: ignore  # `"GeneratedProtocolMessageType" has no attribute "SUCCESS"`
            raise YQError("Non-ok response", resp)

    def _unpack(self, value: Any, proto: Any, resp: Any) -> Any:
        if not value.Is(proto.DESCRIPTOR):
            raise YQError("Unexpected response content", resp or value)
        result = proto()
        value.Unpack(result)
        return result

    async def create_query(
        self,
        query: str,
        name: str = "DataLens YQ query",
    ) -> str:
        idempotency_key = str(uuid.uuid4())

        request = yqpb.CreateQueryRequest()
        request.execute_mode = yqpb.ExecuteMode.RUN  # type: ignore  # `"EnumTypeWrapper" has no attribute "RUN"`
        request.content.type = request.content.ANALYTICS
        request.content.name = name
        request.content.text = query
        request.content.acl.visibility = request.content.acl.PRIVATE
        request.content.automatic = True
        request.idempotency_key = idempotency_key

        resp = await self.service.CreateQuery.aio(request)
        self._raise_for_status(resp)

        create_res = self._unpack(value=resp.operation.result, proto=yqpb.CreateQueryResult, resp=resp)
        query_id = create_res.query_id
        return query_id

    async def describe_query_str(self, query_id: str) -> str:
        req = yqpb.DescribeQueryRequest()
        req.query_id = query_id
        resp = await self.service.DescribeQuery.aio(req)
        self._raise_for_status(resp)
        describe_res = self._unpack(value=resp.operation.result, proto=yqpb.DescribeQueryResult, resp=resp)
        return str(describe_res)

    async def query_status(self, query_id: str) -> Tuple[bool, Optional[str], Any]:
        """(query_id) -> (is_done, errors)"""
        req = yqpb.GetQueryStatusRequest()
        req.query_id = query_id
        resp = await self.service.GetQueryStatus.aio(req)
        self._raise_for_status(resp)

        status_res = self._unpack(value=resp.operation.result, proto=yqpb.GetQueryStatusResult, resp=resp)

        if status_res.status == yqpb.QueryMeta.RUNNING:  # type: ignore  # `"GeneratedProtocolMessageType" has no attribute "RUNNING"`
            return False, None, status_res

        if status_res.status == yqpb.QueryMeta.STARTING:  # type: ignore  # `"GeneratedProtocolMessageType" has no attribute "STARTING"`
            return False, None, status_res

        if status_res.status == yqpb.QueryMeta.COMPLETING:  # type: ignore  # `"GeneratedProtocolMessageType" has no attribute "COMPLETING"`
            return False, None, status_res

        if status_res.status == yqpb.QueryMeta.COMPLETED:  # type: ignore  # `"GeneratedProtocolMessageType" has no attribute "COMPLETED"`
            return True, None, status_res

        errors = await self.describe_query_str(query_id)
        return False, errors, status_res

    async def wait_for_query(self, query_id: str) -> Any:
        max_time = time.monotonic() + self.query_total_timeout
        status_res = None
        while True:
            if max_time - time.monotonic() < self.query_poll_pause:
                raise YQError("Timed out waiting for query to complete")
            is_done, errors, status_res = await self.query_status(query_id)
            if errors is not None:
                raise YQError(errors)
            if is_done:
                break
            await asyncio.sleep(self.query_poll_pause)
        return status_res

    async def get_data(
        self,
        query_id: str,
        offset: int = 0,
        limit: int = 100,
        result_set_index: int = 0,
    ) -> TResultData:
        req = yqpb.GetResultDataRequest(
            query_id=query_id,
            result_set_index=result_set_index,  # in range(len(describe_res.query.result_set_meta))
            # See also: `describe_res.query.result_set_meta[0].rows_count`
            offset=offset,  # WARNING: might not work yet.
            limit=limit,
        )
        resp = await self.service.GetResultData.aio(req)
        self._raise_for_status(resp)

        data_res = self._unpack(value=resp.operation.result, proto=yqpb.GetResultDataResult, resp=resp)

        rs = ResultSet.from_message(
            data_res.result_set,
            (TableClientSettings().with_native_date_in_result_sets(True).with_native_datetime_in_result_sets(True)),
        )
        columns = tuple((str(col.name), str(get_column_type(col.type))) for col in rs.columns)
        rows = rs.rows
        return columns, rows

    async def get_data_chunks(self, query_id: str, chunk_size: int = 100) -> AsyncIterable[TResultData]:
        offset = 0
        while True:
            columns, rows = await self.get_data(
                query_id=query_id,
                offset=offset,
                limit=chunk_size,
            )
            if not rows:
                break
            yield columns, rows
            offset += chunk_size

    async def delete_query(self, query_id: str, require: bool = True) -> Any:
        req = yqpb.DeleteQueryRequest(query_id=query_id)
        resp = await self.service.DeleteQuery.aio(req)
        if require:
            self._raise_for_status(resp)
        return resp

    async def run_query(self, query: str, delete_query: bool = False) -> TResultData:
        query_id = await self.create_query(query)
        try:
            LOGGER.debug("Waiting for query %r to complete", query_id)
            await self.wait_for_query(query_id)
            return await self.get_data(query_id)
        finally:
            if delete_query:
                try:
                    await self.delete_query(query_id)
                except BaseException as exc:
                    LOGGER.exception("Error on cleanup of query %r: %r", query_id, exc)

    async def run_query_get_chunks(
        self, query: str, chunk_size: int = 1000, delete_query: bool = False
    ) -> AsyncIterable[TResultData]:
        query_id = await self.create_query(query)
        try:
            LOGGER.debug("Waiting for query %r to complete", query_id)
            await self.wait_for_query(query_id)
            async for chunk in self.get_data_chunks(query_id, chunk_size=chunk_size):
                yield chunk
        finally:
            if delete_query:
                try:
                    await self.delete_query(query_id)
                except BaseException as exc:
                    LOGGER.exception("Error on cleanup of query %r: %r", query_id, exc)

    def run_query_get_chunks_sync(self, query: str, chunk_size: int = 1000) -> Sequence[TResultData]:
        async def work() -> Sequence[TResultData]:
            chunks_gen = self.run_query_get_chunks(query, chunk_size=chunk_size)
            result = []
            async for chunk in chunks_gen:
                result.append(chunk)
            return result

        return await_sync(work())
