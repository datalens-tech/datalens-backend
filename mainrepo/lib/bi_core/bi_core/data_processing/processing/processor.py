from __future__ import annotations

import abc
import logging
import time
from typing import (
    TYPE_CHECKING,
    Collection,
    Dict,
    List,
    Optional,
    TypeVar,
)

import attr
import shortuuid

from bi_api_commons.reporting.models import (
    DataProcessingEndReportingRecord,
    DataProcessingStartReportingRecord,
)
from bi_core.data_processing.processing.context import OpExecutionContext
from bi_core.data_processing.processing.operation import (
    BaseOp,
    MultiSourceOp,
    SingleSourceOp,
)
from bi_core.data_processing.stream_base import (
    AbstractStream,
    DataStreamAsync,
)

if TYPE_CHECKING:
    from bi_api_commons.reporting.registry import ReportingRegistry  # noqa
    from bi_core.services_registry import ServicesRegistry  # noqa


LOGGER = logging.getLogger(__name__)


_OP_PROC_TV = TypeVar("_OP_PROC_TV", bound="OperationProcessorAsyncBase")


class OperationProcessorAsyncBase(abc.ABC):
    @abc.abstractmethod
    async def ping(self) -> Optional[int]:
        """Check processor readiness"""

    async def execute_operation(self, op: BaseOp, ctx: OpExecutionContext) -> AbstractStream:
        raise NotImplementedError

    async def start(self) -> None:
        pass

    async def end(self) -> None:
        pass

    async def __aenter__(self: _OP_PROC_TV) -> _OP_PROC_TV:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):  # type: ignore
        await self.end()

    async def prepare_output_streams(self, output_streams: List[AbstractStream]) -> List[DataStreamAsync]:
        result: List[DataStreamAsync] = []
        for stream in output_streams:
            assert isinstance(
                stream, DataStreamAsync
            ), f"Expected DataStreamAsync, got {type(stream).__name__} ({stream.id}) as out stream"
            result.append(stream)

        return result

    def pre_run(self, ctx: OpExecutionContext) -> None:
        pass

    def post_run(self, ctx: OpExecutionContext, exec_exception: Optional[Exception]) -> None:
        pass

    def _validate_input_stream(self, stream: AbstractStream, op: BaseOp) -> None:
        if not isinstance(stream, op.supported_source_types):
            raise TypeError(
                f"Requested stream {stream.id} has type {type(stream)} "
                f"that is not supported for input by operation {type(op)}"
            )

    def _validate_output_stream(self, stream: AbstractStream, op: BaseOp) -> None:
        if not isinstance(stream, op.supported_dest_types):
            raise TypeError(
                f"Returned stream {stream.id} has type {type(stream)} "
                f"that is not supported for output by operation {type(op)}"
            )

    async def execute_operations(
        self,
        ctx: OpExecutionContext,
        output_stream_ids: Collection[str],
    ) -> List[DataStreamAsync]:
        ready_streams: Dict[str, AbstractStream] = {stream.id: stream for stream in ctx.streams}
        assert all(ready_streams.values())

        async def _make_output_streams_ready_recursive(req_out_stream_ids: Collection[str]) -> None:
            for out_stream_id in sorted(req_out_stream_ids):
                op = ctx.get_generating_operation(stream_id=out_stream_id)

                if isinstance(op, MultiSourceOp):
                    source_stream_ids = op.source_stream_ids
                else:
                    assert isinstance(op, SingleSourceOp)
                    source_stream_ids = {op.source_stream_id}

                if not source_stream_ids.issubset(set(ready_streams)):  # type: ignore  # TODO: fix
                    # Make these streams ready
                    await _make_output_streams_ready_recursive(source_stream_ids)

                # All streams are ready for this operation
                for source_stream_id in source_stream_ids:
                    source_stream = ctx.get_stream(stream_id=source_stream_id)
                    self._validate_input_stream(source_stream, op)  # type: ignore  # TODO: fix
                    if isinstance(source_stream, DataStreamAsync):  # These streams are not reusable
                        del ready_streams[source_stream_id]

                new_stream = await self.execute_operation(op=op, ctx=ctx)
                self._validate_output_stream(new_stream, op)
                ctx.add_stream(new_stream)
                ready_streams[new_stream.id] = new_stream

        await _make_output_streams_ready_recursive(output_stream_ids)
        LOGGER.info("Done processing operations")
        return [ready_streams[stream_id] for stream_id in sorted(output_stream_ids)]  # type: ignore

    async def run(
        self,
        operations: Collection[BaseOp],
        streams: Collection[AbstractStream],
        output_stream_ids: Collection[str],
    ) -> List[DataStreamAsync]:
        processing_id = shortuuid.uuid()
        LOGGER.info(f"Initializing processing context {processing_id}")
        ctx = OpExecutionContext(
            processing_id=processing_id,
            streams=streams,
            operations=operations,
        )

        self.pre_run(ctx=ctx)

        LOGGER.info(f'Processor got operations: {", ".join([type(op).__name__ for op in operations])}')

        exec_exception: Optional[Exception] = None
        try:
            # Execute and get output streams
            stream_list = await self.execute_operations(ctx=ctx, output_stream_ids=output_stream_ids)
            actual_output_stream_ids = {st.id for st in stream_list}
            assert actual_output_stream_ids == set(
                output_stream_ids
            ), f"Destination streams do not match: expected {output_stream_ids}, got {actual_output_stream_ids}"
            # Finalize streams
            result = await self.prepare_output_streams(output_streams=stream_list)  # type: ignore  # TODO: fix

        except Exception as fired_exception:
            exec_exception = fired_exception
            raise
        finally:
            self.post_run(ctx=ctx, exec_exception=exec_exception)

        return result


@attr.s
class SROperationProcessorAsyncBase(OperationProcessorAsyncBase):
    _service_registry: "ServicesRegistry" = attr.ib(default=None, kw_only=True)  # Service registry override

    @property
    def service_registry(self) -> "ServicesRegistry":
        if self._service_registry is not None:
            return self._service_registry

    @property
    def _reporting_registry(self) -> "ReportingRegistry":
        return self.service_registry.get_reporting_registry()

    def _save_start_exec_reporting_record(self, ctx: OpExecutionContext) -> None:
        report = DataProcessingStartReportingRecord(
            timestamp=time.time(),
            processing_id=ctx.processing_id,
            source_query_ids=tuple(
                in_stream.meta.query_id for in_stream in ctx.data_streams if in_stream.meta.query_id is not None
            ),  # type: ignore
        )
        self._reporting_registry.save_reporting_record(report=report)

    def _save_end_exec_reporting_record(
        self,
        ctx: OpExecutionContext,
        exec_exception: Optional[Exception],
    ) -> None:
        report = DataProcessingEndReportingRecord(
            timestamp=time.time(),
            processing_id=ctx.processing_id,
            exception=exec_exception,
        )
        self._reporting_registry.save_reporting_record(report=report)

    def pre_run(self, ctx: OpExecutionContext) -> None:
        self._save_start_exec_reporting_record(ctx=ctx)

    def post_run(self, ctx: OpExecutionContext, exec_exception: Optional[Exception]) -> None:
        self._save_end_exec_reporting_record(ctx=ctx, exec_exception=exec_exception)
