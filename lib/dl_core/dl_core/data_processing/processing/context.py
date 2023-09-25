from __future__ import annotations

from typing import (
    Any,
    Collection,
    Dict,
    Optional,
    Set,
    Tuple,
)

from dl_core.data_processing.processing.operation import (
    BaseOp,
    MultiSourceOp,
    SingleSourceOp,
)
from dl_core.data_processing.stream_base import (
    AbstractStream,
    DataStreamAsync,
)


class OpExecutionContext:
    _processing_id: str
    _streams: Dict[str, AbstractStream]
    _operations: Tuple[BaseOp, ...]

    def __init__(self, processing_id: str, streams: Collection[AbstractStream], operations: Collection[BaseOp]):
        self._processing_id = processing_id
        self._streams = {stream.id: stream for stream in streams}
        self._operations = tuple(op for op in operations)
        self._operations_by_output: Dict[str, BaseOp] = {}

        for op in operations:
            self._operations_by_output[op.dest_stream_id] = op

    @property
    def operations(self) -> Tuple[BaseOp, ...]:
        return self._operations

    @property
    def streams(self) -> Collection[AbstractStream]:
        return [stream for stream in self._streams.values()]

    @property
    def data_streams(self) -> Collection[DataStreamAsync]:
        return [stream for stream in self._streams.values() if isinstance(stream, DataStreamAsync)]

    @property
    def processing_id(self) -> str:
        return self._processing_id

    def get_stream(self, stream_id: str) -> Optional[AbstractStream]:
        return self._streams.get(stream_id)

    def add_stream(self, stream: AbstractStream) -> None:
        self._streams[stream.id] = stream

    def remove_stream(self, stream: AbstractStream) -> None:
        del self._streams[stream.id]

    def get_stream_ids_for_operation(self, op: BaseOp) -> Set[str]:
        result = set()
        if isinstance(op, SingleSourceOp):
            result.add(op.source_stream_id)
        elif isinstance(op, MultiSourceOp):
            for src_id in op.source_stream_ids:
                result.add(src_id)

        return result

    def get_generating_operation(self, stream_id: str) -> BaseOp:
        """Return the operation that generates the data stream with the given ID"""
        return self._operations_by_output[stream_id]

    def clone(self, **kwargs: Any) -> OpExecutionContext:
        new_kwargs = dict(
            processing_id=self._processing_id,
            streams=self._streams.values(),
            operations=self._operations,
        )

        for key, val in kwargs.items():
            if key not in new_kwargs:
                raise ValueError(f"Unknown kwarg: {key!r}")
            new_kwargs[key] = val

        return self.__class__(**new_kwargs)  # type: ignore
