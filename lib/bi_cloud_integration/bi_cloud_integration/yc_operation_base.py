from __future__ import annotations

import abc
import time
from typing import TYPE_CHECKING, Generic, TypeVar, ClassVar, final, Any, Sequence, Type, Optional

from yandex.cloud.priv.operation import operation_pb2

from bi_cloud_integration.model import Operation, OperationError
from bi_cloud_integration.yc_client_base import DLYCSingleServiceClient

if TYPE_CHECKING:
    from google.rpc import status_pb2

_OP_TYPE_TV = TypeVar("_OP_TYPE_TV", bound=Operation)


class OperationConverterBase(Generic[_OP_TYPE_TV], metaclass=abc.ABCMeta):
    operation_cls: ClassVar[Type[_OP_TYPE_TV]] = Operation  # type: ignore  # TODO: fix

    @final
    def convert_operation(self, grpc_op: operation_pb2.Operation) -> _OP_TYPE_TV:  # type: ignore  # TODO: fix
        op_error: Optional[OperationError]

        if grpc_op.HasField('error'):  # type: ignore  # TODO: fix
            op_error = self._convert_error(grpc_op.error)  # type: ignore  # TODO: fix
        else:
            op_error = None

        op_response: Any
        if grpc_op.HasField('response'):  # type: ignore  # TODO: fix
            op_response = self._convert_response(grpc_op.response)  # type: ignore  # TODO: fix
        else:
            op_response = None

        # TODO FIX: BI-2161 Validate operation response type against type in operation_cls
        # noinspection PyArgumentList
        return self.operation_cls(
            id=grpc_op.id,  # type: ignore  # TODO: fix
            description=grpc_op.description,  # type: ignore  # TODO: fix
            done=grpc_op.done,  # type: ignore  # TODO: fix
            error=op_error,
            response=op_response,
        )  # type: ignore

    @final
    def _convert_error(self, grpc_status: status_pb2.Status) -> OperationError:  # type: ignore  # TODO: fix
        return OperationError(
            code=grpc_status.code,  # type: ignore  # TODO: fix
            message=grpc_status.message,  # type: ignore  # TODO: fix
            details=self._convert_details(grpc_status.details)  # type: ignore  # TODO: fix
        )

    @abc.abstractmethod
    def _convert_details(self, details: Sequence[Any]) -> Sequence[Any]:
        raise NotImplementedError()

    @abc.abstractmethod
    def _convert_response(self, grpc_response: Any) -> Any:
        raise NotImplementedError()


class NoResponseOpConverter(OperationConverterBase[Operation]):
    """This converter will ignore response from GRPC operation"""

    def _convert_details(self, details: Sequence[Any]) -> Sequence[Any]:
        return tuple(repr(single_detail) for single_detail in details)

    def _convert_response(self, grpc_response: Any) -> Any:
        return None


class YCOperationException(Exception):
    def __init__(self, operation: Operation):
        self.operation = operation


class YCOperationFailed(YCOperationException):
    pass


class YCOperationAwaitTimeout(YCOperationException):
    pass


_OP_TYPE_2_TV = TypeVar("_OP_TYPE_2_TV", bound=Operation)


class DLGenericOperationService(DLYCSingleServiceClient, Generic[_OP_TYPE_2_TV]):
    def _get_op_converter(self) -> OperationConverterBase[_OP_TYPE_2_TV]:
        return NoResponseOpConverter()  # type: ignore  # TODO: fix

    def _execute_get_operation_request(self, operation_id: str) -> Any:
        return self.service.Get(
            self._create_get_operation_request(operation_id)
        )

    def _create_get_operation_request(self, operation_id: str) -> Any:
        raise NotImplementedError()

    @final
    def get_operation_sync(self, operation_id: str) -> _OP_TYPE_2_TV:
        op_converter = self._get_op_converter()
        grpc_op = self._execute_get_operation_request(operation_id)
        return op_converter.convert_operation(grpc_op)

    @final
    def wait_for_operation_sync(
            self,
            initial_operation: Optional[_OP_TYPE_2_TV] = None,
            operation_id: Optional[str] = None,
            poll_interval: float = 0.1,
            timeout: float = 60,
            raise_on_operation_fail: bool = True,
    ) -> _OP_TYPE_2_TV:
        assert initial_operation is not None or operation_id is not None
        start_time = time.monotonic()

        op: _OP_TYPE_2_TV

        if initial_operation is None:
            op = self.get_operation_sync(operation_id)  # type: ignore  # TODO: fix
        else:
            op = initial_operation

        while not op.done and op.error is None:
            if time.monotonic() > start_time + timeout:
                raise YCOperationAwaitTimeout(
                    operation=op,
                )

            time.sleep(poll_interval)
            op = self.get_operation_sync(op.id)

        if raise_on_operation_fail and op.error is not None:
            raise YCOperationFailed(operation=op)

        return op
