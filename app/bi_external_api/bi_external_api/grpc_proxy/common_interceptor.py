import abc
import logging
from typing import (
    Any,
    Callable,
    ClassVar,
    Tuple,
)

import grpc
import shortuuid

from bi_external_api.grpc_proxy.common import (
    GAuthorizationData,
    GRequestData,
)

LOGGER = logging.getLogger(__name__)


# Copy-paste from https://github.com/d5h-foss/grpc-interceptor/blob/master/src/grpc_interceptor/server.py
def _get_factory_and_method(
    rpc_handler: grpc.RpcMethodHandler,
) -> Tuple[Callable, Callable]:
    if rpc_handler.unary_unary:  # type: ignore
        return grpc.unary_unary_rpc_method_handler, rpc_handler.unary_unary  # type: ignore
    elif rpc_handler.unary_stream:  # type: ignore
        return grpc.unary_stream_rpc_method_handler, rpc_handler.unary_stream  # type: ignore
    elif rpc_handler.stream_unary:  # type: ignore
        return grpc.stream_unary_rpc_method_handler, rpc_handler.stream_unary  # type: ignore
    elif rpc_handler.stream_stream:  # type: ignore
        return grpc.stream_stream_rpc_method_handler, rpc_handler.stream_stream  # type: ignore
    else:  # pragma: no cover
        raise RuntimeError("RPC handler implementation does not exist")


# Copy-paste from https://github.com/d5h-foss/grpc-interceptor/blob/master/src/grpc_interceptor/server.py
class ServerInterceptor(grpc.ServerInterceptor, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def intercept(
        self,
        method: Callable,
        request_or_iterator: Any,
        context: grpc.ServicerContext,
        method_name: str,
    ) -> Any:  # pragma: no cover
        return method(request_or_iterator, context)

    def intercept_service(self, continuation, handler_call_details):  # type: ignore
        next_handler = continuation(handler_call_details)
        # Returns None if the method isn't implemented.
        if next_handler is None:
            return

        handler_factory, next_handler_method = _get_factory_and_method(next_handler)

        def invoke_intercept_method(request_or_iterator, context):  # type: ignore
            method_name = handler_call_details.method
            return self.intercept(
                next_handler_method,
                request_or_iterator,
                context,
                method_name,
            )

        return handler_factory(
            invoke_intercept_method,
            request_deserializer=next_handler.request_deserializer,
            response_serializer=next_handler.response_serializer,
        )


class RequestBootstrapInterceptor(ServerInterceptor):
    md_key_request_id: ClassVar[str] = "x-request-id"
    md_key_authorization: ClassVar[str] = "authorization"

    def intercept(
        self,
        method: Callable,
        request_or_iterator: Any,
        context: grpc.ServicerContext,
        method_name: str,
    ) -> Any:
        # Inbound request ID is currently ignored
        generated_request_id = f"gp.{shortuuid.uuid()}"

        # Sending effective request ID to client
        context.send_initial_metadata(
            (
                (
                    self.md_key_request_id,
                    generated_request_id,
                ),
            )
        )
        authorization_data = GAuthorizationData.from_invocation_metadata(context.invocation_metadata())

        GRequestData.set_request_id(context, generated_request_id)
        GRequestData.set_auth_data(context, authorization_data)

        try:
            return method(request_or_iterator, context)
        except Exception:
            LOGGER.exception("Error occurred during grpc call")
            raise
