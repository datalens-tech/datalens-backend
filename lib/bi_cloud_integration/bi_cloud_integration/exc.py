from __future__ import annotations

import contextlib
import logging
from typing import (
    Dict,
    Generator,
    Optional,
    Type,
)

import attr
import grpc

LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True, auto_attribs=True)
class DefaultYCExcInfo:
    # BI-local operation code to identify which step of flow is failed
    # e.g. sa_create, load_products
    operation_code: Optional[str] = None
    internal_details: Optional[str] = None


@attr.s
class YCBillingAPIError(Exception):
    message: Optional[str] = attr.ib(default="YC Billing API error")
    status_code: Optional[int] = attr.ib(default=None)


class YCException(Exception):
    def __init__(self, info: DefaultYCExcInfo):
        self.info = info
        super().__init__(info)


class YCUnauthenticated(YCException):
    pass


class YCPermissionDenied(YCException):
    pass


class YCBadRequest(YCException):
    pass


class YCResourceExhausted(YCException):
    pass


CODE_TO_SIMPLE_CLS: Dict[grpc.StatusCode, Type[YCException]] = {
    grpc.StatusCode.UNAUTHENTICATED: YCUnauthenticated,
    grpc.StatusCode.PERMISSION_DENIED: YCPermissionDenied,
    grpc.StatusCode.INVALID_ARGUMENT: YCBadRequest,
    grpc.StatusCode.RESOURCE_EXHAUSTED: YCResourceExhausted,
}


def get_grpc_code(exc: grpc.RpcError) -> Optional[grpc.StatusCode]:
    if hasattr(exc, "code") and callable(exc.code):  # type: ignore
        try:
            return exc.code()  # type: ignore
        except Exception:  # noqa
            LOGGER.warning("Can not get error code from GRPC exception", exc_info=True)
    return None


def get_grpc_details(exc: grpc.RpcError) -> Optional[str]:
    if hasattr(exc, "details") and callable(exc.details):  # type: ignore
        try:
            return exc.details()  # type: ignore
        except Exception:  # noqa
            LOGGER.warning("Can not get error code from GRPC exception", exc_info=True)
    return None


def handle_grpc_error(err: grpc.RpcError, operation_code: Optional[str] = None) -> None:
    internal_details = get_grpc_details(err)
    err_grpc_code = get_grpc_code(err)
    err_cls = CODE_TO_SIMPLE_CLS.get(err_grpc_code)  # type: ignore
    if err_cls is not None:
        raise err_cls(
            DefaultYCExcInfo(
                operation_code=operation_code,
                internal_details=internal_details,
            )
        ) from err


@contextlib.contextmanager
def grpc_exc_handler(operation_code: Optional[str] = None) -> Generator[None, None, None]:
    try:
        yield
    except grpc.RpcError as err:
        handle_grpc_error(err, operation_code=operation_code)
        raise
