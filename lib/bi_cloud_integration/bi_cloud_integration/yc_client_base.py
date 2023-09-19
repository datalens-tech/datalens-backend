from __future__ import annotations

import abc
import asyncio
import functools
import itertools
import logging
import random
import time
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    FrozenSet,
    Iterable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)
from urllib.parse import unquote
import uuid

import attr
import grpc

from bi_cloud_integration.exc import (
    get_grpc_code,
    handle_grpc_error,
)
from dl_configs.utils import get_root_certificates
from dl_utils.aio import await_sync


LOGGER = logging.getLogger(__name__)
GRPC_PREFIX = "grpc://"
GRPCS_PREFIX = "grpcs://"


class Unspecified:
    """Special class for the `unspecified value` singleton marker"""


UNSPECIFIED = Unspecified()


_VAL_TV = TypeVar("_VAL_TV")


def get_if_specified(value: Union[_VAL_TV, Unspecified], default: _VAL_TV) -> _VAL_TV:
    if value is UNSPECIFIED or isinstance(value, Unspecified):
        return default
    result: _VAL_TV = value
    return result


ServiceType = Callable[[grpc.Channel], Any]


class DLYCRetryPolicyBase:
    call_timeout: float
    total_timeout: float
    min_timeout: float = 0.1

    @abc.abstractmethod
    def gen_exponential_backoff(self) -> Iterable[float]:
        raise NotImplementedError

    @abc.abstractmethod
    def can_retry_err(self, err: grpc.RpcError) -> bool:
        raise NotImplementedError


@attr.s(auto_attribs=True, frozen=True)
class DLYCRetryPolicy(DLYCRetryPolicyBase):
    call_timeout: float = 60.0
    total_timeout: float = 66.0
    min_timeout: float = 0.1

    initial_backoff: float = 0.1
    max_backoff: float = 3
    backoff_multiplier: float = 2

    retryable_status_codes: FrozenSet[grpc.StatusCode] = frozenset(
        (
            grpc.StatusCode.ABORTED,
            grpc.StatusCode.CANCELLED,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.FAILED_PRECONDITION,
        )
    )

    def gen_exponential_backoff(self) -> Iterable[float]:
        for idx in itertools.count():
            yield min(
                self.max_backoff,
                self.initial_backoff * (self.backoff_multiplier**idx),
            )

    def can_retry_err(self, err: grpc.RpcError) -> bool:
        return get_grpc_code(err) in self.retryable_status_codes


@attr.s(auto_attribs=True, frozen=True)
class NoRetryPolicy(DLYCRetryPolicyBase):
    """Mostly intended for debug"""

    call_timeout: float = 15.0
    total_timeout: float = 17.0
    min_timeout: float = 0.1

    def gen_exponential_backoff(self) -> Iterable[float]:
        yield self.min_timeout

    def can_retry_err(self, err: grpc.RpcError) -> bool:
        return False


@attr.s(auto_attribs=True, frozen=True)
class DLYCServiceConfig:
    endpoint: str = attr.ib(converter=unquote)
    tls: bool = attr.ib(default=None)  # `None` effectively defaults to `True` unless specified in the `endpoint`
    retry_policy: DLYCRetryPolicyBase = attr.ib(factory=DLYCRetryPolicy)

    # TODO: take the defaults from the `dl_configs`
    keepalive_time_msec: int = attr.ib(default=10_000)
    keepalive_timeout_msec: int = attr.ib(default=1000)
    user_agent: str = attr.ib(default="datalens")

    add_timeout: bool = attr.ib(default=True)
    add_metadata: bool = attr.ib(default=True)

    default_page_size: int = attr.ib(default=100)

    @property
    def grpc_options(self) -> Tuple[Tuple[str, Any], ...]:
        return (
            ("grpc.keepalive_time_ms", self.keepalive_time_msec),
            ("grpc.keepalive_timeout_ms", self.keepalive_timeout_msec),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.primary_user_agent", self.user_agent),
        )

    def make_channel(self) -> grpc.Channel:
        endpoint = self.endpoint
        tls = self.tls
        if tls is None:
            if endpoint.startswith(GRPCS_PREFIX):
                endpoint = endpoint[len(GRPCS_PREFIX) :]
                tls = True
            elif endpoint.startswith(GRPC_PREFIX):
                endpoint = endpoint[len(GRPC_PREFIX) :]
                tls = False
            else:
                tls = True

        grpc_options = self.grpc_options
        if tls:
            credentials = grpc.ssl_channel_credentials(
                root_certificates=get_root_certificates(),
            )
            channel = grpc.secure_channel(endpoint, credentials, options=grpc_options)
        else:
            channel = grpc.insecure_channel(endpoint, options=grpc_options)
        return channel


@attr.s(auto_attribs=True, frozen=True, slots=True)
class WrappedGRPCFunc:
    service_name: str
    call_name: str
    aio_raw: Callable[..., Awaitable[Any]]
    aio: Callable[..., Awaitable[Any]]
    grpc_func: Callable[..., Any]
    future_func: Callable[..., Any]

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return await_sync(self.aio(*args, **kwargs))


class WrappedGRPCService:
    # TODO?: use mixins instead, for static checking?

    def __init__(
        self,
        service_cls: ServiceType,
        channel: grpc.Channel,
        dl_yc_client: DLYCClientCommon,
        service_name: Optional[str] = None,
    ):
        self.service_cls = service_cls
        self.channel = channel
        self.dl_yc_client = dl_yc_client
        self.service = service_cls(channel)
        if service_name is None:
            service_name = getattr(service_cls, "__name__", None) or repr(service_cls)
        self.service_name = service_name

    def __getattr__(self, key: str) -> WrappedGRPCFunc:
        rpc_call = getattr(self.service, key)
        return self.dl_yc_client._wrap_call(rpc_call, service_name=self.service_name, call_name=key)


_CLS_TV = TypeVar("_CLS_TV", bound="DLYCClientCommon")


# TODO: commonize and apply mdb's grpc client tricks:
# https://a.yandex-team.ru/arc/trunk/arcadia/cloud/mdb/internal/python/grpcutil/service.py?rev=r7680925#L108-135
# See also:
# https://a.yandex-team.ru/arc/trunk/arcadia/cloud/iam/accessservice/client/python/client.py?rev=r6670095#L143
@attr.s
class DLYCClientCommon:
    """Common base class for ycloud gRPC API services"""

    service_config: DLYCServiceConfig = attr.ib()

    _channel: Optional[grpc.Channel] = attr.ib(default=None)
    logger: logging.Logger = attr.ib(default=LOGGER)

    request_id: Optional[str] = attr.ib(default=None)
    bearer_token: Optional[str] = attr.ib(default=None, repr=False)
    metadata: Tuple[Tuple[str, str], ...] = attr.ib(default=())

    @staticmethod
    def normalize_request_id_base(
        request_id: Optional[str], random_size: int = 3, allow_cut: bool = True
    ) -> Optional[str]:
        if not request_id:
            return None

        uuid_len = 36
        if allow_cut and len(request_id) >= uuid_len:
            uuid_piece = request_id[:uuid_len]
            try:
                uuid.UUID(uuid_piece)
            except ValueError:
                pass
            else:
                return uuid_piece

        data = request_id.encode("utf-8")
        uuid_size = 16  # bytes
        data_size = uuid_size - random_size
        if len(data) < data_size:
            data_size = len(data)
            random_size = uuid_size - data_size
        return str(
            uuid.UUID(
                bytes=(
                    data[:data_size]
                    + b"".join(chr(random.randrange(255)).encode("iso-8859-1") for _ in range(random_size))
                )
            )
        )

        return request_id

    @classmethod
    def normalize_request_id(cls, request_id: Optional[str]) -> Optional[str]:
        result = cls.normalize_request_id_base(request_id)
        if not result:
            return result
        if result != request_id:
            LOGGER.info("request_id mapped on %s: %r -> %r", cls.__name__, request_id, result)
        return result

    @classmethod
    def create(
        cls: Type[_CLS_TV],
        endpoint: str,
        channel: Optional[grpc.Channel] = None,
        request_id: Optional[str] = None,
        bearer_token: Optional[str] = None,
        metadata: Tuple[Tuple[str, str], ...] = (),
        **kwargs: Any,
    ) -> _CLS_TV:
        service_config = DLYCServiceConfig(endpoint=endpoint, **kwargs)
        return cls(
            service_config=service_config,
            channel=channel,
            request_id=request_id,
            bearer_token=bearer_token,
            metadata=metadata,
        )

    def __attrs_post_init__(self) -> None:
        if self._channel is not None:
            channel_target = self._channel._channel.target()  # type: ignore
            if isinstance(channel_target, bytes):
                channel_target = channel_target.decode("utf-8")
            endpoint = self.service_config.endpoint
            if endpoint != channel_target:
                raise ValueError(
                    "Passed channel's endpoint does not match the service config endpoint",
                    dict(channel_target=channel_target, service_config_endpoint=endpoint),
                )

    def clone(
        self: _CLS_TV,
        request_id: Union[Optional[str], Unspecified] = UNSPECIFIED,
        bearer_token: Union[Optional[str], Unspecified] = UNSPECIFIED,
    ) -> _CLS_TV:
        return self.__class__(
            service_config=self.service_config,
            channel=self._channel,
            logger=self.logger,
            request_id=get_if_specified(request_id, default=self.request_id),
            bearer_token=get_if_specified(bearer_token, default=self.bearer_token),
        )

    @property
    def retry_policy(self) -> DLYCRetryPolicyBase:
        return self.service_config.retry_policy

    @property
    def channel(self) -> grpc.Channel:
        if self._channel is None:
            self._channel = self.service_config.make_channel()
        return self._channel

    @property
    def _default_metadata(self) -> Tuple[Tuple[str, str], ...]:
        result: Tuple[Tuple[str, str], ...] = tuple(self.metadata)
        if self.request_id is not None:
            result += (("x-request-id", self.request_id),)
        if self.bearer_token is not None:
            result += (("authorization", "Bearer {}".format(self.bearer_token)),)
        return result

    def _get_page_size(self, page_size: Optional[int] = None) -> int:
        if page_size is not None:
            return page_size
        return self.service_config.default_page_size

    def _handle_grpc_error(self, err: grpc.RpcError) -> None:
        handle_grpc_error(err)

    def _wrap_call(self, func: Callable, service_name: str, call_name: str) -> WrappedGRPCFunc:
        future_func = getattr(func, "future", None)
        if future_func is None:
            raise Exception("Only gRPC callables with '.future' are supported here")

        # TODO: Move this logic to the `WrappedGRPCFunc` class itself (but
        # preserve the `functools.wraps` somehow).
        @functools.wraps(func)
        async def awrapped_call_raw(*args: Any, **kwargs: Any) -> Any:
            """Minimal wrapping of grpc future-function into an asyncio future"""
            self.logger.debug("Calling %s.%s @ %r ...", service_name, call_name, self.service_config.endpoint)

            loop = asyncio.get_event_loop()
            grpc_fut = future_func(*args, **kwargs)
            aio_fut = loop.create_future()

            def callback(grpc_fut: Any = grpc_fut, aio_fut: Any = aio_fut, loop: Any = loop) -> None:
                exc = grpc_fut.exception()
                if exc is not None:
                    loop.call_soon_threadsafe(aio_fut.set_exception, exc)
                    return
                # Note that `grpc_fut.result()` can raise a future's exception too.
                res = grpc_fut.result()
                loop.call_soon_threadsafe(aio_fut.set_result, res)

            grpc_fut.add_done_callback(callback)

            return await aio_fut

        @functools.wraps(func)
        async def awrapped_call(*args: Any, **kwargs: Any) -> Any:
            """Async wrapping with retries and other additions"""
            start_time = time.monotonic()
            max_time = start_time + self.retry_policy.total_timeout

            if self.service_config.add_metadata:
                metadata = kwargs.get("metadata", None) or ()
                metadata = tuple(self._default_metadata) + tuple(metadata)
                kwargs.update(metadata=metadata)

            add_timeout = self.service_config.add_timeout and "timeout" not in kwargs

            for backoff in self.retry_policy.gen_exponential_backoff():
                attempt_start_time = time.monotonic()

                if add_timeout:
                    timeout = min(
                        max_time - time.monotonic(),
                        self.retry_policy.call_timeout,
                    )
                    kwargs.update(timeout=timeout)

                try:
                    result = await awrapped_call_raw(*args, **kwargs)
                    return result
                except grpc.RpcError as err:
                    now = time.monotonic()
                    if (max_time - now < self.retry_policy.min_timeout) or not self.retry_policy.can_retry_err(err):
                        self._handle_grpc_error(err)
                        raise
                    sleep_time = backoff - (now - attempt_start_time)
                    self.logger.debug(
                        "Retrying %s.%s, sleeping for %.3fs, error %r", service_name, call_name, sleep_time, err
                    )
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)

            raise Exception("Incorrect retry policy, backoffs reached end of the loop")

        return WrappedGRPCFunc(
            service_name=service_name,
            call_name=call_name,
            aio=awrapped_call,
            aio_raw=awrapped_call_raw,
            grpc_func=func,
            future_func=future_func,
        )

    def _wrap_service(
        self,
        service_cls: Any,  # `ServiceType,` but mypy fails weirdly with it.
        service_name: Optional[str] = None,
    ) -> WrappedGRPCService:
        return WrappedGRPCService(
            service_cls=service_cls, channel=self.channel, dl_yc_client=self, service_name=service_name
        )

    def close(self) -> None:
        channel = self._channel
        if channel is not None:
            self._channel = None
            channel.close()

    def __enter__(self: _CLS_TV) -> _CLS_TV:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


@attr.s
class DLYCSingleServiceClient(DLYCClientCommon):
    service_cls: ClassVar[ServiceType]

    _wrapped_service: Optional[WrappedGRPCService] = attr.ib(default=None)

    @property
    def service(self) -> WrappedGRPCService:
        if self._wrapped_service is None:
            self._wrapped_service = self._wrap_service(self.service_cls)  # type: ignore
        return self._wrapped_service
