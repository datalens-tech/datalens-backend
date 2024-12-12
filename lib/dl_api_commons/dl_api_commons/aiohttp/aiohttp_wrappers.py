from __future__ import annotations

import enum
import functools
import inspect
import json
from typing import (
    Any,
    Awaitable,
    Callable,
    FrozenSet,
    Generic,
    Literal,
    Optional,
    Type,
    TypeVar,
    Union,
    overload,
)

from aiohttp import web
from aiohttp.typedefs import Handler, Middleware

from dl_api_commons.base_models import RequestContextInfo
from dl_api_commons.exc import InvalidHeaderException
from dl_api_commons.logging import RequestLoggingContextController
from dl_api_commons.reporting.profiler import ReportingProfiler
from dl_api_commons.reporting.registry import ReportingRegistry
from dl_constants.api_constants import DLHeaders


class RequiredResource(enum.Enum):
    pass


class RequiredResourceCommon(RequiredResource):
    US_MANAGER = enum.auto()
    SERVICE_US_MANAGER = enum.auto()
    MASTER_KEY = enum.auto()
    SKIP_AUTH = enum.auto()
    SKIP_CSRF = enum.auto()


class RCINotSet(Exception):
    pass


class DLRequestBase:
    __slots__ = ("request",)
    request: web.Request

    KEY_DL_REQUEST = "dl_request"
    KEY_RCI_TEMP = "bi_request_context_info_temp"
    KEY_RCI = "bi_request_context_info"
    KEY_LOG_CTX_CONTROLLER = "bi_log_ctx_controller"
    KEY_REPORTING_REGISTRY = "reporting_registry"
    KEY_REPORTING_PROFILER = "reporting_profiler"

    HANDLER_ATTR_NAME_REQUIRED_RESOURCES = "REQUIRED_RESOURCES"
    NO_RESOURCES = frozenset()  # type: ignore  # TODO: fix

    # flag that forces `.url` to use `https` scheme
    enforce_https_on_self_url: bool = True

    def __init__(self, request: web.Request):
        self.request = request

    @classmethod
    def init_and_bind_to_aiohttp_request(cls, request: web.Request) -> DLRequestBase:
        dl_request = cls(request)
        request[cls.KEY_DL_REQUEST] = dl_request
        return dl_request

    @classmethod
    def get_for_request(cls, request: web.Request) -> Optional[DLRequestBase]:
        if cls.KEY_DL_REQUEST in request:
            return request[cls.KEY_DL_REQUEST]
        return None

    def _set_attr_once(self, name, value) -> None:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        if name in self.request:
            raise ValueError(f"Request key '{name}' already set")
        self.request[name] = value

    @property
    def url(self) -> str:
        if self.enforce_https_on_self_url and self.request.scheme == "http":
            # 'correct' but possibly problematic: `return self.request.clone(scheme='https').url`
            result = str(self.request.url)
            pfx = "http://"
            if result.startswith(pfx):
                return "https://" + result[len(pfx) :]
        return self.request.url  # type: ignore  # TODO: fix

    # TODO FIX: Check that is not used and remove
    @property
    def request_id(self) -> Optional[str]:
        return self.rci.request_id

    # TODO FIX: Check that is not used and remove
    @property
    def user_id(self) -> str:
        return self.rci.user_id  # type: ignore  # TODO: fix

    # TODO FIX: Check that is not used and remove
    @property
    def user_name(self):  # type: ignore  # TODO: fix
        return self.rci.user_name

    @property
    def temp_rci(self) -> RequestContextInfo:
        """
        Returns not-committed RCI. Should be used only during RCI building.
        """
        if self.KEY_RCI_TEMP not in self.request:
            raise RCINotSet("Temp RCI was not initiated")
        return self.request[self.KEY_RCI_TEMP]

    def init_temp_rci(self, rci: RequestContextInfo):  # type: ignore  # TODO: fix
        """This method should be called in request_id middleware (or another top-level middleware)"""
        self._set_attr_once(self.KEY_RCI_TEMP, rci)

    def replace_temp_rci(self, rci: RequestContextInfo) -> None:
        if self.is_rci_committed():
            raise ValueError("Attempt to modify temp RequestContextInfo after commit")
        self.request[self.KEY_RCI_TEMP] = rci
        if self.reporting_registry is not None:
            self.reporting_registry.rci = rci

    def update_temp_rci(self, **kwargs: Any) -> None:
        return self.replace_temp_rci(self.temp_rci.clone(**kwargs))

    def commit_rci(self) -> None:
        """
        Mark RCI as fully build. Should be called after all RCI-forming middleware done their job.
        """
        self._set_attr_once(self.KEY_RCI, self.temp_rci)
        if self.reporting_registry is not None:
            self.reporting_registry.rci = self.rci

    def is_rci_committed(self) -> bool:
        return self.KEY_RCI in self.request

    @property
    def rci(self) -> RequestContextInfo:
        """
        Returns RCI if committed. If not committed - exception will be thrown.
        """
        if self.is_rci_committed():
            return self.request[self.KEY_RCI]
        raise RCINotSet("RequestContextInfo is not committed for this request")

    @property
    def last_resort_rci(self) -> Optional[RequestContextInfo]:
        """
        :return: Returns committed RCI if exists
        """
        try:
            return self.rci
        except RCINotSet:
            try:
                return self.temp_rci
            except RCINotSet:
                return None

    @property
    def log_ctx_controller(self) -> Optional[RequestLoggingContextController]:
        return self.request.get(self.KEY_LOG_CTX_CONTROLLER)

    @property
    def reporting_registry(self) -> ReportingRegistry:
        return self.request.get(self.KEY_REPORTING_REGISTRY)  # type: ignore  # TODO: fix

    @reporting_registry.setter
    def reporting_registry(self, value: ReportingRegistry):  # type: ignore  # TODO: fix
        value.rci = self.temp_rci
        self._set_attr_once(self.KEY_REPORTING_REGISTRY, value)

    @property
    def reporting_profiler(self) -> ReportingProfiler:
        return self.request.get(self.KEY_REPORTING_PROFILER)  # type: ignore  # TODO: fix

    @reporting_profiler.setter
    def reporting_profiler(self, value: ReportingProfiler) -> None:
        self._set_attr_once(self.KEY_REPORTING_PROFILER, value)

    @overload
    def get_single_header(self, header: Union[DLHeaders, str]) -> Optional[str]:
        pass

    @overload  # noqa
    def get_single_header(self, header: Union[DLHeaders, str], required: Literal[False]) -> Optional[str]:
        pass

    @overload  # noqa
    def get_single_header(self, header: Union[DLHeaders, str], required: Literal[True]) -> str:
        pass

    def get_single_header(self, header, required=False):  # type: ignore  # TODO: fix  # noqa
        header_name = header.value if isinstance(header, DLHeaders) else header
        header_value_list = self.request.headers.getall(header_name, ())

        if len(header_value_list) == 0:
            if required:
                raise InvalidHeaderException("Header required, but missing", header_name=header_name)
            return None
        elif len(header_value_list) > 1:
            raise InvalidHeaderException("Expecting single header but multiple received", header_name=header_name)

        return header_value_list[0]  # type: ignore  # 2024-01-24 # TODO: Tuple index out of range  [misc]

    def get_single_json_header(self, header: DLHeaders) -> Union[bool, int, float, list, dict, None]:
        raw_header = self.request.headers.get(header.value)
        if raw_header is None:
            return None
        try:
            return json.loads(raw_header)
        except json.JSONDecodeError as e:
            raise InvalidHeaderException(
                "Invalid JSON in header content",
                header_name=header.value,
            ) from e

    @property
    def required_resources(self) -> FrozenSet[RequiredResource]:
        handler = self.request.match_info.handler

        if hasattr(handler, self.HANDLER_ATTR_NAME_REQUIRED_RESOURCES):
            return getattr(handler, self.HANDLER_ATTR_NAME_REQUIRED_RESOURCES)

        return self.NO_RESOURCES

    _SELF_TYPE = TypeVar("_SELF_TYPE", bound="DLRequestBase")

    @classmethod
    def use_dl_request(
        cls: Type[_SELF_TYPE], coro: Callable[[_SELF_TYPE, Handler], Awaitable[web.StreamResponse]]
    ) -> Middleware:
        if not inspect.iscoroutinefunction(coro):
            raise ValueError("This decorator may only be applied to a coroutine")

        @functools.wraps(coro)
        async def wrapper(request: web.Request, handler: Handler) -> web.StreamResponse:
            dl_request = request[cls.KEY_DL_REQUEST]
            if not isinstance(dl_request, cls):
                raise TypeError(
                    f"Error in .use_dl_request decorator on {coro}."
                    f" Actual class of DLRequest {type(dl_request).__name__}"
                    f" is not subclass of request: {cls.__name__}."
                )
            return await coro(dl_request, handler)

        return wrapper

    @classmethod
    def use_dl_request_on_method(
        cls, coro: Callable[[Any, _SELF_TYPE, Handler], Awaitable[web.StreamResponse]]
    ) -> Middleware:
        if not inspect.iscoroutinefunction(coro):
            raise ValueError("This decorator may only be applied to a coroutine")

        @functools.wraps(coro)
        async def wrapper(self: Any, request: web.Request, handler: Handler) -> web.StreamResponse:
            dl_request = request[cls.KEY_DL_REQUEST]
            return await coro(self, dl_request, handler)

        return wrapper


_DL_REQUEST_TV = TypeVar("_DL_REQUEST_TV", bound=DLRequestBase)


class DLRequestView(web.View, Generic[_DL_REQUEST_TV]):
    dl_request_cls: Type[_DL_REQUEST_TV] = DLRequestBase  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "type[DLRequestBase]", variable has type "type[_DL_REQUEST_TV]")  [assignment]

    @property
    def dl_request(self) -> _DL_REQUEST_TV:
        dl_request = self.request[DLRequestBase.KEY_DL_REQUEST]
        if not isinstance(dl_request, self.dl_request_cls):
            raise TypeError(
                f"Actual class of DLRequest {type(dl_request).__name__}"
                f" is not subclass of request: {self.dl_request_cls.__name__}"
            )
        return dl_request
