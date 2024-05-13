from __future__ import annotations

import abc
from typing import (
    Any,
    Optional,
    Type,
    TypeVar,
    Union,
)

import attr
from multidict import (
    CIMultiDict,
    CIMultiDictProxy,
)

from dl_constants.api_constants import (
    DLCookies,
    DLHeaders,
    DLHeadersCommon,
)


class TenantDef(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_outbound_tenancy_headers(self) -> dict[DLHeaders, str]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_tenant_id(self) -> str:
        raise NotImplementedError()

    def get_reporting_extra(self) -> dict[str, str]:
        return dict(
            billing_folder_id=self.get_tenant_id(),
        )


class AuthData(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_headers(self) -> dict[DLHeaders, str]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_cookies(self) -> dict[DLCookies, str]:
        raise NotImplementedError()


@attr.s(frozen=True)
class RequestContextInfo:
    request_id: Optional[str] = attr.ib(default=None)
    tenant: Optional[TenantDef] = attr.ib(default=None)
    user_id: Optional[str] = attr.ib(default=None)
    user_name: Optional[str] = attr.ib(default=None)
    x_dl_debug_mode: Optional[bool] = attr.ib(default=None)
    endpoint_code: Optional[str] = attr.ib(default=None)

    _x_dl_context: dict[str, str] = attr.ib(factory=dict)
    _plain_headers: CIMultiDict = attr.ib(factory=CIMultiDict)
    _secret_headers: CIMultiDict = attr.ib(repr=False, factory=CIMultiDict)
    auth_data: Optional[AuthData] = attr.ib(repr=False, default=None)

    @property
    def x_dl_context(self) -> dict[str, str]:
        return self._x_dl_context.copy()

    @property
    def plain_headers(self) -> CIMultiDictProxy:
        return CIMultiDictProxy(self._plain_headers)

    @property
    def secret_headers(self) -> CIMultiDictProxy:
        return CIMultiDictProxy(self._secret_headers)

    @property
    def forwarder_for(self) -> Optional[str]:
        return self.plain_headers.get(DLHeadersCommon.FORWARDED_FOR.value)

    @property
    def workbook_id(self) -> Optional[str]:
        return self.plain_headers.get(DLHeadersCommon.WORKBOOK_ID.value)

    @property
    def client_ip(self) -> Optional[str]:
        if self.forwarder_for is not None:
            ip_list = [ip.strip() for ip in self.forwarder_for.split(",")]
            if len(ip_list) > 1:
                return ip_list[-2]
            else:
                # unlikely to happen
                return ip_list[0]
        return None

    @property
    def host(self) -> Optional[str]:
        return self.plain_headers.get("Host")

    @property
    def locale(self) -> Optional[str]:
        return self.plain_headers.get(DLHeadersCommon.ACCEPT_LANGUAGE.value)

    @staticmethod
    def normalize_headers_dict(headers: Union[CIMultiDict, dict, None]) -> CIMultiDict:
        if headers is None:
            return CIMultiDict()
        if isinstance(headers, dict):
            return CIMultiDict(headers)
        if isinstance(headers, CIMultiDict):
            return CIMultiDict(headers.items())
        raise TypeError(f"Unexpected type of headers: {type(headers)}")

    @classmethod
    def create_empty(cls: Type[_REQUEST_CONTEXT_INFO_TV]) -> _REQUEST_CONTEXT_INFO_TV:
        return cls()

    @classmethod
    def create(
        cls: Type[_REQUEST_CONTEXT_INFO_TV],
        request_id: Optional[str],
        tenant: Optional[TenantDef],
        user_id: Optional[str],
        user_name: Optional[str],
        x_dl_debug_mode: Optional[bool],
        endpoint_code: Optional[str],
        x_dl_context: Optional[dict],
        plain_headers: Union[CIMultiDict, dict, None],
        secret_headers: Union[CIMultiDict, dict, None],
        auth_data: Optional[AuthData] = None,
    ) -> _REQUEST_CONTEXT_INFO_TV:
        return cls(
            request_id=request_id,
            tenant=tenant,
            user_id=user_id,
            user_name=user_name,
            x_dl_debug_mode=x_dl_debug_mode,
            endpoint_code=endpoint_code,
            x_dl_context=x_dl_context or {},
            plain_headers=cls.normalize_headers_dict(plain_headers),
            secret_headers=cls.normalize_headers_dict(secret_headers),
            auth_data=auth_data,
        )

    def clone(self: _REQUEST_CONTEXT_INFO_TV, **kwargs: Any) -> _REQUEST_CONTEXT_INFO_TV:
        return attr.evolve(self, **kwargs)

    def get_reporting_extra(self) -> dict[str, str | None]:
        tenant_reporting_extra: dict[str, str | None] = (
            dict(billing_folder_id=None) if self.tenant is None else self.tenant.get_reporting_extra()
        )
        return dict(
            user_id=self.user_id,
            source=self.endpoint_code,
            username=self.user_name,
            **tenant_reporting_extra,
        )


_REQUEST_CONTEXT_INFO_TV = TypeVar("_REQUEST_CONTEXT_INFO_TV", bound="RequestContextInfo")


@attr.s(frozen=True)
class TenantCommon(TenantDef):
    def get_outbound_tenancy_headers(self) -> dict[DLHeaders, str]:
        return {}

    def get_tenant_id(self) -> str:
        return "common"


@attr.s()
class NoAuthData(AuthData):
    def get_headers(self) -> dict[DLHeaders, str]:
        return {}

    def get_cookies(self) -> dict[DLCookies, str]:
        return {}
