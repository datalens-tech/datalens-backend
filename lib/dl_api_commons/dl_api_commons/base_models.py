import abc
from typing import Any

import attr
from multidict import (
    CIMultiDict,
    CIMultiDictProxy,
)
from typing_extensions import Self

import dl_auth
import dl_constants


@attr.s()
class TenantDef(metaclass=abc.ABCMeta):
    is_data_export_enabled: bool = attr.ib(default=False, kw_only=True)
    is_background_data_export_allowed: bool = attr.ib(default=False, kw_only=True)

    @abc.abstractmethod
    def get_outbound_tenancy_headers(self) -> dict[dl_constants.DLHeaders, str]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_tenant_id(self) -> str:
        raise NotImplementedError()

    def get_reporting_extra(self) -> dict[str, str | int | None]:
        return dict(
            billing_folder_id=self.get_tenant_id(),
        )


@attr.s(frozen=True)
class RequestContextInfo:
    request_id: str | None = attr.ib(default=None)
    tenant: TenantDef | None = attr.ib(default=None)
    user_id: str | None = attr.ib(default=None)
    user_name: str | None = attr.ib(default=None)
    x_dl_debug_mode: bool | None = attr.ib(default=None)
    endpoint_code: str | None = attr.ib(default=None)

    _x_dl_context: dict[str, str] = attr.ib(factory=dict)
    _plain_headers: CIMultiDict = attr.ib(factory=CIMultiDict)
    _secret_headers: CIMultiDict = attr.ib(repr=False, factory=CIMultiDict)
    auth_data: dl_auth.AuthData | None = attr.ib(repr=False, default=None)

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
    def forwarder_for(self) -> str | None:
        # TODO migrate all usages to `forwarded_for` and remove
        return self.forwarded_for

    @property
    def forwarded_for(self) -> str | None:
        return self.plain_headers.get(dl_constants.DLHeadersCommon.FORWARDED_FOR.value)

    @property
    def real_ip(self) -> str | None:
        return self.plain_headers.get(dl_constants.DLHeadersCommon.REAL_IP.value)

    @property
    def workbook_id(self) -> str | None:
        return self.plain_headers.get(dl_constants.DLHeadersCommon.WORKBOOK_ID.value)

    @property
    def is_embedded(self) -> bool:
        if self.user_id is None:
            return False
        return "EMBEDDED" in self.user_id

    @property
    def is_public(self) -> bool:
        if self.user_id is None:
            return False
        return "PUBLIC" in self.user_id

    @property
    def client_ip(self) -> str | None:
        real_ip = self.real_ip
        if real_ip is not None:
            return real_ip
        forwarded_for = self.forwarded_for
        if forwarded_for is not None:
            ip_list = [ip.strip() for ip in forwarded_for.split(",")]
            if len(ip_list) > 1:
                return ip_list[-2]
            else:
                # unlikely to happen
                return ip_list[0]
        return None

    @property
    def host(self) -> str | None:
        return self.plain_headers.get("Host")

    @property
    def locale(self) -> str | None:
        return self.plain_headers.get(dl_constants.DLHeadersCommon.ACCEPT_LANGUAGE.value)

    @staticmethod
    def normalize_headers_dict(headers: CIMultiDict | dict | None) -> CIMultiDict:
        if headers is None:
            return CIMultiDict()
        if isinstance(headers, dict):
            return CIMultiDict(headers)
        if isinstance(headers, CIMultiDict):
            return CIMultiDict(headers.items())
        raise TypeError(f"Unexpected type of headers: {type(headers)}")

    @classmethod
    def create_empty(cls) -> Self:
        return cls()

    @classmethod
    def create(
        cls,
        request_id: str | None,
        tenant: TenantDef | None,
        user_id: str | None,
        user_name: str | None,
        x_dl_debug_mode: bool | None,
        endpoint_code: str | None,
        x_dl_context: dict | None,
        plain_headers: CIMultiDict | dict | None,
        secret_headers: CIMultiDict | dict | None,
        auth_data: dl_auth.AuthData | None = None,
    ) -> Self:
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

    def clone(self, **kwargs: Any) -> Self:
        return attr.evolve(self, **kwargs)

    def get_reporting_extra(self) -> dict[str, str | int | None]:
        reporting_extra: dict[str, str | int | None] = dict(
            user_id=self.user_id,
            source=self.endpoint_code,
            username=self.user_name,
        )
        if self.tenant is not None:
            reporting_extra.update(self.tenant.get_reporting_extra())
        return reporting_extra


@attr.s(frozen=True)
class TenantCommon(TenantDef):
    is_data_export_enabled: bool = attr.ib(default=True, kw_only=True)

    def get_outbound_tenancy_headers(self) -> dict[dl_constants.DLHeaders, str]:
        return {}

    def get_tenant_id(self) -> str:
        return "common"


@attr.s(frozen=True)
class FormConfigParams:
    conn_id: str | None = attr.ib(default=None)
    exports_history_url_path: str | None = attr.ib(default=None)
    user_id: str | None = attr.ib(default=None)
