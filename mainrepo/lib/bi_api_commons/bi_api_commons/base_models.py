from __future__ import annotations

import abc
from typing import ClassVar, Optional, Union, Type, Any, TypeVar

import attr
from multidict import CIMultiDict, CIMultiDictProxy

from bi_cloud_integration import model as cloud_model
from bi_constants.api_constants import DLHeaders, DLCookies, DLHeadersCommon, DLCookiesCommon


class TenantDef(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_outbound_tenancy_headers(self) -> dict[DLHeaders, str]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_iam_resource(self) -> cloud_model.IAMResource:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_tenant_id(self) -> str:
        raise NotImplementedError()


class AuthData(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_headers(self) -> dict[DLHeaders, str]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_cookies(self) -> dict[DLCookies, str]:
        raise NotImplementedError()


@attr.s(frozen=True)
class TenantYCFolder(TenantDef):
    folder_id: str = attr.ib()

    def get_outbound_tenancy_headers(self) -> dict[DLHeaders, str]:
        return {DLHeadersCommon.TENANT_ID: self.folder_id}

    def get_iam_resource(self) -> cloud_model.IAMResource:
        return cloud_model.IAMResource.folder(self.folder_id)

    def get_tenant_id(self) -> str:
        return self.folder_id


@attr.s(frozen=True)
class TenantYCOrganization(TenantDef):
    tenant_id_prefix: ClassVar[str] = "org_"

    org_id: str = attr.ib()

    def get_outbound_tenancy_headers(self) -> dict[DLHeaders, str]:
        return {DLHeadersCommon.TENANT_ID: self.get_tenant_id()}

    def get_iam_resource(self) -> cloud_model.IAMResource:
        return cloud_model.IAMResource.organization(self.org_id)

    def get_tenant_id(self) -> str:
        return f"{self.tenant_id_prefix}{self.org_id}"


@attr.s(frozen=True)
class TenantDCProject(TenantDef):
    """
    Project is not a real tenant in US. It is a kind of authorization label at this moment.
     Project must be provided in headers for all requests to US regular API.
     From backend PoV it is looks and smells like tenant: objects in different projects can not reference each other.
     So we leave it as tenant to do not overload logic of US client.
     May be will better if we rename this class in DCTenantDef.
    """
    project_id: str = attr.ib()

    def get_outbound_tenancy_headers(self) -> dict[DLHeaders, str]:
        return {DLHeadersCommon.PROJECT_ID: self.project_id}

    def get_iam_resource(self) -> cloud_model.IAMResource:
        raise NotImplementedError()

    def get_tenant_id(self) -> str:
        return self.project_id  # ???


@attr.s(frozen=True)
class IAMAuthData(AuthData):
    iam_token: str = attr.ib(repr=False)

    def get_headers(self) -> dict[DLHeaders, str]:
        return {DLHeadersCommon.IAM_TOKEN: self.iam_token}

    def get_cookies(self) -> dict[DLCookies, str]:
        return {}


@attr.s(frozen=True)
class EmbedAuthData(AuthData):
    embed_token: str = attr.ib(repr=False)

    def get_headers(self) -> dict[DLHeaders, str]:
        return {DLHeadersCommon.EMBED_TOKEN: self.embed_token}

    def get_cookies(self) -> dict[DLCookies, str]:
        return {}


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

    def get_tenant_id_if_cloud_env_none_else(self) -> Optional[str]:
        tenant = self.tenant
        if isinstance(tenant, (TenantYCFolder, TenantYCOrganization)):
            return tenant.get_tenant_id()
        return None

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
        return self.plain_headers.get('Host')

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


_REQUEST_CONTEXT_INFO_TV = TypeVar('_REQUEST_CONTEXT_INFO_TV', bound='RequestContextInfo')


@attr.s(frozen=True)
class TenantCommon(TenantDef):
    def get_outbound_tenancy_headers(self) -> dict[DLHeaders, str]:
        return {}

    def get_iam_resource(self) -> cloud_model.IAMResource:
        raise NotImplementedError()

    def get_tenant_id(self) -> str:
        return "common"


@attr.s()
class NoAuthData(AuthData):
    def get_headers(self) -> dict[DLHeaders, str]:
        return {}

    def get_cookies(self) -> dict[DLCookies, str]:
        return {}


@attr.s(frozen=True)
class YaTeamAuthData(AuthData):
    oauth_token: Optional[str] = attr.ib(default=None, repr=False)
    cookie_session_id: Optional[str] = attr.ib(default=None, repr=False)
    cookie_sessionid2: Optional[str] = attr.ib(default=None, repr=False)

    def __attrs_post_init__(self) -> None:
        if self.oauth_token is None and self.cookie_session_id is None and self.cookie_sessionid2 is None:
            raise AssertionError("YaTeamAuthData must contain at least one of cookie or oAuth token")

    def get_headers(self) -> dict[DLHeaders, str]:
        if self.oauth_token is not None:
            return {DLHeadersCommon.AUTHORIZATION_TOKEN: f"OAuth {self.oauth_token}"}
        return {}

    def get_cookies(self) -> dict[DLCookies, str]:
        cookies: dict[DLCookiesCommon, Optional[str]] = {
            DLCookiesCommon.YA_TEAM_SESSION_ID: self.cookie_session_id,
            DLCookiesCommon.YA_TEAM_SESSION_ID_2: self.cookie_sessionid2,
        }
        return {
            c_name: c_value
            for c_name, c_value in cookies.items()
            if c_value is not None
        }
