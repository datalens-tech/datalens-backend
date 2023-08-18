from typing import Generic, TypeVar, ClassVar, Type, Union

import attr
from typeguard import check_type

from bi_api_commons.base_models import TenantYCFolder, TenantYCOrganization, TenantDCProject
from bi_api_commons.base_models import TenantDef

_AUTH_OBJ_TV = TypeVar("_AUTH_OBJ_TV")


class AuthorizationMode(Generic[_AUTH_OBJ_TV]):
    supported_tenant_types: ClassVar[Type[_AUTH_OBJ_TV]]

    def ensure_tenant(self, obj: TenantDef) -> _AUTH_OBJ_TV:
        check_type("obj", obj, self.supported_tenant_types)
        return obj  # type: ignore


@attr.s(frozen=True)
class AuthorizationModeYandexCloud(AuthorizationMode[Union[TenantYCFolder, TenantYCOrganization]]):
    supported_tenant_types = Union[TenantYCFolder, TenantYCOrganization]

    folder_permission_to_check: str = attr.ib()
    organization_permission_to_check: str = attr.ib()


@attr.s(frozen=True)
class AuthorizationModeDataCloud(AuthorizationMode[TenantDCProject]):
    supported_tenant_types = TenantDCProject

    project_permission_to_check: str = attr.ib()
