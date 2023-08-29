import abc
from typing import Optional

from bi_api_commons.base_models import TenantDef, TenantCommon


class TenantResolver(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def resolve_tenant_def_by_tenant_id(self, tenant_id_str: Optional[str]) -> TenantDef:
        raise NotImplementedError()


class CommonTenantResolver(TenantResolver):
    def resolve_tenant_def_by_tenant_id(self, tenant_id_str: Optional[str]) -> TenantDef:
        return TenantCommon()
