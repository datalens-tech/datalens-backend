from __future__ import annotations

from typing import ClassVar

import attr

from bi_api_commons_ya_cloud.constants import DLHeadersYC
from dl_api_commons.base_models import (
    AuthData,
    TenantDef,
)
from dl_constants.api_constants import (
    DLCookies,
    DLHeaders,
    DLHeadersCommon,
)


@attr.s(frozen=True)
class TenantYCFolder(TenantDef):
    folder_id: str = attr.ib()

    def get_outbound_tenancy_headers(self) -> dict[DLHeaders, str]:
        return {DLHeadersCommon.TENANT_ID: self.folder_id}

    def get_tenant_id(self) -> str:
        return self.folder_id


@attr.s(frozen=True)
class TenantYCOrganization(TenantDef):
    tenant_id_prefix: ClassVar[str] = "org_"

    org_id: str = attr.ib()

    def get_outbound_tenancy_headers(self) -> dict[DLHeaders, str]:
        return {DLHeadersCommon.TENANT_ID: self.get_tenant_id()}

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

    def get_tenant_id(self) -> str:
        return self.project_id  # ???


@attr.s(frozen=True)
class IAMAuthData(AuthData):
    iam_token: str = attr.ib(repr=False)

    def get_headers(self) -> dict[DLHeaders, str]:
        return {DLHeadersYC.IAM_TOKEN: self.iam_token}

    def get_cookies(self) -> dict[DLCookies, str]:
        return {}


@attr.s()
class ExternalIAMAuthData(IAMAuthData):
    def get_headers(self) -> dict[DLHeaders, str]:
        return {DLHeadersCommon.AUTHORIZATION_TOKEN: f"Bearer {self.iam_token}"}


@attr.s(frozen=True)
class EmbedAuthData(AuthData):
    embed_token: str = attr.ib(repr=False)

    def get_headers(self) -> dict[DLHeaders, str]:
        return {DLHeadersCommon.EMBED_TOKEN: self.embed_token}

    def get_cookies(self) -> dict[DLCookies, str]:
        return {}
