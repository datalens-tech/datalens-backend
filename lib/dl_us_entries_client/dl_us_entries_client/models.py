import enum
from typing import Protocol

import attrs
import pydantic
from typing_extensions import TypeAlias

import dl_constants
import dl_httpx
import dl_json
import dl_pydantic
import dl_us_entries_client.exceptions as exceptions


EntryId: TypeAlias = str


@attrs.define(kw_only=True, frozen=True)
class PingRequest(dl_httpx.BaseRequest):
    # auth_provider is not needed for ping request
    @property
    def path(self) -> str:
        return "/ping"

    @property
    def method(self) -> str:
        return "GET"


class TenantProtocol(Protocol):
    def get_outbound_tenancy_headers(self) -> dict[dl_constants.DLHeaders, str]: ...


@attrs.define(kw_only=True, frozen=True)
class BaseRequest(dl_httpx.BaseRequest):
    tenant: TenantProtocol | None = None

    @property
    def headers(self) -> dict[str, str]:
        result = super().headers
        if self.tenant is not None:
            tenant_headers = self.tenant.get_outbound_tenancy_headers()
            result.update({k.value.lower(): v for k, v in tenant_headers.items()})
        return result


class EntryScope(str, enum.Enum):
    dashboard = "dash"
    connection = "connection"


class EntryPermissions(dl_httpx.BaseResponseSchema):
    execute: bool
    read: bool
    edit: bool
    admin: bool


class EntryData(dl_pydantic.BaseSchema):
    scope: EntryScope
    type: str = ""
    key: str
    permissions: EntryPermissions | None = None

    @property
    def permissions_strict(self) -> EntryPermissions:
        if self.permissions is None:
            raise ValueError("permissions_strict called but permissions were not requested")
        return self.permissions


class Entry(EntryData):
    entry_id: EntryId = pydantic.Field(alias="entryId")


@attrs.define(kw_only=True, frozen=True)
class EntryGetRequest(BaseRequest):
    entry_id: EntryId
    include_permissions_info: bool = False
    error_transformer: dl_httpx.ErrorTransformerProtocol = dl_httpx.StatusMapTransformer(
        status_map={404: exceptions.EntryNotFoundError.from_httpx_exception},
    )

    @property
    def path(self) -> str:
        return f"/v1/entries/{self.entry_id}"

    @property
    def method(self) -> str:
        return "GET"

    @property
    def query_params(self) -> dict[str, str]:
        params = super().query_params
        if self.include_permissions_info:
            params["includePermissionsInfo"] = "1"
        return params


EntryGetResponse = Entry


@attrs.define(kw_only=True, frozen=True)
class EntryPostRequest(BaseRequest):
    entry: EntryData

    @property
    def path(self) -> str:
        return "/v1/entries"

    @property
    def method(self) -> str:
        return "POST"

    @property
    def body(self) -> dl_json.JsonSerializableMapping:
        return self.entry.model_dump_jsonable()


EntryPostResponse = Entry


@attrs.define(kw_only=True, frozen=True)
class EntryDeleteRequest(BaseRequest):
    entry_id: EntryId

    @property
    def path(self) -> str:
        return f"/v1/entries/{self.entry_id}"

    @property
    def method(self) -> str:
        return "DELETE"
