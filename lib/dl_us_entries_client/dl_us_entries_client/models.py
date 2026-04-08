import enum

import attrs
import pydantic
from typing_extensions import TypeAlias

import dl_auth
import dl_httpx
import dl_json
import dl_pydantic


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


class PingResponse(dl_httpx.BaseResponseSchema):
    result: str


class BaseRequest(dl_httpx.BaseRequest):
    auth_provider: dl_auth.AuthProviderProtocol


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


class Entry(EntryData):
    entry_id: EntryId = pydantic.Field(alias="entryId")


@attrs.define(kw_only=True, frozen=True)
class EntryGetRequest(BaseRequest):
    entry_id: EntryId
    include_permissions_info: bool = False

    @property
    def path(self) -> str:
        return f"/v1/entries/{self.entry_id}"

    @property
    def method(self) -> str:
        return "GET"

    @property
    def query_params(self) -> dict[str, str]:
        params = {}
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
