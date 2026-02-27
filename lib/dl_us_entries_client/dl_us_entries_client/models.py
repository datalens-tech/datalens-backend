import enum

import attrs
import pydantic
from typing_extensions import TypeAlias

import dl_httpx
import dl_json
import dl_pydantic


EntryId: TypeAlias = str


@attrs.define(kw_only=True, frozen=True)
class PingRequest(dl_httpx.BaseRequest):
    @property
    def path(self) -> str:
        return "/ping-db"

    @property
    def method(self) -> str:
        return "GET"


class PingResponse(dl_httpx.BaseResponseSchema):
    result: str


class EntryScope(str, enum.Enum):
    dashboard = "dash"


class EntryData(dl_pydantic.BaseSchema):
    scope: EntryScope
    type: str = ""
    key: str


class Entry(EntryData):
    entry_id: EntryId = pydantic.Field(alias="entryId")


@attrs.define(kw_only=True, frozen=True)
class EntryGetRequest(dl_httpx.BaseRequest):
    entry_id: EntryId

    @property
    def path(self) -> str:
        return f"/v1/entries/{self.entry_id}"

    @property
    def method(self) -> str:
        return "GET"


EntryGetResponse = Entry


@attrs.define(kw_only=True, frozen=True)
class EntryPostRequest(dl_httpx.BaseRequest):
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
class EntryDeleteRequest(dl_httpx.BaseRequest):
    entry_id: EntryId

    @property
    def path(self) -> str:
        return f"/v1/entries/{self.entry_id}"

    @property
    def method(self) -> str:
        return "DELETE"
