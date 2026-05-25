import attrs

import dl_httpx
import dl_json
import dl_us_entries_client.exceptions as exceptions
from dl_us_entries_client.models.base import BaseRequest
from dl_us_entries_client.models.entry import (
    Entry,
    EntryData,
    EntryId,
)


@attrs.define(kw_only=True, frozen=True)
class PrivateEntryGetRequest(BaseRequest):
    entry_id: EntryId
    include_permissions_info: bool = False
    error_transformer: dl_httpx.ErrorTransformerProtocol = dl_httpx.StatusMapTransformer(
        status_map={404: exceptions.EntryNotFoundError.from_httpx_exception},
    )

    @property
    def path(self) -> str:
        return f"/private/entries/{self.entry_id}"

    @property
    def method(self) -> str:
        return "GET"

    @property
    def query_params(self) -> dict[str, str]:
        params = super().query_params
        if self.include_permissions_info:
            params["includePermissionsInfo"] = "1"
        return params


class PrivateEntryGetResponse(Entry, dl_httpx.BaseResponseSchema): ...


@attrs.define(kw_only=True, frozen=True)
class PrivateEntryPostRequest(BaseRequest):
    entry: EntryData

    @property
    def path(self) -> str:
        return "/private/entries"

    @property
    def method(self) -> str:
        return "POST"

    @property
    def body(self) -> dl_json.JsonSerializableMapping:
        return self.entry.model_dump_jsonable()


class PrivateEntryPostResponse(Entry, dl_httpx.BaseResponseSchema): ...


@attrs.define(kw_only=True, frozen=True)
class PrivateEntryDeleteRequest(BaseRequest):
    entry_id: EntryId

    @property
    def path(self) -> str:
        return f"/private/entries/{self.entry_id}"

    @property
    def method(self) -> str:
        return "DELETE"
