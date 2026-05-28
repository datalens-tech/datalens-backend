import datetime

import attrs

import dl_httpx
import dl_json
from dl_us_entries_client.models.base import BaseRequest
from dl_us_entries_client.models.entry import (
    EntryId,
)
from dl_us_entries_client.models.lock import (
    Lock,
    LockToken,
)


@attrs.define(kw_only=True, frozen=True)
class PrivateEntryLockPostRequest(BaseRequest):
    entry_id: EntryId
    duration: datetime.timedelta | None = None
    force: bool | None = None

    @property
    def path(self) -> str:
        return f"/private/locks/{self.entry_id}"

    @property
    def method(self) -> str:
        return "POST"

    @property
    def body(self) -> dl_json.JsonSerializableMapping:
        body: dict[str, dl_json.JsonSerializable] = {}

        if self.duration is not None:
            body["duration"] = round(self.duration.total_seconds() * 1000)  # US accepts duration in milliseconds

        if self.force is not None:
            body["force"] = self.force

        return body


class PrivateEntryLockPostResponse(LockToken, dl_httpx.BaseResponseSchema): ...


@attrs.define(kw_only=True, frozen=True)
class PrivateEntryLockDeleteRequest(BaseRequest):
    entry_id: EntryId
    lock_token: str | None = None
    force: bool | None = None

    @property
    def path(self) -> str:
        return f"/private/locks/{self.entry_id}"

    @property
    def method(self) -> str:
        return "POST"

    @property
    def query_params(self) -> dict[str, str]:
        query_params: dict[str, str] = {}

        if self.lock_token is not None:
            query_params["lockToken"] = self.lock_token

        if self.force is not None:
            query_params["force"] = "1" if self.force else "0"

        return query_params


class PrivateEntryLockDeleteResponse(Lock, dl_httpx.BaseResponseSchema): ...
