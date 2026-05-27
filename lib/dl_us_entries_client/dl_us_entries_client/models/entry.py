import enum
from typing import TypeAlias

import pydantic

import dl_httpx
import dl_pydantic

EntryId: TypeAlias = str


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
