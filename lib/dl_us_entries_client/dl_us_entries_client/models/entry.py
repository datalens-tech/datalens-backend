import enum

import pydantic

import dl_httpx
import dl_pydantic

type EntryId = str


class EntryScope(str, enum.Enum):
    dashboard = "dash"
    connection = "connection"
    dataset = "dataset"


class EntryPermissions(dl_httpx.BaseResponseSchema):
    execute: bool
    read: bool
    edit: bool
    admin: bool


class Data(dl_pydantic.BaseSchema): ...


class UnversionedData(dl_pydantic.BaseSchema): ...


class EntryData(dl_pydantic.BaseSchema):
    scope: EntryScope
    type: str = ""
    key: str
    hidden: bool = False
    permissions: EntryPermissions | None = None

    data: dl_pydantic.JsonableDict | None = None
    unversioned_data: dl_pydantic.JsonableDict | None = pydantic.Field(
        default_factory=dict,
        alias="unversionedData",
    )

    @property
    def permissions_strict(self) -> EntryPermissions:
        if self.permissions is None:
            raise ValueError("permissions_strict called but permissions were not requested")
        return self.permissions


class Entry(EntryData):
    entry_id: EntryId = pydantic.Field(alias="entryId")
    rev_id: str = pydantic.Field(alias="revId")
