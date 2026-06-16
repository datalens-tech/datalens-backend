import enum

import pydantic

import dl_constants
import dl_httpx
import dl_pydantic

type EntryId = str


class EntryScope(enum.StrEnum):
    dashboard = "dash"
    connection = "connection"
    dataset = "dataset"


class EntryPermissions(dl_httpx.BaseResponseSchema):
    execute: bool
    read: bool
    edit: bool
    admin: bool


class Data(dl_pydantic.BaseSchema):
    model_config = pydantic.ConfigDict(extra="allow")


class UnversionedData(dl_pydantic.BaseSchema):
    model_config = pydantic.ConfigDict(extra="allow")


class EntryData(dl_pydantic.BaseSchema):
    scope: EntryScope
    type: str = ""
    key: str
    hidden: bool = False
    permissions: EntryPermissions | None = None
    mode: dl_constants.USEntryMode = dl_constants.USEntryMode.publish

    data: Data | None = None
    unversioned_data: UnversionedData | None = pydantic.Field(
        default_factory=UnversionedData,
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
