import pydantic

import dl_pydantic


class LockToken(dl_pydantic.BaseSchema):
    lock_token: str = pydantic.Field(alias="lockToken")


class Lock(dl_pydantic.BaseSchema):
    entry_id: str = pydantic.Field(alias="entryId")
    lock_token: str = pydantic.Field(alias="lockToken")
    expiry_date: str = pydantic.Field(alias="expiryDate")
    start_date: str = pydantic.Field(alias="startDate")
    login: str | None = pydantic.Field(default=None)
