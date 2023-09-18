from __future__ import annotations

from marshmallow import fields

from dl_constants.enums import IndexKind
from dl_core.db import IndexInfo
from dl_core.us_manager.storage_schemas.base import BaseStorageSchema


class DataSourceIndexInfoStorageSchema(BaseStorageSchema[IndexInfo]):
    columns = fields.List(fields.String)
    kind = fields.Enum(IndexKind, allow_none=True)

    def to_object(self, data: dict) -> IndexInfo:
        columns = tuple(data.pop("columns"))
        return IndexInfo(
            columns=columns,
            **data,
        )
