from __future__ import annotations

import logging

from marshmallow import fields

from dl_constants.enums import ManagedBy
from dl_core.data_source_spec.collection import DataSourceCollectionSpec
from dl_core.us_manager.storage_schemas.base import (
    BaseStorageSchema,
    CtxKey,
)
from dl_core.us_manager.storage_schemas.data_source_spec import GenericDataSourceSpecStorageSchema


LOGGER = logging.getLogger(__name__)


class DataSourceCollectionSpecStorageSchema(BaseStorageSchema[DataSourceCollectionSpec]):
    TARGET_CLS = DataSourceCollectionSpec

    id = fields.String(required=False, allow_none=True)
    title = fields.String(required=False, allow_none=True, load_default=None)
    managed_by = fields.Enum(ManagedBy, required=False, allow_none=True)
    valid = fields.Boolean(load_default=True)

    # Sources
    origin = fields.Nested(GenericDataSourceSpecStorageSchema, required=False, allow_none=True)
    materialization = fields.Nested(GenericDataSourceSpecStorageSchema, required=False, allow_none=True)
    sample = fields.Nested(GenericDataSourceSpecStorageSchema, required=False, allow_none=True)

    def push_ctx(self, data: dict) -> None:
        self.context[CtxKey.dsc_id] = data.get("id")

    def pop_ctx(self, data: dict) -> None:
        self.context.pop(CtxKey.dsc_id, None)

    def to_object(self, data):  # type: ignore  # TODO: fix
        return self.get_target_cls()(
            id=data["id"],
            title=data["title"],
            managed_by=data["managed_by"],
            origin=data["origin"],
            materialization=data["materialization"],
            sample=data["sample"],
            valid=data["valid"],
        )
