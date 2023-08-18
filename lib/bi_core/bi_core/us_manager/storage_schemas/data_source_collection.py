from __future__ import annotations

import logging
from typing import Any, Optional

from marshmallow import fields
from marshmallow_oneofschema import OneOfSchema
from marshmallow_enum import EnumField

from bi_constants.enums import ManagedBy, DataSourceCollectionType

from bi_core.data_source_spec.collection import (
    DataSourceCollectionSpec, DataSourceCollectionSpecBase
)
from bi_core.us_manager.storage_schemas.base import BaseStorageSchema, CtxKey
from bi_core.us_manager.storage_schemas.data_source_spec import GenericDataSourceSpecStorageSchema


LOGGER = logging.getLogger(__name__)


class DataSourceCollectionSpecStorageSchema(BaseStorageSchema[DataSourceCollectionSpec]):
    TARGET_CLS = DataSourceCollectionSpec

    id = fields.String(required=False, allow_none=True)
    title = fields.String(required=False, allow_none=True, load_default=None)
    managed_by = EnumField(ManagedBy, required=False, allow_none=True)
    valid = fields.Boolean(load_default=True)

    # Sources
    origin = fields.Nested(GenericDataSourceSpecStorageSchema, required=False, allow_none=True)
    materialization = fields.Nested(GenericDataSourceSpecStorageSchema, required=False, allow_none=True)
    sample = fields.Nested(GenericDataSourceSpecStorageSchema, required=False, allow_none=True)

    def push_ctx(self, data):  # type: ignore  # TODO: fix
        self.context[CtxKey.dsc_id] = data.get('id')

    def pop_ctx(self, data):  # type: ignore  # TODO: fix
        self.context.pop(CtxKey.dsc_id, None)

    def to_object(self, data):  # type: ignore  # TODO: fix
        return self.get_target_cls()(
            id=data['id'],
            title=data['title'],
            managed_by=data['managed_by'],
            origin=data['origin'],
            materialization=data['materialization'],
            sample=data['sample'],
            valid=data['valid'],
        )


class GenericDataSourceCollectionStorageSchema(OneOfSchema):
    type_field = 'type'
    type_field_remove = True
    type_schemas = {
        k.name: v for k, v in {
            DataSourceCollectionType.collection: DataSourceCollectionSpecStorageSchema,
        }.items()
    }

    def get_obj_type(self, obj: Any) -> Optional[DataSourceCollectionType]:
        if isinstance(obj, DataSourceCollectionSpecBase):
            return obj.dsrc_coll_type.name  # type: ignore  # TODO: fix
        return None
