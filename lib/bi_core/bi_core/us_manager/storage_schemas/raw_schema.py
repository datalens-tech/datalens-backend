from __future__ import annotations

import logging
from typing import Any, Dict

from marshmallow import fields
from marshmallow_enum import EnumField

from bi_constants.enums import BIType

from bi_core.db import SchemaColumn
from bi_core.db.native_type_schema import OneOfNativeTypeSchema
from bi_core.us_manager.storage_schemas.base import BaseStorageSchema, CtxKey


LOGGER = logging.getLogger(__name__)


class DataSourceRawSchemaEntryStorageSchema(BaseStorageSchema[SchemaColumn]):
    name = fields.String(required=False, allow_none=True)
    title = fields.String(required=False, allow_none=True)
    description = fields.String(required=False, allow_none=True)

    type = EnumField(BIType, attribute='user_type')
    nullable = fields.Boolean(required=False, allow_none=True)

    lock_aggregation = fields.Boolean(required=False, allow_none=True)
    has_auto_aggregation = fields.Boolean(required=False, allow_none=True)

    native_type = fields.Nested(OneOfNativeTypeSchema, required=False, load_default=None)

    def to_object(self, data: Dict[str, Any]) -> SchemaColumn:
        return SchemaColumn(
            source_id=self.context.get(CtxKey.dsc_id),
            name=data['name'],
            title=data['title'],
            description=data['description'],
            user_type=data['user_type'],
            native_type=data['native_type'],
            nullable=data['nullable'],
            lock_aggregation=data['lock_aggregation'],
            has_auto_aggregation=data['has_auto_aggregation'],
        )
