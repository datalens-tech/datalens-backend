from __future__ import annotations

from marshmallow import fields as ma_fields

from dl_core.components.dependencies.primitives import (
    FieldInterDependencyInfo,
    FieldInterDependencyItem,
)
from dl_core.us_dataset import Dataset
from dl_model_tools.schema.base import DefaultSchema


class ResultSchemaAuxSchema(DefaultSchema):
    TARGET_CLS = Dataset.DataModel.ResultSchemaAux

    class FieldInterDependencyInfoSchema(DefaultSchema):
        TARGET_CLS = FieldInterDependencyInfo

        class FieldInterDependencyItemSchema(DefaultSchema):
            TARGET_CLS = FieldInterDependencyItem

            dep_field_id = ma_fields.String(allow_none=False)
            ref_field_ids = ma_fields.List(ma_fields.String(allow_none=False), allow_none=False)

        deps = ma_fields.List(ma_fields.Nested(FieldInterDependencyItemSchema()), allow_none=False)

    inter_dependencies = ma_fields.Nested(FieldInterDependencyInfoSchema(), allow_none=False)
