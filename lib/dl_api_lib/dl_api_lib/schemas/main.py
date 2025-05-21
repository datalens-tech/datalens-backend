from __future__ import annotations

import datetime
from typing import (
    Any,
    Dict,
    Optional,
)

from flask_restx import (
    Namespace,
    fields,
)
from flask_restx.model import RawModel
from marshmallow import Schema
from marshmallow import fields as ma_fields

from dl_api_lib.schemas.dataset_base import (
    DatasetContentInternalSchema,
    DatasetContentSchema,
)
from dl_constants.enums import (
    DataSourceCreatedVia,
    NotificationLevel,
)
from dl_model_tools.schema.base import BaseSchema
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


def get_api_model(
    ma_schema: Optional[Schema],
    ns: Namespace,
    name: Optional[str] = None,
    schema_fields: Optional[Dict[str, ma_fields.Field]] = None,
) -> RawModel:
    """Generate a ``flask_restx`` schema that will be used for generating Swagger documentation."""

    name = name or ma_schema.__class__.__name__
    schema_fields = schema_fields or (ma_schema._declared_fields if ma_schema is not None else None)

    fields_map = {
        ma_fields.Integer: fields.Integer,
        ma_fields.String: fields.String,
        ma_fields.Boolean: fields.Boolean,
        ma_fields.Raw: fields.Raw,
    }

    def _translate_field(_field: ma_fields.Field) -> fields.Raw:
        """
        Convert marshmallow field instance into a ``flask_restx`` field.
        Do this recursively for complex fields like ``Nested`` and ``List``.
        """

        if isinstance(_field, ma_fields.Nested):
            nested = _field.nested
            nested = nested() if isinstance(nested, type) else nested
            assert isinstance(nested, Schema)
            return fields.Nested(
                get_api_model(nested, ns),
                as_list=_field.many,
                required=_field.required,
            )
        elif isinstance(_field, ma_fields.Enum):
            return fields.String(required=_field.required, enum=[x.name for x in list(_field.enum)])
        elif isinstance(_field, ma_fields.List):
            return fields.List(
                _translate_field(_field.inner),
                required=_field.required,
            )
        elif isinstance(_field, ma_fields.DateTime):
            return fields.DateTime(
                example=datetime.datetime(2018, 1, 1).strftime(_field.format or _field.DEFAULT_FORMAT),
            )
        elif isinstance(_field, ma_fields.Date):
            return fields.Date(
                example=datetime.date(2018, 1, 1).strftime(_field.format or _field.DEFAULT_FORMAT),
            )
        else:
            api_field_type = fields_map.get(type(_field), fields.String)
            return api_field_type(required=_field.required)

    api_model_dict = {}
    if schema_fields is not None:
        for field_name, field in schema_fields.items():
            api_model_dict[field_name] = _translate_field(field)

    return ns.model(name, api_model_dict)


class CreateDatasetSchema(DatasetContentSchema):
    name = ma_fields.String()
    dir_path = ma_fields.String()
    workbook_id = ma_fields.String()
    # TODO FIX: Ensure that not used and remove
    preview = ma_fields.Boolean(load_default=False, required=False)
    created_via = DynamicEnumField(DataSourceCreatedVia, load_default=DataSourceCreatedVia.user)


class CreateDatasetResponseSchema(DatasetContentSchema):
    id = ma_fields.String()


class NormalizedDateTime(ma_fields.DateTime):
    """A DateTime field that also accepts strings (formatted datetimes)"""

    def _serialize(
        self,
        value: str | datetime.datetime,
        attr: Optional[str],
        obj: Any,
        **kwargs: Any,
    ) -> Optional[str | float]:
        if isinstance(value, str):
            value = datetime.datetime.strptime(value.split(".")[0], "%Y-%m-%d %H:%M:%S")
        return super()._serialize(value, attr, obj, **kwargs)


class GetDatasetResponseSchema(DatasetContentSchema):
    id = ma_fields.String()
    name = ma_fields.String()
    pub_operation_id = ma_fields.String()
    row_count = ma_fields.Integer()
    ctime = NormalizedDateTime(format="%Y-%m-%d %H:%M:%S")
    mtime = NormalizedDateTime(format="%Y-%m-%d %H:%M:%S")
    is_favorite = ma_fields.Boolean()
    permissions = ma_fields.Dict(keys=ma_fields.String(), values=ma_fields.Boolean())


class UpdateDatasetSchema(BaseSchema):
    name = ma_fields.String()


class DatasetUpdateSchema(DatasetContentSchema):
    pass


class DatasetCopyRequestSchema(BaseSchema):
    new_key = ma_fields.String(required=True)


class DatasetCopyResponseSchema(DatasetContentSchema):
    id = ma_fields.String(required=True)


class GetDatasetVersionQuerySchema(BaseSchema):
    rev_id = ma_fields.String()


class GetDatasetVersionResponseSchema(GetDatasetResponseSchema):
    key = ma_fields.String()
    workbook_id = ma_fields.String()


class BadRequestResponseSchema(BaseSchema):
    message = ma_fields.String()


class DashSQLBindParam(BaseSchema):
    type_name = ma_fields.String(required=True)
    # value = ma_fields.String(required=True, allow_none=True)
    # ^ Can also be a list of strings, so:
    value = ma_fields.Raw(required=True, allow_none=True)


class DashSQLRequestSchema(BaseSchema):
    sql_query = ma_fields.String(required=True)
    params = ma_fields.Dict(ma_fields.String(), ma_fields.Nested(DashSQLBindParam()), required=False)
    db_params = ma_fields.Dict(ma_fields.String(), ma_fields.String(), required=False)
    connector_specific_params = ma_fields.Dict(ma_fields.String(), ma_fields.String(), required=False)


class IdMappingContentSchema(BaseSchema):
    id_mapping = ma_fields.Dict(ma_fields.String(), ma_fields.String(), required=True)


class DatasetExportRequestSchema(IdMappingContentSchema):
    pass


class NotificationContentSchema(BaseSchema):
    code = ma_fields.String()
    message = ma_fields.String()
    level = ma_fields.Enum(NotificationLevel)


class DatasetExportResponseSchema(BaseSchema):
    class DatasetContentInternalExportSchema(DatasetContentInternalSchema):
        class Meta(DatasetContentInternalSchema.Meta):
            exclude = ("rls",)  # not exporting rls at all, only rls2

        name = ma_fields.String()

    dataset = ma_fields.Nested(DatasetContentInternalExportSchema)
    notifications = ma_fields.Nested(NotificationContentSchema, many=True)


class DatasetContentImportSchema(BaseSchema):
    class DatasetContentInternalImportSchema(DatasetContentInternalSchema):
        class Meta(DatasetContentInternalSchema.Meta):
            exclude = ("rls",)  # not accepting rls at all, only rls2

        name = ma_fields.String()

    dataset = ma_fields.Nested(DatasetContentInternalImportSchema, required=True)
    workbook_id = ma_fields.String(allow_none=True, required=True)


class DatasetImportRequestSchema(IdMappingContentSchema):
    data = ma_fields.Nested(DatasetContentImportSchema, required=True)


class ImportResponseSchema(BaseSchema):
    notifications = ma_fields.Nested(NotificationContentSchema, many=True)
    id = ma_fields.String()
