from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Mapping,
    Optional,
    Union,
)

from marshmallow import EXCLUDE
from marshmallow import fields as ma_fields
from marshmallow import post_load
from marshmallow_oneofschema import OneOfSchema

from dl_constants.enums import (
    BinaryJoinOperator,
    ConditionPartCalcMode,
    DataSourceType,
    IndexKind,
    JoinConditionType,
    JoinType,
    ManagedBy,
    UserDataType,
)
from dl_core.db import (
    IndexInfo,
    SchemaColumn,
)
from dl_core.db.native_type_schema import OneOfNativeTypeSchema
from dl_core.marshmallow import FrozenSetField
from dl_core.multisource import (
    BinaryCondition,
    ConditionPartDirect,
    ConditionPartFormula,
    ConditionPartResultField,
)
from dl_model_tools.schema.base import BaseSchema
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


if TYPE_CHECKING:
    from dl_core.multisource import ConditionPart


class RawSchemaColumnSchema(BaseSchema):
    name = ma_fields.String()
    title = ma_fields.String()

    native_type = ma_fields.Nested(OneOfNativeTypeSchema, allow_none=True)

    user_type = ma_fields.Enum(UserDataType)
    description = ma_fields.String(dump_default="", allow_none=True)
    has_auto_aggregation = ma_fields.Boolean(dump_default=False, allow_none=True)
    lock_aggregation = ma_fields.Boolean(dump_default=False, allow_none=True)
    nullable = ma_fields.Boolean(dump_default=None, allow_none=True)

    @post_load
    def make_column(self, data: dict, **kwargs: Any) -> SchemaColumn:
        return SchemaColumn(
            name=data["name"],
            title=data["title"],
            user_type=data["user_type"],
            native_type=data["native_type"],
            description=data.get("description", ""),
            has_auto_aggregation=data.get("has_auto_aggregation", False),
            lock_aggregation=data.get("lock_aggregation", False),
            nullable=data["nullable"],
        )


class IndexInfoSchema(BaseSchema):
    columns = ma_fields.List(ma_fields.String, required=True)
    kind = ma_fields.Enum(IndexKind, allow_none=True)

    @post_load
    def make_index_info(self, data: dict, **kwargs: Any) -> IndexInfo:
        columns = tuple(data.pop("columns"))
        return IndexInfo(columns=columns, **data)


class SimpleParametersSchema(BaseSchema):
    pass


class SQLParametersSchema(BaseSchema):
    db_name = ma_fields.String(allow_none=True)
    table_name = ma_fields.String(allow_none=True)
    db_version = ma_fields.String(allow_none=True)


class SchematizedParametersSchema(SQLParametersSchema):
    schema_name = ma_fields.String(allow_none=True)


class SubselectParametersSchema(SimpleParametersSchema):
    subsql = ma_fields.String()


class DataSourceCommonSchema(BaseSchema):
    title = ma_fields.String(required=True)
    connection_id = ma_fields.String(allow_none=True)
    source_type = DynamicEnumField(DataSourceType)
    raw_schema = ma_fields.Nested(RawSchemaColumnSchema, many=True, allow_none=True)
    index_info_set = FrozenSetField(
        ma_fields.Nested(IndexInfoSchema),
        sort_output=True,
        allow_none=True,
        load_default=None,
        dump_default=None,
    )
    parameters = ma_fields.Nested(SimpleParametersSchema)  # redefined in subclasses
    parameter_hash = ma_fields.String(dump_only=True)


class DataSourceTemplateResponseField(ma_fields.Field):
    """
    Optimized version of the `DataSourceTemplateResponseSchema` that does a bare minimum.
    """

    _allowed_keys = frozenset(
        (
            "title",
            "tab_title",
            "connection_id",
            "source_type",
            # In `DataSourceCommonSchema` but not intended for dsrc templates: 'raw_schema'
            "index_info_set",
            "parameters",
            "parameter_hash",
            "id",
            "managed_by",
            "virtual",
            "valid",
            "group",
            "form",
            "disabled",
        )
    )

    def _serialize(self, value: Optional[dict], attr: Optional[str], obj: Any, **kwargs: Any) -> Optional[dict]:
        if value is None:
            return None
        assert isinstance(value, dict)
        allowed_keys = self._allowed_keys
        value = {key: val for key, val in value.items() if key in allowed_keys}
        st = value.get("source_type", None)
        if st is not None:
            value["source_type"] = st.name
        return value

    def _deserialize(self, value: Any, attr: Optional[str], data: Optional[Mapping[str, Any]], **kwargs: Any) -> Any:
        raise Exception("Not Applicable")


class VirtualFlagField(ma_fields.Field):
    def _serialize(self, value: Union[str, ManagedBy, None], attr: Optional[str], obj: Any, **kwargs: Any) -> bool:
        if isinstance(value, str):
            value = ManagedBy[value]
        if value is None:
            return False
        return value != ManagedBy.user


class DataSourceBaseSchema(DataSourceCommonSchema):
    id = ma_fields.String(required=True)
    managed_by = ma_fields.Enum(ManagedBy, allow_none=True, dump_default=ManagedBy.user)
    virtual = VirtualFlagField(attribute="managed_by", dump_only=True)
    valid = ma_fields.Boolean(load_default=True)


class DataSourceTemplateBaseSchema(DataSourceCommonSchema):
    group = ma_fields.List(ma_fields.String(), required=True)
    form = ma_fields.List(ma_fields.Dict(), required=False)
    tab_title = ma_fields.String(load_default=None, required=False)
    disabled = ma_fields.Boolean(load_default=False, required=False)


class SimpleDataSourceSchema(DataSourceBaseSchema):
    pass


class SimpleDataSourceTemplateSchema(DataSourceTemplateBaseSchema):
    pass


class SQLDataSourceSchema(SimpleDataSourceSchema):
    parameters = ma_fields.Nested(SQLParametersSchema)


class SQLDataSourceTemplateSchema(DataSourceTemplateBaseSchema):
    parameters = ma_fields.Nested(SQLParametersSchema)


class SchematizedSQLDataSourceSchema(SQLDataSourceSchema):
    parameters = ma_fields.Nested(SchematizedParametersSchema)


class SchematizedSQLDataSourceTemplateSchema(DataSourceTemplateBaseSchema):
    parameters = ma_fields.Nested(SchematizedParametersSchema)


class SubselectDataSourceSchema(SimpleDataSourceSchema):
    parameters = ma_fields.Nested(SubselectParametersSchema)


class SubselectDataSourceTemplateSchema(DataSourceTemplateBaseSchema):
    parameters = ma_fields.Nested(SubselectParametersSchema)


class SourceAvatarSchema(BaseSchema):
    id = ma_fields.String(required=True)
    source_id = ma_fields.String()
    title = ma_fields.String()
    is_root = ma_fields.Boolean()
    managed_by = ma_fields.Enum(ManagedBy, allow_none=True, dump_default=ManagedBy.user)
    virtual = VirtualFlagField(attribute="managed_by", dump_only=True)
    valid = ma_fields.Boolean(dump_only=True)


class SourceAvatarStrictSchema(SourceAvatarSchema):
    managed_by = ma_fields.Enum(ManagedBy, allow_none=True, dump_default=ManagedBy.user, required=True)


class ConditionPartSchema(BaseSchema):
    calc_mode = ma_fields.Enum(ConditionPartCalcMode, required=True, dump_default=ConditionPartCalcMode.direct)


class ConditionPartDirectSchema(ConditionPartSchema):
    source = ma_fields.String(required=True)

    @post_load
    def make_condition_part(self, data: dict, **kwargs: Any) -> ConditionPartDirect:
        return ConditionPartDirect(source=data["source"])


class ConditionPartFormulaSchema(ConditionPartSchema):
    formula = ma_fields.String(required=True)

    @post_load
    def make_condition_part(self, data: dict, **kwargs: Any) -> ConditionPartFormula:
        return ConditionPartFormula(formula=data["formula"])


class ConditionPartResultFieldSchema(ConditionPartSchema):
    field_id = ma_fields.String(required=True)

    @post_load
    def make_condition_part(self, data: dict, **kwargs: Any) -> ConditionPartResultField:
        return ConditionPartResultField(field_id=data["field_id"])


class ConditionPartGenericSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = False
    type_field = "calc_mode"
    type_schemas = {
        ConditionPartCalcMode.direct.name: ConditionPartDirectSchema,
        ConditionPartCalcMode.formula.name: ConditionPartFormulaSchema,
        ConditionPartCalcMode.result_field.name: ConditionPartResultFieldSchema,
    }

    def get_obj_type(self, obj: ConditionPart) -> str:
        return getattr(obj, self.type_field).name


class AvatarRelationSchema(BaseSchema):
    class JoinConditionSchema(BaseSchema):
        type = ma_fields.Enum(JoinConditionType, attribute="condition_type", required=True)
        operator = ma_fields.Enum(BinaryJoinOperator, required=True)
        left = ma_fields.Nested(ConditionPartGenericSchema, attribute="left_part", required=True)
        right = ma_fields.Nested(ConditionPartGenericSchema, attribute="right_part", required=True)

        @post_load
        def make_condition(self, data: dict, **kwargs: Any) -> BinaryCondition:
            return BinaryCondition(
                operator=data["operator"],
                left_part=data["left_part"],
                right_part=data["right_part"],
            )

    id = ma_fields.String(required=True)
    left_avatar_id = ma_fields.String()
    right_avatar_id = ma_fields.String()
    conditions = ma_fields.Nested(JoinConditionSchema, many=True)
    join_type = ma_fields.Enum(JoinType)
    managed_by = ma_fields.Enum(ManagedBy, allow_none=True, dump_default=ManagedBy.user, load_default=ManagedBy.user)
    virtual = VirtualFlagField(attribute="managed_by", dump_only=True)
    required = ma_fields.Bool(load_default=False, required=False, dump_default=False)
