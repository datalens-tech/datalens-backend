from __future__ import annotations

import logging
from typing import Any

from marshmallow import EXCLUDE
from marshmallow import fields as ma_fields
from marshmallow_oneofschema import OneOfSchema

from dl_api_connector.api_schema.source import DataSourceSchema
from dl_api_connector.api_schema.source_base import (
    AvatarRelationSchema,
    SourceAvatarSchema,
)
from dl_api_lib.enums import (
    DatasetAction,
    DatasetSettingName,
)
from dl_api_lib.request_model.data import (
    AddField,
    AddUpdateObligatoryFilterAction,
    AddUpdateSourceAction,
    AvatarActionBase,
    CloneField,
    DeleteField,
    DeleteObligatoryFilter,
    DeleteObligatoryFilterAction,
    FieldAction,
    RelationActionBase,
    ReplaceConnection,
    ReplaceConnectionAction,
    SourceActionBase,
    UpdateDescriptionAction,
    UpdateField,
    UpdateSettingAction,
)
from dl_api_lib.schemas.filter import ObligatoryFilterSchema
from dl_api_lib.schemas.parameters import ParameterValueConstraintSchema
from dl_constants.enums import (
    AggregationFunction,
    CalcMode,
    UserDataType,
)
from dl_model_tools.schema.base import (
    BaseSchema,
    DefaultValidateSchema,
)
from dl_model_tools.schema.typed_values import (
    ValueSchema,
    WithNestedValueSchema,
)


LOGGER = logging.getLogger(__name__)


class ActionBaseSchema(BaseSchema):
    action = ma_fields.Enum(DatasetAction, required=True)
    order_index = ma_fields.Integer(load_default=0, required=False)


class FieldActionBaseSchema(ActionBaseSchema, DefaultValidateSchema[FieldAction]):
    class FieldBaseSchema(BaseSchema):
        guid = ma_fields.String()
        strict = ma_fields.Boolean(required=False, load_default=False)

    field = ma_fields.Nested(FieldBaseSchema, required=True)


class UpdateFieldActionSchema(FieldActionBaseSchema, DefaultValidateSchema[FieldAction]):
    TARGET_CLS = FieldAction

    class UpdateFieldBaseSchema(WithNestedValueSchema, FieldActionBaseSchema.FieldBaseSchema):
        TYPE_FIELD_NAME = "cast"
        source = ma_fields.String()
        calc_mode = ma_fields.Enum(CalcMode)
        hidden = ma_fields.Boolean()
        description = ma_fields.String()
        aggregation = ma_fields.Enum(AggregationFunction)
        formula = ma_fields.String()
        guid_formula = ma_fields.String()
        cast = ma_fields.Enum(UserDataType, allow_none=True)
        avatar_id = ma_fields.String(allow_none=True)
        new_id = ma_fields.String(allow_none=True)
        default_value = ma_fields.Nested(ValueSchema, allow_none=True)
        value_constraint = ma_fields.Nested(ParameterValueConstraintSchema, allow_none=True)
        template_enabled = ma_fields.Bool(allow_none=True)
        ui_settings = ma_fields.String()

    class UpdateFieldSchema(UpdateFieldBaseSchema, DefaultValidateSchema[UpdateField]):
        TARGET_CLS = UpdateField
        title = ma_fields.String()

    field = ma_fields.Nested(UpdateFieldSchema, required=False)


class AddFieldActionSchema(UpdateFieldActionSchema):
    TARGET_CLS = FieldAction

    class AddFieldSchema(UpdateFieldActionSchema.UpdateFieldBaseSchema, DefaultValidateSchema[AddField]):
        TARGET_CLS = AddField
        title = ma_fields.String(required=True)

    field = ma_fields.Nested(AddFieldSchema, required=False)


class CloneFieldActionSchema(FieldActionBaseSchema, DefaultValidateSchema[FieldAction]):
    TARGET_CLS = FieldAction

    class CloneFieldSchema(FieldActionBaseSchema.FieldBaseSchema, DefaultValidateSchema[CloneField]):
        TARGET_CLS = CloneField

        title = ma_fields.String()
        from_guid = ma_fields.String()
        aggregation = ma_fields.Enum(AggregationFunction, allow_none=True)
        cast = ma_fields.Enum(UserDataType, allow_none=True)

    field = ma_fields.Nested(CloneFieldSchema, required=True)


class DeleteFieldActionSchema(FieldActionBaseSchema):
    TARGET_CLS = FieldAction

    class DeleteFieldSchema(FieldActionBaseSchema.FieldBaseSchema, DefaultValidateSchema[DeleteField]):
        TARGET_CLS = DeleteField

    field = ma_fields.Nested(DeleteFieldSchema, required=True)


class SourceActionBaseSchema(ActionBaseSchema, DefaultValidateSchema[SourceActionBase]):
    class SourceBaseSchema(BaseSchema):
        id = ma_fields.String()

    source = ma_fields.Nested(SourceBaseSchema, required=True)


class AddUpdateSourceActionSchema(SourceActionBaseSchema, DefaultValidateSchema[SourceActionBase]):
    TARGET_CLS = AddUpdateSourceAction

    source = ma_fields.Nested(DataSourceSchema, required=True)


class DeleteSourceActionSchema(SourceActionBaseSchema, DefaultValidateSchema[SourceActionBase]):
    TARGET_CLS = SourceActionBase

    pass


class RefreshSourceActionSchema(SourceActionBaseSchema, DefaultValidateSchema[SourceActionBase]):
    TARGET_CLS = SourceActionBase

    class RefreshSourceSchema(SourceActionBaseSchema.SourceBaseSchema):
        force_update_fields = ma_fields.Boolean(load_default=False)

    source = ma_fields.Nested(RefreshSourceSchema, required=True)


class AvatarActionBaseSchema(ActionBaseSchema):
    class AvatarBaseSchema(BaseSchema):
        id = ma_fields.String()

    source_avatar = ma_fields.Nested(AvatarBaseSchema, required=True)
    # Should be used by external API only
    disable_fields_update = ma_fields.Boolean(load_default=False, dump_default=False, required=False, allow_none=False)


class AddUpdateAvatarActionSchema(AvatarActionBaseSchema, DefaultValidateSchema[AvatarActionBase]):
    TARGET_CLS = AvatarActionBase
    source_avatar = ma_fields.Nested(SourceAvatarSchema, required=True)


class DeleteAvatarActionSchema(AvatarActionBaseSchema, DefaultValidateSchema[AvatarActionBase]):
    TARGET_CLS = AvatarActionBase
    pass


class RelationActionBaseSchema(ActionBaseSchema):
    class RelationBaseSchema(BaseSchema):
        id = ma_fields.String()

    avatar_relation = ma_fields.Nested(RelationBaseSchema, required=True)


class AddUpdateRelationActionSchema(RelationActionBaseSchema, DefaultValidateSchema[RelationActionBase]):
    TARGET_CLS = RelationActionBase
    avatar_relation = ma_fields.Nested(AvatarRelationSchema, required=True)


class DeleteRelationActionSchema(RelationActionBaseSchema, DefaultValidateSchema[RelationActionBase]):
    TARGET_CLS = RelationActionBase
    pass


class ReplaceConnectionActionSchema(ActionBaseSchema, DefaultValidateSchema[ReplaceConnectionAction]):
    TARGET_CLS = ReplaceConnectionAction

    class ReplaceConnectionSchema(DefaultValidateSchema[ReplaceConnection]):
        TARGET_CLS = ReplaceConnection
        id = ma_fields.String(allow_none=False, required=True)
        new_id = ma_fields.String(allow_none=False, required=True)

    connection = ma_fields.Nested(ReplaceConnectionSchema, required=False)


class ObligatoryFilterActionBaseSchema(ActionBaseSchema):
    class ObligatoryFilterBaseSchema(BaseSchema):
        id = ma_fields.String(allow_none=False, required=True)

    obligatory_filter = ma_fields.Nested(ObligatoryFilterBaseSchema, required=True)


class AddUpdateObligatoryFilterActionSchema(
    ObligatoryFilterActionBaseSchema, DefaultValidateSchema[AddUpdateObligatoryFilterAction]
):
    TARGET_CLS = AddUpdateObligatoryFilterAction
    obligatory_filter = ma_fields.Nested(ObligatoryFilterSchema, required=True)


class DeleteObligatoryFilterActionSchema(
    ObligatoryFilterActionBaseSchema, DefaultValidateSchema[DeleteObligatoryFilterAction]
):
    TARGET_CLS = DeleteObligatoryFilterAction

    class DeleteObligatoryFilterSchema(
        ObligatoryFilterActionBaseSchema.ObligatoryFilterBaseSchema, DefaultValidateSchema[DeleteObligatoryFilter]
    ):
        TARGET_CLS = DeleteObligatoryFilter
        id = ma_fields.String(allow_none=False, required=True)

    obligatory_filter = ma_fields.Nested(DeleteObligatoryFilterSchema, required=True)


class UpdateSettingActionSchema(ActionBaseSchema, DefaultValidateSchema[UpdateSettingAction]):
    TARGET_CLS = UpdateSettingAction

    class SettingSchema(DefaultValidateSchema[UpdateSettingAction.Setting]):
        TARGET_CLS = UpdateSettingAction.Setting

        name = ma_fields.Enum(DatasetSettingName, allow_none=False, required=True)
        value = ma_fields.Boolean(allow_none=False, required=True)

    setting = ma_fields.Nested(SettingSchema, required=True)


class UpdateDescriptionActionSchema(ActionBaseSchema, DefaultValidateSchema[UpdateDescriptionAction]):
    TARGET_CLS = UpdateDescriptionAction
    description = ma_fields.String(allow_none=False, required=True)


class ActionSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = False
    type_field = "action"
    type_schemas = {
        # legacy  TODO: remove
        DatasetAction.add.name: AddFieldActionSchema,
        DatasetAction.update.name: UpdateFieldActionSchema,
        DatasetAction.delete.name: DeleteFieldActionSchema,
        # fields
        DatasetAction.add_field.name: AddFieldActionSchema,
        DatasetAction.update_field.name: UpdateFieldActionSchema,
        DatasetAction.delete_field.name: DeleteFieldActionSchema,
        DatasetAction.clone_field.name: CloneFieldActionSchema,
        # sources
        DatasetAction.add_source.name: AddUpdateSourceActionSchema,
        DatasetAction.update_source.name: AddUpdateSourceActionSchema,
        DatasetAction.delete_source.name: DeleteSourceActionSchema,
        DatasetAction.refresh_source.name: RefreshSourceActionSchema,
        # avatars
        DatasetAction.add_source_avatar.name: AddUpdateAvatarActionSchema,
        DatasetAction.update_source_avatar.name: AddUpdateAvatarActionSchema,
        DatasetAction.delete_source_avatar.name: DeleteAvatarActionSchema,
        # relations
        DatasetAction.add_avatar_relation.name: AddUpdateRelationActionSchema,
        DatasetAction.update_avatar_relation.name: AddUpdateRelationActionSchema,
        DatasetAction.delete_avatar_relation.name: DeleteRelationActionSchema,
        # connections
        DatasetAction.replace_connection.name: ReplaceConnectionActionSchema,
        # filters
        DatasetAction.add_obligatory_filter.name: AddUpdateObligatoryFilterActionSchema,
        DatasetAction.update_obligatory_filter.name: AddUpdateObligatoryFilterActionSchema,
        DatasetAction.delete_obligatory_filter.name: DeleteObligatoryFilterActionSchema,
        # settings
        DatasetAction.update_setting.name: UpdateSettingActionSchema,
        # description
        DatasetAction.update_description.name: UpdateDescriptionActionSchema,
    }

    def get_obj_type(self, obj: dict[str, Any]) -> str:
        return obj[self.type_field].name
