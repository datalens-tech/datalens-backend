import enum
from typing import (
    ClassVar,
    Optional,
)

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor

from .avatars import Avatar
from .data_source import DataSource
from .fields import ResultSchemaField


# TODO FIX: BI-3005 Remove when will be extracted to bi-constants
@enum.unique
class DatasetAction(enum.Enum):
    # field
    update_field = "update_field"
    add_field = "add_field"
    delete_field = "delete_field"
    clone_field = "clone_field"

    # source
    update_source = "update_source"
    add_source = "add_source"
    delete_source = "delete_source"
    refresh_source = "refresh_source"

    # source avatar
    update_source_avatar = "update_source_avatar"
    add_source_avatar = "add_source_avatar"
    delete_source_avatar = "delete_source_avatar"

    # source relation
    update_avatar_relation = "update_avatar_relation"
    add_avatar_relation = "add_avatar_relation"
    delete_avatar_relation = "delete_avatar_relation"


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="action")
@attr.s
class Action:
    action: ClassVar[DatasetAction]


@ModelDescriptor()
@attr.s
class ActionDataSourceAdd(Action):
    action = DatasetAction.add_source

    source: DataSource = attr.ib()


@ModelDescriptor()
@attr.s
class ActionAvatarAdd(Action):
    action = DatasetAction.add_source_avatar

    source_avatar: Avatar = attr.ib()
    disable_fields_update: bool = attr.ib()


@ModelDescriptor()
@attr.s(kw_only=True)
class ActionFieldAdd(Action):
    action = DatasetAction.add_field

    field: ResultSchemaField = attr.ib()
    order_index: Optional[int] = attr.ib(default=None)
