from typing import ClassVar

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.structs.mappings import FrozenMappingStrToStrOrStrSeq
from .tab_item_data_widget import TabItemDataWidget
from .enums import TabItemType
from .tab_item_data import TabItemData, TabItemDataText, TabItemDataTitle
from .tab_item_data_control import ControlData
from ..dl_common import DatasetAPIBaseModel


# TODO FIX: Add order field & check it behaviour
@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="type")
@attr.s(frozen=True, kw_only=True)
class TabItem(DatasetAPIBaseModel):
    type: ClassVar[TabItemType]

    id: str = attr.ib()
    namespace: str = attr.ib(default="default")

    data: TabItemData = attr.ib()

    ignore_not_declared_fields = True


@ModelDescriptor()
@attr.s()
class ItemTitle(TabItem):
    type = TabItemType.title

    data: TabItemDataTitle = attr.ib()


@ModelDescriptor()
@attr.s()
class ItemText(TabItem):
    type = TabItemType.text

    data: TabItemDataText = attr.ib()


@ModelDescriptor()
@attr.s()
class ItemWidget(TabItem):
    type = TabItemType.widget

    data: TabItemDataWidget = attr.ib()


@ModelDescriptor()
@attr.s()
class ItemControl(TabItem):
    type = TabItemType.control

    data: ControlData = attr.ib()
    defaults: FrozenMappingStrToStrOrStrSeq = attr.ib()
