import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from .enums import TextSize
from ..dl_common import DatasetAPIBaseModel


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class TabItemData(DatasetAPIBaseModel):
    """
    Just a marker class to set some type to TabItem.data
    Currently, there are no polymorphic cases for this class.
    """


@ModelDescriptor()
@attr.s(auto_attribs=True)
class TabItemDataTitle(TabItemData):
    text: str
    size: TextSize
    showInTOC: bool


@ModelDescriptor()
@attr.s(auto_attribs=True)
class TabItemDataText(TabItemData):
    text: str
