from typing import Sequence

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.attrs_model_mapper.base import AttribDescriptor
from bi_external_api.structs.mappings import FrozenMappingStrToStrOrStrSeq

from ...utils import ensure_tuple
from ..dl_common import IntModelTags
from .tab_item_data import TabItemData


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class WidgetTabItem:
    id: str = attr.ib()
    title: str = attr.ib()
    params: FrozenMappingStrToStrOrStrSeq = attr.ib()
    chartId: str = attr.ib(metadata=AttribDescriptor(tags=frozenset({IntModelTags.chart_id})).to_meta())
    isDefault: bool = attr.ib(default=True)
    autoHeight: bool = attr.ib(default=False)
    description: str = attr.ib(default="")


@ModelDescriptor()
@attr.s()
class TabItemDataWidget(TabItemData):
    hideTitle: bool = attr.ib()
    tabs: Sequence[WidgetTabItem] = attr.ib(converter=ensure_tuple)
