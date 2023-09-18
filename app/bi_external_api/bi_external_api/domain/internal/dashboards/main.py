from typing import Sequence

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.attrs_model_mapper.base import AttribDescriptor

from ...utils import ensure_tuple
from ..dl_common import (
    DatasetAPIBaseModel,
    EntryInstance,
    EntryScope,
)
from .tab_items import TabItem


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class LayoutItem:
    i: str = attr.ib()
    h: int = attr.ib()
    w: int = attr.ib()
    x: int = attr.ib()
    y: int = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class Connection:
    from_: str = attr.ib(metadata=AttribDescriptor(serialization_key="from").to_meta())
    to: str = attr.ib()
    kind: str = attr.ib(default="ignore")


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class Aliases:
    default: Sequence[Sequence[str]] = attr.ib(converter=ensure_tuple, factory=tuple)


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class Tab:
    id: str = attr.ib()
    title: str = attr.ib()
    items: Sequence[TabItem] = attr.ib(converter=ensure_tuple)
    layout: Sequence[LayoutItem] = attr.ib(converter=ensure_tuple)
    connections: Sequence[Connection] = attr.ib(converter=ensure_tuple)
    aliases: Aliases = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class Dashboard(DatasetAPIBaseModel):
    tabs: Sequence[Tab] = attr.ib(converter=ensure_tuple)

    ignored_keys = {
        "salt",
        "counter",
        "settings",
        "schemeVersion",
        "description",
        "accessDescription",
        "supportDescription",
    }


@attr.s()
class DashInstance(EntryInstance):
    SCOPE = EntryScope.dash

    dash: Dashboard = attr.ib()
