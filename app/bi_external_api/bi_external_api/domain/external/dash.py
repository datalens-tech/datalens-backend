from typing import Sequence, Optional

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.attrs_model_mapper.utils import MText
from bi_external_api.domain.external.dash_elements import DashElement
from bi_external_api.domain.utils import ensure_tuple


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class DashTabItemPlacement:
    x: int = attr.ib()
    y: int = attr.ib()
    h: int = attr.ib()
    w: int = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class DashboardTabItem:
    id: str = attr.ib()
    element: DashElement = attr.ib()
    placement: DashTabItemPlacement = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class IgnoredConnection:
    from_id: str = attr.ib()
    to_id: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class DashboardTab:
    id: str = attr.ib()
    title: str = attr.ib()
    items: Sequence[DashboardTabItem] = attr.ib(converter=ensure_tuple)
    ignored_connections: Optional[Sequence[IgnoredConnection]] = attr.ib(
        converter=ensure_tuple,  # type: ignore
        default=(),
    )


@ModelDescriptor(
    description=MText(
        ru="Сетка: 36 колонок, каждая единица высоты 18px.",
        en="Placement grid: 36 columns, each height unit 18px. X & Y starts from `0`.",
    ),
)
@attr.s(frozen=True, kw_only=True)
class Dashboard:
    tabs: Sequence[DashboardTab] = attr.ib(converter=ensure_tuple)
