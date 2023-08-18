from collections.abc import Sequence
from typing import ClassVar

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from . import EntryKind
from .charts import Chart
from .connection import Connection
from .dash import Dashboard
from .dataset_main import Dataset
from ..utils import ensure_tuple


@attr.s(frozen=True)
class EntryInstance:
    kind: ClassVar[EntryKind]

    name: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class DatasetInstance(EntryInstance):
    kind = EntryKind.dataset

    dataset: Dataset = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class ChartInstance(EntryInstance):
    kind = EntryKind.chart

    chart: Chart = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class DashInstance(EntryInstance):
    kind = EntryKind.dashboard

    dashboard: Dashboard = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class ConnectionInstance(EntryInstance):
    kind = EntryKind.connection

    connection: Connection = attr.ib()
    name: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class WorkbookConnectionsOnly:
    connections: Sequence[ConnectionInstance] = attr.ib(converter=ensure_tuple)


@ModelDescriptor()
@attr.s(frozen=True)
class WorkBook:
    datasets: Sequence[DatasetInstance] = attr.ib(converter=ensure_tuple)
    charts: Sequence[ChartInstance] = attr.ib(converter=ensure_tuple)
    dashboards: Sequence[DashInstance] = attr.ib(converter=ensure_tuple)

    @classmethod
    def create_empty(cls) -> "WorkBook":
        return WorkBook(
            datasets=(),
            charts=(),
            dashboards=(),
        )


@ModelDescriptor()
@attr.s(frozen=True)
class WorkbookIndexItem:
    id: str = attr.ib()
    title: str = attr.ib()
