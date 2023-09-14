from __future__ import annotations

import abc
from collections import defaultdict
from copy import deepcopy
from itertools import chain
from operator import itemgetter
from typing import (
    TYPE_CHECKING,
    Any,
    Generator,
    TypeVar,
)

import attr

from bi_api_lib.pivot.primitives import PivotHeaderInfo
from bi_constants.enums import (
    FieldRole,
    PivotHeaderRole,
)

if TYPE_CHECKING:
    from bi_api_lib.pivot.primitives import (
        MeasureValues,
        PivotHeader,
    )
    from bi_query_processing.legend.field_legend import Legend


_PIVOT_DATA_FRAME_TV = TypeVar("_PIVOT_DATA_FRAME_TV", bound="PivotDataFrame")


@attr.s
class PivotDataFrame(abc.ABC):
    headers_info: dict[tuple, PivotHeaderInfo] = attr.ib(default=attr.Factory(lambda: defaultdict(PivotHeaderInfo)))

    @abc.abstractmethod
    def iter_columns(self) -> Generator[PivotHeader, None, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def iter_row_headers(self) -> Generator[PivotHeader, None, None]:
        raise NotImplementedError

    @abc.abstractmethod
    def iter_rows(self) -> Generator[tuple[PivotHeader, MeasureValues], None, None]:
        raise NotImplementedError

    def clone(self: _PIVOT_DATA_FRAME_TV, **kwargs: Any) -> _PIVOT_DATA_FRAME_TV:
        kwargs["headers_info"] = kwargs.pop("headers_info", deepcopy(self.headers_info))
        return attr.evolve(self, **kwargs)

    @abc.abstractmethod
    def get_column_count(self) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def get_row_count(self) -> int:
        raise NotImplementedError

    def populate_headers_info(self, legend: Legend) -> None:
        total_liids = {item.legend_item_id for item in legend.list_for_role(FieldRole.template)}
        for header in chain(self.iter_columns(), map(itemgetter(0), self.iter_rows())):
            if any(value.main_legend_item_id in total_liids for value in header.values):
                header.info.role_spec.role = PivotHeaderRole.total
