from __future__ import annotations

from itertools import chain
from typing import (
    AbstractSet,
    Any,
    Generator,
    Optional,
)

import attr

from bi_api_client.dsmaker.api.data_api import HttpDataApiResponse
from bi_api_client.dsmaker.data_abstraction.legend import FieldLegend
from bi_api_client.dsmaker.data_abstraction.mapping_base import (
    DataCellMapper1D,
    SimpleDataCellMapper1D,
)
from bi_api_client.dsmaker.data_abstraction.primitives import (
    DataCell,
    DataCellTuple,
    DataItem,
    DataItemMeta,
    DataItemTag,
)
from bi_constants.enums import (
    FieldRole,
    FieldType,
)
from bi_constants.internal_constants import MEASURE_NAME_TITLE


@attr.s(frozen=True)
class ResultRawDataRow:
    _raw_row_data: dict[str, Any] = attr.ib()

    @property
    def data(self) -> list[Any]:
        return self._raw_row_data["data"]

    @property
    def legend(self) -> list[int]:
        return self._raw_row_data["legend"]


@attr.s(frozen=True)
class ResultRowSplitter:
    _field_legend: FieldLegend = attr.ib(kw_only=True)
    _dimension_liids: AbstractSet[int] = attr.ib(kw_only=True)
    _measure_liids: AbstractSet[int] = attr.ib(kw_only=True)

    @_measure_liids.default
    def _make_measure_liids(self) -> set[int]:
        return {field.legend_item_id for field in self._field_legend.fields if field.field_type == FieldType.MEASURE}

    @_dimension_liids.default
    def _make_dimension_liids(self) -> set[int]:
        return {field.legend_item_id for field in self._field_legend.fields if field.field_type == FieldType.DIMENSION}

    def _make_and_filter_cells(
        self,
        raw_row: ResultRawDataRow,
        legend_item_ids: AbstractSet[int],
    ) -> Generator[tuple[DataCell, int], None, None]:
        for liid, value in zip(raw_row.legend, raw_row.data):
            if liid in legend_item_ids:
                yield DataCell(value=value, title=self._field_legend.get_title_by_legend_item_id(liid)), liid

    def split_row_by_measures(self, raw_row: ResultRawDataRow) -> Generator[tuple[DataCellTuple, DataCell], None, None]:
        """
        Split raw data row into an iterable (generator) of pairs:
        1. a `DataCellTuple` instance containing all of the dimension values, including `Measure Name`,
        2. a `DataCell` containing the measure value.
        """

        if not self._measure_liids:
            return

        dimension_cells = tuple(cell for cell, liid in self._make_and_filter_cells(raw_row, self._dimension_liids))
        measure_cells_and_liids = self._make_and_filter_cells(raw_row, self._measure_liids)

        for measure_cell, measure_liid in measure_cells_and_liids:
            tags: set[DataItemTag] = set()
            role = self._field_legend.get_item_by_legend_item_id(measure_liid).role_spec.role
            if role == FieldRole.total:
                tags.add(DataItemTag.total)

            mname_cell = DataCell(value=measure_cell.title, title=MEASURE_NAME_TITLE)
            dim_tuple = DataCellTuple(
                tuple(sorted(chain(dimension_cells + (mname_cell,)), key=lambda cell: cell.title))
            )
            yield dim_tuple, DataItem(cell=measure_cell, meta=DataItemMeta(tags=frozenset(tags)))


@attr.s
class ResultDataAbstraction:
    """
    A data abstraction interface for regular data API v2 responses.
    """

    resp_data: dict = attr.ib(kw_only=True)

    def get_field_legend(self) -> FieldLegend:
        return FieldLegend(self.resp_data["fields"])

    @property
    def _raw_rows(self) -> list[dict]:
        assert len(self.resp_data["result_data"]) == 1, "Only responses with one data array are supported"
        return self.resp_data["result_data"][0]["rows"]

    def iter_raw_rows(self) -> Generator[ResultRawDataRow, None, None]:
        for raw_row_data in self._raw_rows:
            yield ResultRawDataRow(raw_row_data)

    def _iter_split_rows_by_measure(
        self,
        dimension_liids: Optional[frozenset[int]] = None,
        measure_liids: Optional[frozenset[int]] = None,
    ) -> Generator[tuple[DataCellTuple, DataCell], None, None]:
        liid_sets = {}
        if dimension_liids is not None:
            liid_sets["dimension_liids"] = dimension_liids
        if measure_liids is not None:
            liid_sets["measure_liids"] = measure_liids

        splitter = ResultRowSplitter(field_legend=self.get_field_legend(), **liid_sets)
        for raw_row in self.iter_raw_rows():
            yield from splitter.split_row_by_measures(raw_row)

    def get_1d_mapper(
        self,
        dimension_liids: Optional[frozenset[int]] = None,
        measure_liids: Optional[frozenset[int]] = None,
    ) -> DataCellMapper1D:
        return SimpleDataCellMapper1D(
            cells={
                dim_tuple: cell
                for dim_tuple, cell in self._iter_split_rows_by_measure(
                    dimension_liids=dimension_liids,
                    measure_liids=measure_liids,
                )
            },
        )

    @classmethod
    def from_response(cls, response: HttpDataApiResponse) -> ResultDataAbstraction:
        assert response.api_version == "v2"
        return cls(resp_data=response.data)
