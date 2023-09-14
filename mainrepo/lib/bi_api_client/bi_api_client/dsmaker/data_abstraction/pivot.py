from __future__ import annotations

import abc
from itertools import chain
from typing import (
    Any,
    Generator,
    Iterable,
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
from bi_constants.enums import FieldRole
from bi_constants.internal_constants import MEASURE_NAME_TITLE


@attr.s(frozen=True)
class PivotDataSliceProxy(abc.ABC):
    resp_data: dict = attr.ib(kw_only=True)

    @abc.abstractmethod
    def get_compound_header(self) -> tuple[Any, ...]:
        raise ModuleNotFoundError

    @abc.abstractmethod
    def get_flat_header(self) -> Any:
        raise ModuleNotFoundError


@attr.s(frozen=True)
class PivotDataRowProxy(PivotDataSliceProxy):
    row_idx: int = attr.ib(kw_only=True)

    def get_compound_header(self) -> tuple[Any, ...]:
        row_data = self.resp_data["pivot_data"]["rows"][self.row_idx]["header"]
        return tuple(raw_vect[0][0] for raw_vect in row_data)

    def get_flat_header(self) -> Any:
        row_data = self.resp_data["pivot_data"]["rows"][self.row_idx]["header"]
        assert len(row_data) == 1
        return row_data[0][0][0]


@attr.s(frozen=True)
class PivotDataColumnProxy(PivotDataSliceProxy):
    col_idx: int = attr.ib(kw_only=True)

    def get_compound_header(self) -> tuple[Any, ...]:
        col_data = self.resp_data["pivot_data"]["columns"][self.col_idx]
        return tuple(raw_vect[0][0] for raw_vect in col_data)

    def get_flat_header(self) -> Any:
        col_data = self.resp_data["pivot_data"]["columns"][self.col_idx]
        assert len(col_data) == 1
        return col_data[0][0][0]


@attr.s
class PivotDataAbstraction:
    """
    A data abstraction interface for pivot table responses.
    """

    resp_data: dict = attr.ib(kw_only=True)

    def get_field_legend(self) -> FieldLegend:
        return FieldLegend(self.resp_data["fields"])

    def _iter_all_cells(self) -> Generator[tuple[DataCellTuple, DataItem], None, None]:
        """
        Iterate over all (row x column) dimension value combinations
        and yield `dimensions: measure` pairs.
        Key points:
        - `Measure Name` should be present as a dimension in each such pair.
        - annotation measures are yielded as well as the "main" measure values,
          and annotations have their original `Measure Name` values
        """

        legend = self.get_field_legend()
        columns = self.resp_data["pivot_data"]["columns"]

        def make_cell_from_raw_cell(raw_cell: list[Any]) -> DataCell:
            """Convert raw value pair into a `DataCell` object."""
            return DataCell(
                value=raw_cell[0],
                title=legend.get_title_by_legend_item_id(raw_cell[1]),
            )

        def remove_measure_name_cells(cells: Iterable[DataCell]) -> tuple[DataCell, ...]:
            """Strip dimension sequence of `Measure Name` cells."""
            return tuple(cell for cell in cells if cell.title != MEASURE_NAME_TITLE)

        # Since `Measure Name` might be present once, twice or not at all,
        # we will first remove it, and the add it again manually
        # to normalized the `dimensions -> measure` mapping

        rows = self.resp_data["pivot_data"]["rows"]
        column_value_sets = [
            remove_measure_name_cells(make_cell_from_raw_cell(dim_cell_vector[0]) for dim_cell_vector in column)
            for column in columns
        ]

        for row in rows:
            row_value_set = set(
                remove_measure_name_cells(
                    make_cell_from_raw_cell(dim_cell_vector[0]) for dim_cell_vector in row["header"]
                )
            )

            for col_idx, col_value_set in enumerate(column_value_sets):
                measure_raw_cell_vector = row["values"][col_idx]
                if measure_raw_cell_vector is None:
                    # no data to yield here, so skip
                    continue

                # Iterate over "layers":
                # - 0th is the main value
                # - 1st (is present) is the annotation
                for measure_raw_cell in measure_raw_cell_vector:
                    measure_cell = make_cell_from_raw_cell(measure_raw_cell)

                    measure_liid = measure_raw_cell[1]
                    tags: set[DataItemTag] = set()
                    role = legend.get_item_by_legend_item_id(measure_liid).role_spec.role
                    if role == FieldRole.total:
                        tags.add(DataItemTag.total)

                    # Create measure name cell and add it to dimensions
                    mname_cell = DataCell(value=measure_cell.title, title=MEASURE_NAME_TITLE)

                    dim_tuple = DataCellTuple(
                        cells=tuple(
                            sorted(chain(row_value_set, col_value_set, [mname_cell]), key=lambda cell: cell.title)
                        ),
                    )

                    yield dim_tuple, DataItem(cell=measure_cell, meta=DataItemMeta(tags=frozenset(tags)))

    def get_1d_mapper(self) -> DataCellMapper1D:
        return SimpleDataCellMapper1D(
            cells={dim_tuple: cell for dim_tuple, cell in self._iter_all_cells()},
        )

    def get_row(self, idx: int) -> PivotDataRowProxy:
        return PivotDataRowProxy(resp_data=self.resp_data, row_idx=idx)

    def get_column(self, idx: int) -> PivotDataColumnProxy:
        return PivotDataColumnProxy(resp_data=self.resp_data, col_idx=idx)

    def iter_rows(self) -> Generator[PivotDataRowProxy, None, None]:
        for row_idx in range(len(self.resp_data["pivot_data"]["rows"])):
            yield self.get_row(row_idx)

    def iter_columns(self) -> Generator[PivotDataColumnProxy, None, None]:
        for col_idx in range(len(self.resp_data["pivot_data"]["columns"])):
            yield self.get_column(col_idx)

    def get_flat_row_headers(self) -> list[Any]:
        return [row.get_flat_header() for row in self.iter_rows()]

    def get_flat_column_headers(self) -> list[Any]:
        return [col.get_flat_header() for col in self.iter_columns()]

    def get_compound_row_headers(self) -> list[tuple[Any, ...]]:
        return [row.get_compound_header() for row in self.iter_rows()]

    def get_compound_column_headers(self) -> list[tuple[Any, ...]]:
        return [col.get_compound_header() for col in self.iter_columns()]

    @classmethod
    def from_response(cls, response: HttpDataApiResponse) -> PivotDataAbstraction:
        assert response.api_version == "v2"
        return cls(resp_data=response.data)
