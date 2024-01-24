from __future__ import annotations

import logging
from typing import (  # It thinks NamedTuple isn't used, but it is  # noqa: F401
    TYPE_CHECKING,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import attr

from dl_api_lib.pivot.pivot_legend import (
    PivotAnnotationRoleSpec,
    PivotLegend,
)
from dl_api_lib.pivot.primitives import (
    DataCell,
    DataCellVector,
    MeasureNameValue,
)
from dl_constants.enums import (
    FieldType,
    PivotRole,
)
from dl_query_processing.legend.field_legend import (
    FieldObjSpec,
    TemplateRoleSpec,
)
from dl_query_processing.merging.primitives import MergedQueryDataRow


if TYPE_CHECKING:
    from dl_api_lib.pivot.hashable_packing import HashableValuePackerBase
    from dl_query_processing.legend.field_legend import Legend


LOGGER = logging.getLogger(__name__)


@attr.s
class DataCellConverter:
    """
    Converts raw data stream into a stream of ``DataCell`` lists.
    """

    _rows: Iterable[MergedQueryDataRow] = attr.ib()
    _cell_packer: HashableValuePackerBase = attr.ib(kw_only=True)
    _legend: Legend = attr.ib(kw_only=True)
    _pivot_legend: PivotLegend = attr.ib(kw_only=True)
    _legend_item_id_map: dict[int, int] = attr.ib(init=False)

    @_legend_item_id_map.default
    def _make_legend_item_id_map(self) -> dict[int, int]:
        main_field_items_by_field_id: dict[tuple[str, int], int] = {}
        main_template_items_by_template: dict[tuple[Optional[str], int], int] = {}

        legend_item_id_map: dict[int, int] = {}
        for item in self._legend.items:
            for pivot_item_id in self._pivot_legend.leg_item_id_to_pivot_item_id_list(item.legend_item_id):
                if item.field_type == FieldType.MEASURE:
                    continue

                if isinstance(item.obj, FieldObjSpec):
                    field_id = item.obj.id
                    field_item_key = (field_id, pivot_item_id)
                    remapped_legend_item_id = main_field_items_by_field_id.get(field_item_key)
                    if remapped_legend_item_id is not None:
                        legend_item_id_map[item.legend_item_id] = remapped_legend_item_id
                    else:
                        main_field_items_by_field_id[field_item_key] = item.legend_item_id

                elif isinstance(item.role_spec, TemplateRoleSpec):
                    template = item.role_spec.template
                    templ_item_key = (template, pivot_item_id)
                    remapped_legend_item_id = main_template_items_by_template.get(templ_item_key)
                    if remapped_legend_item_id is not None:
                        legend_item_id_map[item.legend_item_id] = remapped_legend_item_id
                    else:
                        main_template_items_by_template[templ_item_key] = item.legend_item_id

        return legend_item_id_map

    def _remap_legend_item_id(self, legend_item_id: int) -> int:
        """
        Re-map legend_item_id for dimension values so that
        same values of the same dimension from different blocks
        fall into the same group during pivoting.
        """
        return self._legend_item_id_map.get(legend_item_id, legend_item_id)

    def _iter_rows(self) -> Generator[Sequence[DataCell], None, None]:
        for row in self._rows:
            dcell_row: list[DataCell] = []
            for value, legend_item_id in zip(row.data, row.legend_item_ids):
                pivot_item_id_list = self._pivot_legend.leg_item_id_to_pivot_item_id_list(legend_item_id)
                for pivot_item_id in pivot_item_id_list:
                    cell = DataCell(
                        value=self._cell_packer.pack(value),
                        legend_item_id=self._remap_legend_item_id(legend_item_id),
                        pivot_item_id=pivot_item_id,
                    )
                    dcell_row.append(cell)

            yield dcell_row

    def __iter__(self) -> Iterator[Sequence[DataCell]]:
        return iter(self._iter_rows())


class TransposedDataRow(NamedTuple):
    dimensions: Tuple[DataCellVector, ...]
    value: Optional[DataCellVector]


@attr.s
class MeasureDataTransposer:
    """
    Transposes measure fields within the stream by
    turning multiple measure fields into two pseudo fields:
    1. measure name (string, referencing the MEASURE_NAME legend item);
    2. measure value (original value referencing the original legend item);
    and multiplying the number of rows by the number of measures.
    """

    _dcell_stream: Iterable[Sequence[DataCell]] = attr.ib()
    _pivot_legend: PivotLegend = attr.ib(kw_only=True)
    _cell_packer: HashableValuePackerBase = attr.ib(kw_only=True)

    # Internal
    _dimension_pivot_item_ids: Set[int] = attr.ib(init=False)
    _measure_name_pivot_item_ids: List[int] = attr.ib(init=False)
    _measure_piid_and_name_mask_transp: Sequence[Tuple[int, str]] = attr.ib(init=False)
    _anno_piid_list_by_measure_liid: Dict[int, List[int]] = attr.ib(init=False)
    _should_add_fake_measure: bool = attr.ib(init=False)
    _measure_name_legend_item_id: Optional[int] = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self._dimension_pivot_item_ids = set()
        for role in (PivotRole.pivot_row, PivotRole.pivot_column):
            for item in self._pivot_legend.list_for_role(role):
                self._dimension_pivot_item_ids.add(item.pivot_item_id)

        self._measure_name_pivot_item_ids = self._pivot_legend.get_measure_name_pivot_item_ids()
        measures = self._pivot_legend.list_for_role(PivotRole.pivot_measure)
        anno_items = self._pivot_legend.list_for_role(PivotRole.pivot_annotation)

        self._measure_piid_and_name_mask_transp = tuple(
            # Transposed mask. Each item will generate a new row
            (item.pivot_item_id, item.title)
            for item in self._pivot_legend.list_for_role(PivotRole.pivot_measure)
        )

        self._measure_name_legend_item_id = None
        if self._measure_name_pivot_item_ids:
            self._measure_name_legend_item_id = self._pivot_legend.get_item(
                self._measure_name_pivot_item_ids[0]
            ).legend_item_ids[0]

        # Direct mask for annotations (on a per-measure basis)
        self._anno_piid_list_by_measure_liid = {}
        _anno_mapping = self._anno_piid_list_by_measure_liid  # shortcut for readability
        for measure_item in measures:
            for measure_legend_item_id in measure_item.legend_item_ids:
                if measure_legend_item_id not in _anno_mapping:
                    _anno_mapping[measure_legend_item_id] = []
                for anno_item in anno_items:
                    anno_role_spec = anno_item.role_spec
                    assert isinstance(anno_role_spec, PivotAnnotationRoleSpec)
                    if (
                        anno_role_spec.target_legend_item_ids is None
                        or measure_legend_item_id in anno_role_spec.target_legend_item_ids
                    ):
                        _anno_mapping[measure_legend_item_id].append(anno_item.pivot_item_id)

        self._should_add_fake_measure = not self._measure_piid_and_name_mask_transp
        if self._should_add_fake_measure:
            LOGGER.info("No measures found in legend, so using a fake one")

    @property
    def measure_name_legend_item_id(self) -> int:
        assert self._measure_name_legend_item_id is not None
        return self._measure_name_legend_item_id

    def _expand_row(self, row: Sequence[DataCell]) -> Generator[TransposedDataRow, None, None]:
        # Apply direct dimension mask
        common_dim_part = tuple(
            DataCellVector(cells=(cell,)) for cell in row if cell[2] in self._dimension_pivot_item_ids
        )
        measure_value_dict: Dict[int, DataCell] = {
            cell[2]: cell for cell in row if cell[2] not in self._dimension_pivot_item_ids  # pivot_item_id: cell
        }

        # Expand transposed measure mask
        for measure_piid, measure_name in self._measure_piid_and_name_mask_transp:
            mnames_dim_part = tuple(
                # There can be multiple `measure_name` items
                DataCellVector(
                    cells=(
                        DataCell(  # the measure name cell
                            value=MeasureNameValue(
                                value=self._cell_packer.pack(measure_name),
                                measure_piid=measure_piid,
                            ),
                            legend_item_id=self.measure_name_legend_item_id,
                            pivot_item_id=measure_name_pivot_item_id,
                        ),
                    )
                )
                for measure_name_pivot_item_id in self._measure_name_pivot_item_ids
            )
            dim_part = common_dim_part + mnames_dim_part
            value_cell = measure_value_dict[measure_piid]
            measure_liid = value_cell.legend_item_id
            anno_cells = (
                measure_value_dict[anno_piid] for anno_piid in self._anno_piid_list_by_measure_liid[measure_liid]
            )  # generator!!

            value_vector = DataCellVector(cells=(value_cell, *anno_cells))
            yield TransposedDataRow(dimensions=dim_part, value=value_vector)

        if self._should_add_fake_measure:
            # There are no measures
            yield TransposedDataRow(dimensions=common_dim_part, value=None)

    def _iter_rows(self) -> Generator[TransposedDataRow, None, None]:
        for row in self._dcell_stream:
            yield from self._expand_row(row)

    def __iter__(self) -> Iterator[TransposedDataRow]:
        """Iterable object interface"""
        return iter(self._iter_rows())
