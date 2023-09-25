from __future__ import annotations

import itertools
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Optional,
)

import attr

from dl_api_lib.query.formalization.id_gen import IdGenerator
from dl_api_lib.query.formalization.raw_pivot_specs import (
    RawAnnotationPivotRoleSpec,
    RawPivotSpec,
    RawPivotTotalsItemSpec,
    RawPivotTotalsSpec,
)
from dl_api_lib.query.formalization.raw_specs import (
    PlaceholderRef,
    RawQuerySpecUnion,
    RawRoleSpec,
    RawRowRoleSpec,
    RawTemplateRoleSpec,
    spec_is_field_name_pseudo_dimension,
)
from dl_api_lib.request_model.data import PivotDataRequestModel
from dl_api_lib.request_model.normalization.drm_normalizer_base import (
    RequestModelNormalizerBase,
    RequestPartSpecNormalizerBase,
)
from dl_constants.enums import (
    FieldRole,
    PivotRole,
)
import dl_query_processing.exc


if TYPE_CHECKING:
    from dl_api_lib.query.formalization.raw_pivot_specs import RawPivotLegendItem
    from dl_api_lib.query.formalization.raw_specs import RawSelectFieldSpec


@attr.s
class PivotTotalsNormalizerHelper:
    _original_select_specs: list[RawSelectFieldSpec] = attr.ib(kw_only=True)
    _legend_item_id_gen: IdGenerator = attr.ib(kw_only=True)
    _block_id_gen: IdGenerator = attr.ib(kw_only=True)
    _base_block_id: int = attr.ib(kw_only=True)

    # Item types:
    T_DIMENSION: ClassVar[int] = 1
    T_TEMPLATE: ClassVar[int] = 2
    T_MEASURE: ClassVar[int] = 3

    def _get_legend_item_by_id(self, legend_item_id: int) -> RawSelectFieldSpec:
        for item in self._original_select_specs:
            if item.legend_item_id == legend_item_id:
                return item
        raise dl_query_processing.exc.LegendItemReferenceError(f"Unknown legend item: {legend_item_id}")

    def make_role_spec(self, item_type: int) -> RawRoleSpec:
        if item_type is self.T_TEMPLATE:
            return RawTemplateRoleSpec(role=FieldRole.template, template="")
        if item_type is self.T_MEASURE:
            return RawRoleSpec(role=FieldRole.total)
        return RawRowRoleSpec(role=FieldRole.row)

    def resolve_main_legend_item(self, pivot_item: RawPivotLegendItem) -> Optional[RawSelectFieldSpec]:
        # TODO: Some validation would be nice here
        for legend_item_id in pivot_item.legend_item_ids:
            item = self._get_legend_item_by_id(legend_item_id=legend_item_id)
            # We are looking for the item from the original base block
            if item.block_id == self._base_block_id:
                return item
        return None

    def make_legend_item(
        self,
        item_type: int,
        orig_pivot_item: RawPivotLegendItem,
        block_id: int,
    ) -> tuple[Optional[RawSelectFieldSpec], RawPivotLegendItem]:
        """
        Generate raw legend item and an updated version of the given pivot item.
        """
        original_item = self.resolve_main_legend_item(orig_pivot_item)
        if original_item is None:
            return None, orig_pivot_item
        new_role_spec = self.make_role_spec(item_type=item_type)
        legend_item_id = self._legend_item_id_gen.generate_id()
        ref = original_item.ref
        if item_type is self.T_TEMPLATE:
            ref = PlaceholderRef()
        new_item = original_item.clone(
            ref=ref,
            role_spec=new_role_spec,
            legend_item_id=legend_item_id,
            block_id=block_id,
        )
        updated_pivot_item = orig_pivot_item.clone(legend_item_ids=orig_pivot_item.legend_item_ids + [legend_item_id])
        return new_item, updated_pivot_item

    def gen_single_dir_totals(
        self,
        this_level: Optional[int],
        other_level: Optional[int],
        this_dir_dimensions: list[RawPivotLegendItem],
        other_dir_dimensions: list[RawPivotLegendItem],
        measures: list[RawPivotLegendItem],
    ) -> tuple[list[RawSelectFieldSpec], list[RawPivotLegendItem], list[RawPivotLegendItem], list[RawPivotLegendItem],]:
        """
        Generate new items for the legend (raw_query_spec_union)
        and pivot spec.
        """

        block_id = self._block_id_gen.generate_id()

        this_dir_dimensions = this_dir_dimensions[:]
        other_dir_dimensions = other_dir_dimensions[:]
        measures = measures[:]

        # Generate items
        new_block_select_items: list[RawSelectFieldSpec] = []
        for item_idx, orig_pivot_item in enumerate(this_dir_dimensions):
            item_type = self.T_DIMENSION if (this_level is None or item_idx < this_level) else self.T_TEMPLATE
            new_legend_item, updated_pivot_item = self.make_legend_item(
                item_type=item_type,
                orig_pivot_item=orig_pivot_item,
                block_id=block_id,
            )
            if new_legend_item is not None:
                new_block_select_items.append(new_legend_item)
                this_dir_dimensions[item_idx] = updated_pivot_item

        for item_idx, orig_pivot_item in enumerate(other_dir_dimensions):
            item_type = self.T_DIMENSION if (other_level is None or item_idx < other_level) else self.T_TEMPLATE
            new_legend_item, updated_pivot_item = self.make_legend_item(
                item_type=item_type,
                orig_pivot_item=orig_pivot_item,
                block_id=block_id,
            )
            if new_legend_item is not None:
                new_block_select_items.append(new_legend_item)
                other_dir_dimensions[item_idx] = updated_pivot_item

        for item_idx, orig_pivot_item in enumerate(measures):
            if orig_pivot_item.role_spec.role == PivotRole.pivot_annotation:
                # Totals are not annotated
                continue
            item_type = self.T_MEASURE
            new_legend_item, updated_pivot_item = self.make_legend_item(
                item_type=item_type,
                orig_pivot_item=orig_pivot_item,
                block_id=block_id,
            )
            if new_legend_item is not None:
                new_block_select_items.append(new_legend_item)
                measures[item_idx] = updated_pivot_item

        return (
            new_block_select_items,
            this_dir_dimensions,
            other_dir_dimensions,
            measures,
        )


class PivotSpecNormalizer(RequestPartSpecNormalizerBase[RawPivotSpec]):
    DEFAULT_BLOCK_ID: ClassVar[int] = 0

    def _normalize_simple_totals_from_flag(self, pivot_spec: RawPivotSpec) -> RawPivotSpec:
        if pivot_spec.with_totals:
            pivot_totals: Optional[RawPivotTotalsSpec]
            if pivot_spec.totals is None:
                pivot_totals = RawPivotTotalsSpec(
                    rows=[RawPivotTotalsItemSpec(level=0)],
                    columns=[RawPivotTotalsItemSpec(level=0)],
                )
            else:
                pivot_totals = pivot_spec.totals

            pivot_spec = pivot_spec.clone(
                totals=pivot_totals,
                with_totals=None,
            )

        return pivot_spec

    def _normalize_totals(
        self,
        pivot_spec: RawPivotSpec,
        raw_query_spec_union: RawQuerySpecUnion,
        liid_gen: IdGenerator,
    ) -> tuple[RawPivotSpec, RawQuerySpecUnion]:
        raw_pivot_items = pivot_spec.structure
        totals = pivot_spec.totals
        assert totals is not None

        # Verify that there is only one block
        block_ids = raw_query_spec_union.get_unique_block_ids()
        if len(block_ids) > 1:
            raise dl_query_processing.exc.MultipleBlocksUnsupportedError(
                "Incoming multiple blocks not supported in simplified API"
            )
        base_block_id = next(iter(block_ids)) if block_ids else self.DEFAULT_BLOCK_ID
        block_id_gen = IdGenerator(used_ids={base_block_id})

        # Patch original items with explicit block_id
        select_specs: list[RawSelectFieldSpec] = []
        new_item: RawSelectFieldSpec
        for item in raw_query_spec_union.select_specs:
            if spec_is_field_name_pseudo_dimension(item):
                # Measure Names and such - they must not be block-specific
                new_item = item.clone(block_id=None)
            else:
                new_item = item.clone(block_id=base_block_id)
            select_specs.append(new_item)
        order_by_specs = [
            # All global ORDER BY items must be localized to the base block
            # (i.e. can't sort totals)
            item.clone(block_id=base_block_id)
            for item in raw_query_spec_union.order_by_specs
        ]

        def filter_raw_items(role: PivotRole) -> list[RawPivotLegendItem]:
            return [item for item in raw_pivot_items if item.role_spec.role == role]

        def normalize_target_legend_item_ids(item: RawPivotLegendItem) -> RawPivotLegendItem:
            anno_role_spec = item.role_spec
            assert isinstance(anno_role_spec, RawAnnotationPivotRoleSpec)
            if anno_role_spec.target_legend_item_ids is None:
                item = item.clone(
                    role_spec=anno_role_spec.clone(
                        target_legend_item_ids=measure_legend_item_ids,
                    )
                )
            return item

        row_dimensions = filter_raw_items(PivotRole.pivot_row)
        column_dimensions = filter_raw_items(PivotRole.pivot_column)
        strict_measures = filter_raw_items(PivotRole.pivot_measure)
        measure_legend_item_ids = sorted(
            set(itertools.chain.from_iterable(item.legend_item_ids for item in strict_measures))
        )
        annotations = [normalize_target_legend_item_ids(item) for item in filter_raw_items(PivotRole.pivot_annotation)]
        measures = strict_measures + annotations
        other_pivot_items = [
            item
            for item in raw_pivot_items
            if item.role_spec.role
            not in {
                PivotRole.pivot_row,
                PivotRole.pivot_column,
                PivotRole.pivot_measure,
                PivotRole.pivot_annotation,
            }
        ]

        # Check whether either `rows` or `columns` contains only Measure Names
        mname_liids = {item.legend_item_id for item in select_specs if spec_is_field_name_pseudo_dimension(item)}
        non_mname_row_dims = any(
            legend_item_id not in mname_liids for item in row_dimensions for legend_item_id in item.legend_item_ids
        )
        non_mname_column_dims = any(
            legend_item_id not in mname_liids for item in column_dimensions for legend_item_id in item.legend_item_ids
        )

        if not row_dimensions or not non_mname_row_dims:
            totals = totals.clone(rows=[])
        if not column_dimensions or not non_mname_column_dims:
            totals = totals.clone(columns=[])

        if not measures:
            # Totals are not possible in this case
            return pivot_spec, raw_query_spec_union

        helper = PivotTotalsNormalizerHelper(
            original_select_specs=select_specs,
            legend_item_id_gen=liid_gen,
            block_id_gen=block_id_gen,
            base_block_id=base_block_id,
        )

        # 1. "Horizontal" totals
        for row_total_item in totals.rows:
            add_select_specs, row_dimensions, column_dimensions, measures = helper.gen_single_dir_totals(
                this_level=row_total_item.level,
                other_level=None,
                this_dir_dimensions=row_dimensions,
                other_dir_dimensions=column_dimensions,
                measures=measures,
            )
            select_specs += add_select_specs

        # 2. "Vertical" totals
        for column_total_item in totals.columns:
            add_select_specs, column_dimensions, row_dimensions, measures = helper.gen_single_dir_totals(
                this_level=column_total_item.level,
                other_level=None,
                this_dir_dimensions=column_dimensions,
                other_dir_dimensions=row_dimensions,
                measures=measures,
            )
            select_specs += add_select_specs

        # 3. The intersections
        for row_total_item in totals.rows:
            for column_total_item in totals.columns:
                add_select_specs, row_dimensions, column_dimensions, measures = helper.gen_single_dir_totals(
                    this_level=row_total_item.level,
                    other_level=column_total_item.level,
                    this_dir_dimensions=row_dimensions,
                    other_dir_dimensions=column_dimensions,
                    measures=measures,
                )
                select_specs += add_select_specs

        # Make updated clones
        raw_query_spec_union = raw_query_spec_union.clone(
            select_specs=select_specs,
            order_by_specs=order_by_specs,
        )
        pivot_spec = pivot_spec.clone(
            structure=list(
                itertools.chain(
                    column_dimensions,
                    row_dimensions,
                    measures,
                    other_pivot_items,
                )
            ),
            totals=None,  # We've used it all up, so remove it
        )

        return pivot_spec, raw_query_spec_union

    def normalize_spec(
        self,
        spec: RawPivotSpec,
        raw_query_spec_union: RawQuerySpecUnion,
    ) -> tuple[RawPivotSpec, RawQuerySpecUnion]:
        spec, raw_query_spec_union = super().normalize_spec(
            spec=spec,
            raw_query_spec_union=raw_query_spec_union,
        )
        pivot_spec = spec

        liid_gen = self.get_liid_generator(raw_query_spec_union=raw_query_spec_union)

        if pivot_spec.with_totals:
            pivot_spec = self._normalize_simple_totals_from_flag(pivot_spec=pivot_spec)

        if pivot_spec.totals is not None:
            pivot_spec, raw_query_spec_union = self._normalize_totals(
                pivot_spec=pivot_spec,
                raw_query_spec_union=raw_query_spec_union,
                liid_gen=liid_gen,
            )

        return pivot_spec, raw_query_spec_union


class PivotRequestModelNormalizer(RequestModelNormalizerBase[PivotDataRequestModel]):
    """
    Pivot-specific normalizer.

    Expands:
    - simplified totals API
    """

    def normalize_drm(self, drm: PivotDataRequestModel) -> PivotDataRequestModel:
        drm = super().normalize_drm(drm=drm)

        pivot_spec_normalizer = PivotSpecNormalizer()
        pivot_spec, raw_query_spec_union = pivot_spec_normalizer.normalize_spec(
            spec=drm.pivot,
            raw_query_spec_union=drm.raw_query_spec_union,
        )
        if pivot_spec is not drm.pivot or raw_query_spec_union is not drm.raw_query_spec_union:
            drm = drm.clone(
                raw_query_spec_union=raw_query_spec_union,
                pivot=pivot_spec,
            )

        return drm
