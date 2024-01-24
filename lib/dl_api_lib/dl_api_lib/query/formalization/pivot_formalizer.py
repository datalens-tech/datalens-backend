from typing import cast

import attr

from dl_api_lib.pivot.pivot_legend import (
    PivotAnnotationRoleSpec,
    PivotDimensionRoleSpec,
    PivotLegend,
    PivotLegendItem,
    PivotMeasureRoleSpec,
    PivotRoleSpec,
)
from dl_api_lib.query.formalization.id_gen import IdGenerator
from dl_api_lib.query.formalization.raw_pivot_specs import (
    RawAnnotationPivotRoleSpec,
    RawDimensionPivotRoleSpec,
    RawPivotLegendItem,
    RawPivotMeasureRoleSpec,
    RawPivotRoleSpec,
    RawPivotSpec,
)
from dl_constants.enums import (
    FieldRole,
    FieldType,
    PivotItemType,
    PivotRole,
)
from dl_constants.internal_constants import (
    DIMENSION_NAME_TITLE,
    MEASURE_NAME_TITLE,
)
from dl_core.us_dataset import Dataset
import dl_query_processing.exc
from dl_query_processing.legend.field_legend import (
    Legend,
    LegendItem,
    MeasureNameObjSpec,
)


DIMENSION_ROLES = {PivotRole.pivot_row, PivotRole.pivot_column}
MEASURE_ROLES = {PivotRole.pivot_measure}
ANNOTATION_ROLES = {PivotRole.pivot_annotation}


@attr.s
class PivotFormalizerBase:
    _legend: Legend = attr.ib(kw_only=True)

    def patch_pivot_legend(self, pivot_legend: PivotLegend, id_gen: IdGenerator) -> None:
        """
        Add pivot legend items if they are required
        to make the pivot table configuration valid.
        This mostly concerns `Measure Name` items.
        """

        measure_cnt = len(pivot_legend.list_for_role(PivotRole.pivot_measure))
        row_cnt = len(pivot_legend.list_for_role(PivotRole.pivot_row))
        column_cnt = len(pivot_legend.list_for_role(PivotRole.pivot_column))

        mname_legend_item_ids = self._legend.get_measure_name_legend_item_ids()
        assert mname_legend_item_ids
        mname_legend_item_id = next(iter(mname_legend_item_ids))

        mname_cnt = len([item for item in pivot_legend.items if item.item_type == PivotItemType.measure_name])

        if row_cnt == 0 or column_cnt == 0:
            # Patch one-sided cases
            if measure_cnt == 0:
                # Only dimensions on one side.
                pass  # Nothing to do here - a pseudo-dimension is added at transformer level
            elif measure_cnt == 1 or mname_cnt == 0:
                # One measure or mnames not yet used,
                # so it is safe to add mnames to the side that's empty
                if column_cnt == 0:  # Columns have priority if both are missing
                    pivot_legend.add_item(
                        PivotLegendItem(
                            pivot_item_id=id_gen.generate_id(),
                            legend_item_ids=[mname_legend_item_id],
                            role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_column),
                            item_type=PivotItemType.measure_name,
                            title=MEASURE_NAME_TITLE,
                        )
                    )
                elif row_cnt == 0:
                    pivot_legend.add_item(
                        PivotLegendItem(
                            pivot_item_id=id_gen.generate_id(),
                            legend_item_ids=[mname_legend_item_id],
                            role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_row),
                            item_type=PivotItemType.measure_name,
                            title=MEASURE_NAME_TITLE,
                        )
                    )
            elif measure_cnt > 1:  # and mname_cnt > 0
                # Can't add another mnames.
                # Must use a dummy dimension
                pass  # Nothing to do here - single-sided pivots are supported

        dname_legend_item_ids = self._legend.get_dimension_name_legend_item_ids()
        assert dname_legend_item_ids
        dname_legend_item_id = next(iter(dname_legend_item_ids))
        dname_cnt = len([item for item in pivot_legend.items if item.item_type == PivotItemType.dimension_name])
        if dname_cnt == 0:
            pivot_legend.add_item(
                PivotLegendItem(
                    pivot_item_id=id_gen.generate_id(),
                    legend_item_ids=[dname_legend_item_id],
                    role_spec=PivotDimensionRoleSpec(role=PivotRole.pivot_info),
                    item_type=PivotItemType.dimension_name,
                    title=DIMENSION_NAME_TITLE,
                )
            )

    def validate_pivot_legend(self, pivot_legend: PivotLegend) -> None:
        measures = pivot_legend.list_for_role(PivotRole.pivot_measure)
        measure_cnt = len(measures)
        mnames = [item for item in pivot_legend.items if item.item_type == PivotItemType.measure_name]
        mname_cnt = len(mnames)
        mname_role = mnames[0].role_spec.role if mnames else None

        measure_sorts_by_column, measure_sorts_by_row = 0, 0
        for item in measures:
            role_spec = cast(PivotMeasureRoleSpec, item.role_spec)
            if role_spec.sorting is None:
                continue
            measure_sorts_by_column += int(role_spec.sorting.column is not None)
            measure_sorts_by_row += int(role_spec.sorting.row is not None)

        if mname_cnt == 0 and measure_cnt > 1:
            # Measure names are not used and there are more than 1 dimensions
            raise dl_query_processing.exc.PivotMeasureNameRequired()
        if mname_cnt > 0 and measure_cnt == 0:
            # Measure names are used, but there are no dimensions
            raise dl_query_processing.exc.PivotMeasureNameForbidden()
        if mname_cnt > 1 and measure_cnt > 1:
            # Measure names are more than once, but there are multiple dimensions
            raise dl_query_processing.exc.PivotMeasureNameDuplicate()
        if measure_sorts_by_column > 1 or measure_sorts_by_row > 1:
            # Multiple sort along the same axis
            raise dl_query_processing.exc.PivotSortingMultipleColumnsOrRows()
        if measure_cnt > 1 and (
            (mname_role == PivotRole.pivot_row and measure_sorts_by_column > 0)
            or (mname_role == PivotRole.pivot_column and measure_sorts_by_row > 0)
        ):
            # Multiple measures, but some sorts are directed across them, which makes no sense
            raise dl_query_processing.exc.PivotSortingAgainstMultipleMeasures()


@attr.s
class PivotFormalizer(PivotFormalizerBase):
    """
    Generates PivotLegend form RawPivotSpec.
    """

    _dataset: Dataset = attr.ib(kw_only=True)
    _allow_dimension_annotations: bool = attr.ib(kw_only=True, default=True)

    def make_pivot_role_spec(self, raw_pivot_role_spec: RawPivotRoleSpec) -> PivotRoleSpec:
        role_spec: PivotRoleSpec
        if raw_pivot_role_spec.role in (PivotRole.pivot_row, PivotRole.pivot_column):
            assert isinstance(raw_pivot_role_spec, RawDimensionPivotRoleSpec)
            role_spec = PivotDimensionRoleSpec(
                role=raw_pivot_role_spec.role,
                direction=raw_pivot_role_spec.direction,
            )
        elif raw_pivot_role_spec.role == PivotRole.pivot_measure:
            assert isinstance(raw_pivot_role_spec, RawPivotMeasureRoleSpec)
            role_spec = PivotMeasureRoleSpec(
                role=raw_pivot_role_spec.role,
                sorting=raw_pivot_role_spec.sorting,
            )
        elif raw_pivot_role_spec.role == PivotRole.pivot_annotation:
            assert isinstance(raw_pivot_role_spec, RawAnnotationPivotRoleSpec)
            role_spec = PivotAnnotationRoleSpec(
                role=raw_pivot_role_spec.role,
                annotation_type=raw_pivot_role_spec.annotation_type,
                target_legend_item_ids=raw_pivot_role_spec.target_legend_item_ids,
            )
        else:
            raise ValueError(f"Unknown pivot role: {raw_pivot_role_spec.role}")

        return role_spec

    def make_item_type(self, legend_items: list[LegendItem], role: PivotRole) -> PivotItemType:
        is_mname_set = {isinstance(legend_item.obj, MeasureNameObjSpec) for legend_item in legend_items}
        assert len(is_mname_set) > 0
        if len(is_mname_set) > 1:
            raise dl_query_processing.exc.PivotItemsIncompatibleError(
                f"Pivot items have incompatible types in the same {role.name} role"
            )
        is_mname = next(iter(is_mname_set))
        if is_mname:
            return PivotItemType.measure_name
        return PivotItemType.stream_item

    def make_item_title(self, legend_items: list[LegendItem], role: PivotRole) -> str:
        title_set = {
            legend_item.title
            for legend_item in legend_items
            # ignore templates for totals
            if legend_item.role_spec.role != FieldRole.template
        }
        assert len(title_set) > 0
        if len(title_set) > 1:
            raise dl_query_processing.exc.PivotItemsIncompatibleError(
                f'Pivot items have incompatible titles: {",".join(sorted(title_set))} ' f"in the same {role.name} role"
            )

        return next(iter(title_set))

    def make_pivot_item(self, raw_pivot_item: RawPivotLegendItem, id_gen: IdGenerator) -> PivotLegendItem:
        role_spec = self.make_pivot_role_spec(raw_pivot_item.role_spec)
        target_legend_items = [
            self._legend.get_item(legend_item_id) for legend_item_id in raw_pivot_item.legend_item_ids
        ]

        has_measures = any(item.field_type == FieldType.MEASURE for item in target_legend_items)
        has_dimensions = any(item.field_type == FieldType.DIMENSION for item in target_legend_items)
        if has_measures and role_spec.role in DIMENSION_ROLES:
            raise dl_query_processing.exc.PivotInvalidRoleLegendError(
                "Measure field cannot be used as a pivot dimension"
            )
        if has_dimensions and role_spec.role in MEASURE_ROLES:
            raise dl_query_processing.exc.PivotInvalidRoleLegendError(
                "Dimension field cannot be used as a pivot measure"
            )
        if not self._allow_dimension_annotations and has_dimensions and role_spec.role in ANNOTATION_ROLES:
            raise dl_query_processing.exc.PivotInvalidRoleLegendError(
                "Dimension field cannot be used as a pivot annotation"
            )

        item_type = self.make_item_type(target_legend_items, role=raw_pivot_item.role_spec.role)

        title: str
        if raw_pivot_item.title is not None:
            title = raw_pivot_item.title
        else:
            title = self.make_item_title(target_legend_items, role=raw_pivot_item.role_spec.role)

        item = PivotLegendItem(
            pivot_item_id=id_gen.generate_id(),
            legend_item_ids=raw_pivot_item.legend_item_ids,
            role_spec=role_spec,
            item_type=item_type,
            title=title,
        )
        return item

    def make_pivot_legend(self, raw_pivot_spec: RawPivotSpec) -> PivotLegend:
        id_gen = IdGenerator()
        items = [self.make_pivot_item(raw_pivot_item, id_gen=id_gen) for raw_pivot_item in raw_pivot_spec.structure]
        pivot_legend = PivotLegend(items=items)

        self.patch_pivot_legend(pivot_legend, id_gen=id_gen)
        self.validate_pivot_legend(pivot_legend)

        return pivot_legend
