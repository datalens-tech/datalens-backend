import abc
from contextlib import ExitStack
from typing import Sequence, TypeVar, Type, Generic, ClassVar, final, Optional

import attr

from bi_external_api.converter.charts.coloring_converters import MeasureColoringSpecConverter
from bi_external_api.converter.charts.ds_field_resolvers import MultiDatasetFieldResolver
from bi_external_api.converter.charts.utils import IntVisPack, get_malformed_ext_unsuitable_field, FieldShapeConverter
from bi_external_api.converter.charts.visualization_accessor import InternalVisualizationAccessor
from bi_external_api.converter.converter_exc import ConstraintViolationError, NotSupportedYet
from bi_external_api.converter.converter_exc_composer import ConversionErrHandlingContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import charts
from bi_external_api.structs.mappings import FrozenStrMapping

_VIS_CONV_T = TypeVar("_VIS_CONV_T", bound="VisualizationConverter")

_EXT_VIS_T = TypeVar("_EXT_VIS_T", bound=ext.Visualization)

_F_COLORING_T = TypeVar("_F_COLORING_T", bound=ext.FieldColoring)


@attr.s()
class VisualizationConverter(Generic[_EXT_VIS_T], metaclass=abc.ABCMeta):
    EXT_VIS_CLS: ClassVar[Type[_EXT_VIS_T]]
    INT_VIS_ID: ClassVar[charts.VisualizationId]

    _dataset_field_resolver: MultiDatasetFieldResolver = attr.ib()
    _err_handling_ctx: ConversionErrHandlingContext = attr.ib()

    # BI-3373: Workaround for circular charts, which uses colors as dimensions in UI
    _set_colors_during_ext_to_int: ClassVar[bool] = True

    @classmethod
    def create(
            cls: Type[_VIS_CONV_T],
            dataset_field_resolver: MultiDatasetFieldResolver,
            err_handling_ctx: ConversionErrHandlingContext,
    ) -> _VIS_CONV_T:
        return cls(
            dataset_field_resolver=dataset_field_resolver,
            err_handling_ctx=err_handling_ctx,
        )

    @abc.abstractmethod
    def convert_ext_to_int(self, ext_v: ext.Visualization) -> IntVisPack:
        raise NotImplementedError()

    @abc.abstractmethod
    def convert_int_to_ext(self, int_v_pack: IntVisPack) -> ext.Visualization:
        raise NotImplementedError()


@attr.s()
class UnsupportedVisualizationConverter(VisualizationConverter[ext.UnsupportedVisualization]):
    def convert_ext_to_int(self, ext_v: ext.Visualization) -> IntVisPack:
        raise ConstraintViolationError("Unsupported visualizations can not be handled.")

    def convert_int_to_ext(self, int_v_pack: IntVisPack) -> ext.Visualization:
        with self._err_handling_ctx.postpone_error_in_current_path():
            int_vis = int_v_pack.vis
            vis_name_for_error: str

            if int_vis is not None:
                vis_name_for_error = int_vis.id.name
            else:
                if int_v_pack.js is not None:
                    vis_name_for_error = "Chart Editor"
                else:
                    vis_name_for_error = "unknown"

            raise NotSupportedYet(f"Visualization type not supported: {vis_name_for_error!r}")

        return ext.UnsupportedVisualization(message=f"Unsupported visualization: {vis_name_for_error!r}")


class VisualizationConverterBase(VisualizationConverter[_EXT_VIS_T], metaclass=abc.ABCMeta):
    def field_ref_to_placeholder_item(self, ref: ext.ChartField) -> charts.ChartField:
        return self.convert_field_source_ext_to_int(ref.source)

    def field_ref_seq_to_placeholder_item_seq(
            self,
            ref_seq: Sequence[ext.ChartField],
            path_elem: str,
            single_element: bool = False,
    ) -> Sequence[charts.ChartField]:
        return self.convert_field_source_seq_ext_to_int(
            [ref.source for ref in ref_seq],
            path_elem=path_elem,
            single_element=single_element,
        )

    def convert_field_source_ext_to_int(self, src: ext.ChartFieldSource) -> charts.ChartField:
        return self._dataset_field_resolver.get_int_chart_field_by_ext_cf_source(src)

    def convert_field_source_seq_ext_to_int(
            self,
            src_seq: Sequence[ext.ChartFieldSource],
            path_elem: str,
            single_element: bool = False,
    ) -> Sequence[charts.ChartField]:
        if single_element:
            assert len(src_seq) == 1

        ret: list[charts.ChartField] = []

        with self._err_handling_ctx.push_path(path_elem):
            for idx, src in enumerate(src_seq):
                with ExitStack() as stack:
                    if single_element:
                        stack.enter_context(
                            self._err_handling_ctx.postpone_error_in_current_path()
                        )
                    else:
                        stack.enter_context(
                            self._err_handling_ctx.postpone_error_with_path(path_elem=str(idx))
                        )
                    ret.append(self.convert_field_source_ext_to_int(src))

        return ret

    def placeholder_item_to_ext_field_ref(self, phi: charts.ChartField) -> ext.ChartField:
        return self._dataset_field_resolver.get_ext_chart_field_by_int_field(phi)

    def placeholder_item_seq_to_ext_field_ref_seq(
            self,
            phi_seq: Sequence[charts.ChartField]
    ) -> Sequence[ext.ChartField]:
        return [self.placeholder_item_to_ext_field_ref(phi) for phi in phi_seq]

    @abc.abstractmethod
    def _convert_ext_to_int(self, ext_v: _EXT_VIS_T) -> charts.Visualization:
        raise NotImplementedError()

    @final
    def convert_ext_to_int(self, ext_v: ext.Visualization) -> IntVisPack:
        sort_seq = ext_v.get_sort()
        ext_coloring = ext_v.get_coloring()
        ext_dim_shaping = ext_v.get_dimension_shaping()
        assert isinstance(ext_v, self.EXT_VIS_CLS)

        int_color_fields: Sequence[charts.ChartField] = ()
        int_color_config: Optional[charts.ColorConfig] = None
        int_shape_fields: Sequence[charts.ChartField] = ()
        int_shape_config: Optional[charts.ShapeConfig] = None

        if ext_coloring is not None:
            # TODO FIX: BI-3005 fetch prop name from ext_v.get_coloring()
            #  or make field name common for all visualizations
            with self._err_handling_ctx.postpone_error_with_path("coloring"):
                int_color_fields, int_color_config = self.convert_colors_ext_to_int(ext_coloring)

        if ext_dim_shaping is not None:
            # TODO FIX: BI-3005 fetch prop name from ext_v.get_shaping()
            #  or make field name common for all visualizations
            with self._err_handling_ctx.postpone_error_with_path("shaping"):
                int_shape_fields, int_shape_config = self.convert_dimensions_shapes_ext_to_int(ext_dim_shaping)

        return IntVisPack(
            vis=self._convert_ext_to_int(ext_v),
            js=None,
            sort=self.convert_sort_seq_ext_to_int(sort_seq),
            colors=int_color_fields if self._set_colors_during_ext_to_int else (),
            colors_config=int_color_config,
            shapes=int_shape_fields,
            shapes_config=int_shape_config,
        )

    def convert_sort_ext_to_int(self, ext_sort: ext.ChartSort) -> charts.ChartFieldSort:
        chart_field = self.field_ref_to_placeholder_item(
            ext.ChartField(source=ext_sort.source)
        )
        int_dir = {
            ext.SortDirection.DESC: charts.SortDirection.DESC,
            ext.SortDirection.ASC: charts.SortDirection.ASC,
        }[ext_sort.direction]
        return charts.ChartFieldSort(
            direction=int_dir,
            **attr.asdict(chart_field, recurse=False)
        )

    def convert_sort_int_to_ext(self, int_sort: charts.ChartFieldSort) -> ext.ChartSort:
        ext_field = self.placeholder_item_to_ext_field_ref(int_sort)
        int_dir = int_sort.direction

        if int_dir is None:
            ext_dir = ext.SortDirection.DESC
        else:
            ext_dir = {
                charts.SortDirection.DESC: ext.SortDirection.DESC,
                charts.SortDirection.ASC: ext.SortDirection.ASC,
            }[int_dir]

        return ext.ChartSort(
            source=ext_field.source,
            direction=ext_dir
        )

    def convert_sort_seq_ext_to_int(
            self,
            ext_sort_seq: Sequence[ext.ChartSort]
    ) -> Sequence[charts.ChartFieldSort]:
        return [self.convert_sort_ext_to_int(ext_sort) for ext_sort in ext_sort_seq]

    def convert_sort_seq_int_to_ext(
            self,
            int_sort_seq: Sequence[charts.ChartFieldSort]
    ) -> Sequence[ext.ChartSort]:
        return [self.convert_sort_int_to_ext(int_sort) for int_sort in int_sort_seq]

    def convert_colors_ext_to_int(
            self,
            ext_coloring: ext.FieldColoring,
    ) -> tuple[Sequence[charts.ChartField], Optional[charts.ColorConfig]]:
        int_color_fields = self.convert_field_source_seq_ext_to_int(
            [ext_coloring.source],
            single_element=True,
            path_elem="source"
        )

        # Error occurred during color field resolution
        # So color config resolution can not be performed (we don't know type of field)
        # But we should return anything to continue chart validation
        if len(int_color_fields) == 0:
            return (), None

        field = int_color_fields[0]
        config: Optional[charts.ColorConfig]

        if isinstance(ext_coloring, ext.DimensionColoring):
            if (
                    field.type != charts.DatasetFieldType.DIMENSION
                    and
                    not self._dataset_field_resolver.is_measure_names(field)
            ):
                raise ConstraintViolationError("Dimension coloring can not be applied to non-dimension field")

            config = charts.ColorConfig(
                fieldGuid=field.guid,
                coloredByMeasure=False,
                palette=ext_coloring.palette_id,
                mountedColors=FrozenStrMapping({
                    mount.value: str(mount.color_idx)
                    for mount in ext_coloring.mounts
                })
            )
        elif isinstance(ext_coloring, ext.MeasureColoring):
            if field.type != charts.DatasetFieldType.MEASURE:
                raise ConstraintViolationError("Measure coloring can not be applied to non-measure field")
            config = charts.ColorConfig(
                fieldGuid=field.guid,
                coloredByMeasure=True,
            )
            config = MeasureColoringSpecConverter.convert_ext_to_int(ext_coloring.spec, config)
        else:
            raise AssertionError(f"Unexpected type of field coloring: {type(ext_coloring)=}")

        return int_color_fields, config

    def convert_colors_int_to_ext(
            self,
            field: Optional[charts.ChartField],
            colors_config: Optional[charts.ColorConfig],
    ) -> Optional[ext.FieldColoring]:
        if field is None:
            return None

        field_type = field.type
        ext_source_field = self.placeholder_item_to_ext_field_ref(field).source

        if field_type is charts.DatasetFieldType.DIMENSION or self._dataset_field_resolver.is_measure_names(field):
            if colors_config is not None and colors_config.coloredByMeasure:
                self._err_handling_ctx.log_warning(
                    "Got colors_config.coloredByMeasure=True for coloring by dimensions field",
                    not_for_user=True
                )

            return ext.DimensionColoring(
                source=ext_source_field,
                palette_id=colors_config.palette if colors_config else None,
                mounts=(
                    [
                        ext.ColorMount(value=value, color_idx=int(idx_str))
                        for value, idx_str in colors_config.mountedColors.items()
                    ]
                    if colors_config is not None and colors_config.mountedColors is not None
                    else ()
                ),
            )
        elif field_type is charts.DatasetFieldType.MEASURE:
            if colors_config is not None and not colors_config.coloredByMeasure:
                self._err_handling_ctx.log_warning(
                    "Got colors_config.coloredByMeasure=False for coloring by measure field",
                    not_for_user=True
                )
            return ext.MeasureColoring(
                source=ext_source_field,
                spec=MeasureColoringSpecConverter.convert_int_to_ext(colors_config),
            )

        raise get_malformed_ext_unsuitable_field(
            bad_field_msg="Can not determine exact type of FieldColoring for field",
            field=field,
        )

    def convert_color_int_to_ext_validate_type(
            self,
            the_type: Type[_F_COLORING_T],
            field: Optional[charts.ChartField],
            colors_config: Optional[charts.ColorConfig],
    ) -> Optional[_F_COLORING_T]:
        ret = self.convert_colors_int_to_ext(field, colors_config)
        if isinstance(ret, the_type) or ret is None:
            return ret

        raise get_malformed_ext_unsuitable_field(
            bad_field_msg=f"Unexpected type of coloring instead of {the_type} was constructed. Field is",
            field=field,
        )

    def convert_dimensions_shapes_ext_to_int(
            self,
            ext_shaping: ext.DimensionShaping,
    ) -> tuple[Sequence[charts.ChartField], Optional[charts.ShapeConfig]]:
        int_shape_fields = self.convert_field_source_seq_ext_to_int(
            [ext_shaping.source],
            single_element=True,
            path_elem="source"
        )

        # Error occurred during shapes field resolution
        # So color config resolution can not be performed (we don't know type of field)
        # But we should return anything to continue chart validation
        if len(int_shape_fields) == 0:
            return (), None

        field = int_shape_fields[0]
        config: Optional[charts.ShapeConfig]

        if field.type != charts.DatasetFieldType.DIMENSION and not self._dataset_field_resolver.is_measure_names(field):
            raise ConstraintViolationError("Dimension shaping can not be applied to non-dimension field")

        config = charts.ShapeConfig(
            fieldGuid=field.guid,
            mountedShapes=FrozenStrMapping({
                mount.value: FieldShapeConverter.ext_to_int(mount.shape)
                for mount in ext_shaping.mounts
            }),
        )
        return int_shape_fields, config

    def convert_dimensions_shapes_int_to_ext(
            self,
            field: Optional[charts.ChartField],
            int_config: Optional[charts.ShapeConfig]
    ) -> Optional[ext.DimensionShaping]:
        if field is None:
            return None

        field_type = field.type
        ext_source_field = self.placeholder_item_to_ext_field_ref(field).source

        int_shapes_mounts: Optional[FrozenStrMapping[str]] = None
        if int_config is not None and int_config.mountedShapes is not None:
            int_shapes_mounts = int_config.mountedShapes

        if field_type is charts.DatasetFieldType.DIMENSION or self._dataset_field_resolver.is_measure_names(field):
            return ext.DimensionShaping(
                source=ext_source_field,
                mounts=[
                    ext.ShapeMount(value=value, shape=FieldShapeConverter.int_to_ext(shape_str))
                    for value, shape_str in int_shapes_mounts.items()
                ] if int_shapes_mounts is not None else (),
            )
        raise get_malformed_ext_unsuitable_field(
            bad_field_msg="Unexpected shape field params",
            field=field,
        )

    def measure_coloring_to_int(self, int_v_pack: IntVisPack) -> Optional[ext.MeasureColoring]:
        return self.convert_color_int_to_ext_validate_type(
            ext.MeasureColoring,
            int_v_pack.get_single_color_field(),
            int_v_pack.colors_config,
        )

    def dimension_coloring_to_ext(self, int_v_pack: IntVisPack) -> Optional[ext.DimensionColoring]:
        return self.convert_color_int_to_ext_validate_type(
            ext.DimensionColoring,
            int_v_pack.get_single_color_field(),
            int_v_pack.colors_config,
        )

    def field_coloring_to_ext(self, int_v_pack: IntVisPack) -> Optional[ext.FieldColoring]:
        return self.convert_color_int_to_ext_validate_type(
            ext.FieldColoring,
            int_v_pack.get_single_color_field(),
            int_v_pack.colors_config,
        )


@attr.s()
class VisConvFlatTable(VisualizationConverterBase[ext.FlatTable]):
    INT_VIS_ID = charts.VisualizationId.flatTable
    EXT_VIS_CLS = ext.FlatTable

    def convert_int_to_ext(self, int_v_pack: IntVisPack) -> ext.FlatTable:
        v_accessor = InternalVisualizationAccessor(int_v_pack.vis_strict)

        return ext.FlatTable(
            columns=self.placeholder_item_seq_to_ext_field_ref_seq(
                v_accessor.get_single_placeholder(charts.PlaceholderId.FlatTableColumns).items
            ),
            coloring=self.measure_coloring_to_int(int_v_pack),
            sort=self.convert_sort_seq_int_to_ext(int_v_pack.sort),
        )

    def _convert_ext_to_int(self, ext_v: ext.FlatTable) -> charts.Visualization:
        return charts.Visualization(
            id=self.INT_VIS_ID,
            placeholders=[
                charts.Placeholder(
                    id=charts.PlaceholderId.FlatTableColumns,
                    items=self.field_ref_seq_to_placeholder_item_seq(ext_v.columns, path_elem="columns"),
                )
            ],
        )


@attr.s()
class VisConvPivotTable(VisualizationConverterBase[ext.PivotTable]):
    INT_VIS_ID = charts.VisualizationId.pivotTable
    EXT_VIS_CLS = ext.PivotTable

    def convert_int_to_ext(self, int_v_pack: IntVisPack) -> ext.PivotTable:
        v_accessor = InternalVisualizationAccessor(int_v_pack.vis_strict)

        return ext.PivotTable(
            columns=self.placeholder_item_seq_to_ext_field_ref_seq(
                v_accessor.get_single_placeholder(charts.PlaceholderId.PivotTableColumns).items
            ),
            rows=self.placeholder_item_seq_to_ext_field_ref_seq(
                v_accessor.get_single_placeholder(charts.PlaceholderId.Rows).items
            ),
            measures=self.placeholder_item_seq_to_ext_field_ref_seq(
                v_accessor.get_single_placeholder(charts.PlaceholderId.Measures).items
            ),
            coloring=self.measure_coloring_to_int(int_v_pack),
            sort=self.convert_sort_seq_int_to_ext(int_v_pack.sort),
        )

    def _convert_ext_to_int(self, ext_v: ext.PivotTable) -> charts.Visualization:
        return charts.Visualization(
            id=self.INT_VIS_ID,
            placeholders=[
                charts.Placeholder(
                    id=charts.PlaceholderId.PivotTableColumns,
                    items=self.field_ref_seq_to_placeholder_item_seq(ext_v.columns, path_elem="columns"),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Rows,
                    items=self.field_ref_seq_to_placeholder_item_seq(ext_v.rows, path_elem="rows"),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures,
                    items=self.field_ref_seq_to_placeholder_item_seq(ext_v.measures, path_elem="measures"),
                ),
            ],
        )


@attr.s()
class VisConvLinearDiagram(VisualizationConverterBase[ext.LineChart]):
    INT_VIS_ID = charts.VisualizationId.line
    EXT_VIS_CLS = ext.LineChart

    def convert_int_to_ext(self, int_v_pack: IntVisPack) -> ext.LineChart:
        v_accessor = InternalVisualizationAccessor(int_v_pack.vis_strict)

        return ext.LineChart(
            x=self.placeholder_item_to_ext_field_ref(
                v_accessor.get_single_placeholder_item(charts.PlaceholderId.X)
            ),
            y=self.placeholder_item_seq_to_ext_field_ref_seq(
                v_accessor.get_single_placeholder(charts.PlaceholderId.Y).items
            ),
            y2=self.placeholder_item_seq_to_ext_field_ref_seq(
                v_accessor.get_single_placeholder(charts.PlaceholderId.Y2).items
            ),
            coloring=self.dimension_coloring_to_ext(int_v_pack),
            shaping=self.convert_dimensions_shapes_int_to_ext(
                int_v_pack.get_single_shape_field(),
                int_v_pack.shapes_config,
            ),
            sort=self.convert_sort_seq_int_to_ext(int_v_pack.sort),
        )

    def _convert_ext_to_int(self, ext_v: ext.LineChart) -> charts.Visualization:
        return charts.Visualization(
            id=self.INT_VIS_ID,
            placeholders=[
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=self.field_ref_seq_to_placeholder_item_seq(
                        [ext_v.x],
                        path_elem="x",
                        single_element=True,
                    ),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=self.field_ref_seq_to_placeholder_item_seq(ext_v.y, path_elem="y"),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y2,
                    items=self.field_ref_seq_to_placeholder_item_seq(ext_v.y2, path_elem="y2"),
                ),
            ],
        )


@attr.s()
class VisConvIndicator(VisualizationConverterBase[ext.Indicator]):
    INT_VIS_ID = charts.VisualizationId.metric
    EXT_VIS_CLS = ext.Indicator

    def convert_int_to_ext(self, int_v_pack: IntVisPack) -> ext.Indicator:
        v_accessor = InternalVisualizationAccessor(int_v_pack.vis_strict)

        return ext.Indicator(
            field=self._dataset_field_resolver.get_ext_chart_field_by_int_field(
                v_accessor.get_single_placeholder_item(charts.PlaceholderId.Measures)
            )
        )

    def _convert_ext_to_int(self, ext_v: ext.Indicator) -> charts.Visualization:
        return charts.Visualization(
            id=self.INT_VIS_ID,
            placeholders=[
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures,
                    items=self.field_ref_seq_to_placeholder_item_seq(
                        [ext_v.field],
                        path_elem="field",
                        single_element=True,
                    ),
                )
            ],
        )


_COLUMN_VIS_CLS = TypeVar("_COLUMN_VIS_CLS", bound=ext.BaseColumnChart)


@attr.s()
class VisConvBaseColumnDiagram(VisualizationConverterBase, Generic[_COLUMN_VIS_CLS]):
    EXT_VIS_CLS: ClassVar[Type[_COLUMN_VIS_CLS]]

    def convert_int_to_ext(self, int_v_pack: IntVisPack) -> _COLUMN_VIS_CLS:
        v_accessor = InternalVisualizationAccessor(int_v_pack.vis_strict)

        return self.EXT_VIS_CLS(
            x=self.placeholder_item_seq_to_ext_field_ref_seq(
                v_accessor.get_single_placeholder(charts.PlaceholderId.X).items
            ),
            y=self.placeholder_item_seq_to_ext_field_ref_seq(
                v_accessor.get_single_placeholder(charts.PlaceholderId.Y).items
            ),
            sort=self.convert_sort_seq_int_to_ext(int_v_pack.sort),
            coloring=self.convert_color_int_to_ext_validate_type(
                ext.FieldColoring,
                int_v_pack.get_single_color_field(),
                int_v_pack.colors_config,
            )
        )

    def _convert_ext_to_int(self, ext_v: _COLUMN_VIS_CLS) -> charts.Visualization:
        return charts.Visualization(
            id=self.INT_VIS_ID,
            placeholders=[
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=self.field_ref_seq_to_placeholder_item_seq(ext_v.x, path_elem="x"),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=self.field_ref_seq_to_placeholder_item_seq(ext_v.y, path_elem="y"),
                ),
            ],
        )


@attr.s()
class VisConvColumnDiagram(VisConvBaseColumnDiagram[ext.ColumnChart]):
    EXT_VIS_CLS = ext.ColumnChart
    INT_VIS_ID = charts.VisualizationId.column


@attr.s()
class VisConvNormalizedColumnDiagram(VisConvBaseColumnDiagram[ext.ColumnChartNormalized]):
    EXT_VIS_CLS = ext.ColumnChartNormalized
    INT_VIS_ID = charts.VisualizationId.column100p


_BAR_VIS_CLS = TypeVar("_BAR_VIS_CLS", bound=ext.BaseBarChart)


@attr.s()
class VisConvBaseBarDiagram(VisualizationConverterBase, Generic[_BAR_VIS_CLS]):
    EXT_VIS_CLS: ClassVar[Type[_BAR_VIS_CLS]]

    def convert_int_to_ext(self, int_v_pack: IntVisPack) -> _BAR_VIS_CLS:
        v_accessor = InternalVisualizationAccessor(int_v_pack.vis_strict)

        return self.EXT_VIS_CLS(
            y=self.placeholder_item_seq_to_ext_field_ref_seq(
                v_accessor.get_single_placeholder(charts.PlaceholderId.Y).items
            ),
            x=self.placeholder_item_seq_to_ext_field_ref_seq(
                v_accessor.get_single_placeholder(charts.PlaceholderId.X).items
            ),
            sort=self.convert_sort_seq_int_to_ext(int_v_pack.sort),
            coloring=self.convert_color_int_to_ext_validate_type(
                ext.FieldColoring,
                int_v_pack.get_single_color_field(),
                int_v_pack.colors_config,
            )
        )

    def _convert_ext_to_int(self, ext_v: _COLUMN_VIS_CLS) -> charts.Visualization:
        return charts.Visualization(
            id=self.INT_VIS_ID,
            placeholders=[
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=self.field_ref_seq_to_placeholder_item_seq(ext_v.y, path_elem="y"),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=self.field_ref_seq_to_placeholder_item_seq(ext_v.x, path_elem="x"),
                ),
            ],
        )


@attr.s()
class VisConvBarDiagram(VisConvBaseBarDiagram[ext.BarChart]):
    EXT_VIS_CLS = ext.BarChart
    INT_VIS_ID = charts.VisualizationId.bar


@attr.s()
class VisConvNormalizedBarDiagram(VisConvBaseBarDiagram[ext.BarChartNormalized]):
    EXT_VIS_CLS = ext.BarChartNormalized
    INT_VIS_ID = charts.VisualizationId.bar100p


_AREA_VIS_CLS = TypeVar("_AREA_VIS_CLS", bound=ext.BaseAreaChart)


@attr.s()
class VisConvBaseAreaChart(VisualizationConverterBase, Generic[_AREA_VIS_CLS]):
    EXT_VIS_CLS: ClassVar[Type[_AREA_VIS_CLS]]

    def convert_int_to_ext(self, int_v_pack: IntVisPack) -> _AREA_VIS_CLS:
        v_accessor = InternalVisualizationAccessor(int_v_pack.vis_strict)

        return self.EXT_VIS_CLS(
            x=self.placeholder_item_to_ext_field_ref(
                v_accessor.get_single_placeholder_item(charts.PlaceholderId.X),
            ),
            y=self.placeholder_item_seq_to_ext_field_ref_seq(
                v_accessor.get_single_placeholder(charts.PlaceholderId.Y).items
            ),
            sort=self.convert_sort_seq_int_to_ext(int_v_pack.sort),
            coloring=self.convert_color_int_to_ext_validate_type(
                ext.FieldColoring,
                int_v_pack.get_single_color_field(),
                int_v_pack.colors_config,
            )
        )

    def _convert_ext_to_int(self, ext_v: _AREA_VIS_CLS) -> charts.Visualization:
        return charts.Visualization(
            id=self.INT_VIS_ID,
            placeholders=[
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=self.field_ref_seq_to_placeholder_item_seq([ext_v.x], path_elem="x"),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=self.field_ref_seq_to_placeholder_item_seq(ext_v.y, path_elem="y"),
                ),
            ],
        )


@attr.s()
class VisConvAreaChart(VisConvBaseAreaChart[ext.AreaChart]):
    EXT_VIS_CLS = ext.AreaChart
    INT_VIS_ID = charts.VisualizationId.area


@attr.s()
class VisConvNormalizedAreaChart(VisConvBaseAreaChart[ext.AreaChartNormalized]):
    EXT_VIS_CLS = ext.AreaChartNormalized
    INT_VIS_ID = charts.VisualizationId.area100p


_CIRCULAR_VIS_CLS = TypeVar("_CIRCULAR_VIS_CLS", bound=ext.BaseCircularChart)


@attr.s()
class VisConvBaseCircularChart(VisualizationConverterBase, Generic[_CIRCULAR_VIS_CLS]):
    EXT_VIS_CLS: ClassVar[Type[_CIRCULAR_VIS_CLS]]
    _set_colors_during_ext_to_int = False

    def convert_int_to_ext(self, int_v_pack: IntVisPack) -> _CIRCULAR_VIS_CLS:
        v_accessor = InternalVisualizationAccessor(int_v_pack.vis_strict)
        coloring = self.convert_color_int_to_ext_validate_type(
            ext.DimensionColoring,
            v_accessor.get_single_placeholder_item(charts.PlaceholderId.Dimensions),
            int_v_pack.colors_config,
        )
        if coloring is None:
            raise ConstraintViolationError(f"Coloring is required for {self.EXT_VIS_CLS}")

        return self.EXT_VIS_CLS(
            measures=self.placeholder_item_to_ext_field_ref(
                v_accessor.get_single_placeholder_item(charts.PlaceholderId.Measures)
            ),
            sort=self.convert_sort_seq_int_to_ext(int_v_pack.sort),  # from single field
            coloring=coloring,
        )

    def _convert_ext_to_int(self, ext_v: _CIRCULAR_VIS_CLS) -> charts.Visualization:
        return charts.Visualization(
            id=self.INT_VIS_ID,
            placeholders=[
                charts.Placeholder(
                    id=charts.PlaceholderId.Dimensions,
                    items=(self.convert_field_source_ext_to_int(ext_v.coloring.source),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures,
                    items=self.field_ref_seq_to_placeholder_item_seq(
                        [ext_v.measures], path_elem="measures"
                    ),
                ),
            ],
        )


@attr.s()
class VisConvPieChart(VisConvBaseCircularChart[ext.PieChart]):
    EXT_VIS_CLS = ext.PieChart
    INT_VIS_ID = charts.VisualizationId.pie


@attr.s()
class VisConvDonutChart(VisConvBaseCircularChart[ext.DonutChart]):
    EXT_VIS_CLS = ext.DonutChart
    INT_VIS_ID = charts.VisualizationId.donut


@attr.s()
class VisConvScatterPlot(VisualizationConverterBase[ext.ScatterPlot]):
    EXT_VIS_CLS = ext.ScatterPlot
    INT_VIS_ID = charts.VisualizationId.scatter

    def convert_int_to_ext(self, int_v_pack: IntVisPack) -> ext.ScatterPlot:
        v_accessor = InternalVisualizationAccessor(int_v_pack.vis_strict)

        return ext.ScatterPlot(
            x=self.placeholder_item_to_ext_field_ref(
                v_accessor.get_single_placeholder_item(charts.PlaceholderId.X)
            ),
            y=self.placeholder_item_to_ext_field_ref(
                v_accessor.get_single_placeholder_item(charts.PlaceholderId.Y)
            ),
            points=self.placeholder_item_to_ext_field_ref(
                v_accessor.get_single_placeholder_item(charts.PlaceholderId.Points)
            ),
            size=self.placeholder_item_to_ext_field_ref(
                v_accessor.get_single_placeholder_item(charts.PlaceholderId.Size)
            ),
            coloring=self.dimension_coloring_to_ext(int_v_pack),
            sort=self.convert_sort_seq_int_to_ext(int_v_pack.sort),
        )

    def _convert_ext_to_int(self, ext_v: ext.ScatterPlot) -> charts.Visualization:
        placeholders = [
            charts.Placeholder(
                id=charts.PlaceholderId.X,
                items=self.field_ref_seq_to_placeholder_item_seq(
                    [ext_v.x],
                    path_elem="x",
                    single_element=True,
                ),
            ),
            charts.Placeholder(
                id=charts.PlaceholderId.Y,
                items=self.field_ref_seq_to_placeholder_item_seq(
                    [ext_v.y],
                    path_elem="y",
                    single_element=True,
                ),
            ),
        ]
        if ext_v.points is not None:
            placeholders.append(charts.Placeholder(
                id=charts.PlaceholderId.Points,
                items=self.field_ref_seq_to_placeholder_item_seq(
                    [ext_v.points],
                    path_elem="points",
                    single_element=True,
                ),
            ))

        if ext_v.size is not None:
            placeholders.append(charts.Placeholder(
                id=charts.PlaceholderId.Size,
                items=self.field_ref_seq_to_placeholder_item_seq(
                    [ext_v.size],
                    path_elem="size",
                    single_element=True,
                ),
            ))

        return charts.Visualization(
            id=self.INT_VIS_ID,
            placeholders=placeholders,
        )


@attr.s()
class VisConvTreeMapDiagram(VisualizationConverterBase[ext.TreeMap]):
    INT_VIS_ID = charts.VisualizationId.treemap
    EXT_VIS_CLS = ext.TreeMap

    def convert_int_to_ext(self, int_v_pack: IntVisPack) -> ext.TreeMap:
        v_accessor = InternalVisualizationAccessor(int_v_pack.vis_strict)

        return ext.TreeMap(
            dimensions=self.placeholder_item_seq_to_ext_field_ref_seq(
                v_accessor.get_single_placeholder(charts.PlaceholderId.Dimensions).items
            ),
            measures=self.placeholder_item_to_ext_field_ref(
                v_accessor.get_single_placeholder_item(charts.PlaceholderId.Measures),
            ),
            coloring=self.field_coloring_to_ext(int_v_pack),
            sort=self.convert_sort_seq_int_to_ext(int_v_pack.sort),
        )

    def _convert_ext_to_int(self, ext_v: ext.TreeMap) -> charts.Visualization:
        return charts.Visualization(
            id=self.INT_VIS_ID,
            placeholders=[
                charts.Placeholder(
                    id=charts.PlaceholderId.Dimensions,
                    items=self.field_ref_seq_to_placeholder_item_seq(
                        ext_v.dimensions,
                        path_elem="dimensions"
                    ),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures,
                    items=self.field_ref_seq_to_placeholder_item_seq(
                        [ext_v.measures],
                        path_elem="measures",
                        single_element=True,
                    ),
                )

            ]
        )
