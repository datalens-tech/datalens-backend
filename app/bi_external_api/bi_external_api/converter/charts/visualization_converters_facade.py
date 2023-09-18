from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    ClassVar,
    Type,
)

from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import charts

from . import visualization_converters
from ..converter_exc_composer import ConversionErrHandlingContext
from .utils import IntVisPack

if TYPE_CHECKING:
    from .ds_field_resolvers import MultiDatasetFieldResolver


class VisualizationConverterFacade:
    # TODO FIX: Create registration mechanism
    _REGISTERED_CONVERTERS: ClassVar[frozenset[Type[visualization_converters.VisualizationConverter]]] = frozenset(
        {
            visualization_converters.VisConvFlatTable,
            visualization_converters.VisConvPivotTable,
            visualization_converters.VisConvLinearDiagram,
            visualization_converters.VisConvIndicator,
            visualization_converters.VisConvColumnDiagram,
            visualization_converters.VisConvNormalizedColumnDiagram,
            visualization_converters.VisConvTreeMapDiagram,
            visualization_converters.VisConvPieChart,
            visualization_converters.VisConvDonutChart,
            visualization_converters.VisConvScatterPlot,
            visualization_converters.VisConvAreaChart,
            visualization_converters.VisConvNormalizedAreaChart,
            visualization_converters.VisConvBarDiagram,
            visualization_converters.VisConvNormalizedBarDiagram,
        }
    )

    # TODO FIX: Optimize
    @classmethod
    def resolve_converter_cls_by_visualization_id(
        cls,
        vis_id: charts.VisualizationId,
    ) -> Type[visualization_converters.VisualizationConverter]:
        try:
            return next(vis_cls for vis_cls in cls._REGISTERED_CONVERTERS if vis_cls.INT_VIS_ID == vis_id)
        except StopIteration:
            return visualization_converters.UnsupportedVisualizationConverter

    # TODO FIX: Optimize
    @classmethod
    def resolve_converter_cls_by_ext_vis_cls(
        cls,
        ext_vis_cls: Type[ext.Visualization],
    ) -> Type[visualization_converters.VisualizationConverter]:
        try:
            return next(vis_cls for vis_cls in cls._REGISTERED_CONVERTERS if vis_cls.EXT_VIS_CLS == ext_vis_cls)
        except StopIteration:
            raise NotImplementedError(f"Unsupported class of visualization {ext_vis_cls}")

    @classmethod
    def convert_visualization_ext_to_int(
        cls,
        ext_vis: ext.Visualization,
        dataset_field_resolver: MultiDatasetFieldResolver,
        err_handling_ctx: ConversionErrHandlingContext,
    ) -> IntVisPack:
        vis_conv_cls = cls.resolve_converter_cls_by_ext_vis_cls(type(ext_vis))
        vis_conv = vis_conv_cls.create(
            dataset_field_resolver=dataset_field_resolver,
            err_handling_ctx=err_handling_ctx,
        )
        return vis_conv.convert_ext_to_int(ext_vis)

    @classmethod
    def convert_visualization_int_to_ext(
        cls,
        int_vis_pack: IntVisPack,
        dataset_field_resolver: MultiDatasetFieldResolver,
        err_handling_ctx: ConversionErrHandlingContext,
    ) -> ext.Visualization:
        int_vis = int_vis_pack.vis

        vis_conv_cls: Type[visualization_converters.VisualizationConverter]

        if int_vis is not None:
            vis_conv_cls = cls.resolve_converter_cls_by_visualization_id(int_vis.id)
        else:
            vis_conv_cls = visualization_converters.UnsupportedVisualizationConverter
        vis_conv = vis_conv_cls.create(
            dataset_field_resolver=dataset_field_resolver,
            err_handling_ctx=err_handling_ctx,
        )
        return vis_conv.convert_int_to_ext(int_vis_pack)
