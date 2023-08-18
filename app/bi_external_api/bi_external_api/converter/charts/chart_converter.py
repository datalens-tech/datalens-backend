from __future__ import annotations

from contextlib import ExitStack
from typing import Optional, Sequence

import attr

from bi_external_api.converter.charts.ad_hoc_field_converter import AdHocFieldConverter
from bi_external_api.converter.charts.defaulter import ExtAggregationDefaulter, ExtDatasetRefDefaulter
from bi_external_api.converter.charts.ds_field_resolvers import MultiDatasetFieldResolver
from bi_external_api.converter.charts.filter_converters import FilterExtToIntConverter, FilterIntToExtConverter
from bi_external_api.converter.charts.utils import IntVisPack
from bi_external_api.converter.charts.visualization_converters_facade import VisualizationConverterFacade
from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.converter_exc_composer import ConversionErrHandlingContext
from bi_external_api.converter.dataset_field import DatasetFieldConverter
from bi_external_api.converter.id_gathering_processor import IDGatheringProcessor
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import (
    charts,
    datasets,
)
from bi_external_api.domain.internal.dl_common import EntryScope


@attr.s()
class BaseChartConverter:
    _wb_context: WorkbookContext = attr.ib()
    _converter_ctx: ConverterContext = attr.ib()

    @classmethod
    def fill_defaults(cls, chart: ext.Chart) -> ext.Chart:
        modified_chart = ExtAggregationDefaulter(ext.Aggregation.none).process(chart)

        if len(chart.datasets) == 1:
            default_dataset_name = chart.datasets[0]
            modified_chart = ExtDatasetRefDefaulter(default_dataset_name).process(modified_chart)

        return modified_chart

    def convert_ext_chart_ad_hoc_fields_to_chart_actions(
            self,
            ext_chart: ext.Chart
    ) -> Sequence[charts.ChartActionFieldAdd]:
        """
        This method is implemented for preliminary types discovery via dataset API.
        Results will be passed to ad-hoc field resolver.
        """
        with ConversionErrHandlingContext().cm() as err_handling_ctx:
            # Just to gather more errors
            self._extract_declared_dataset_instances(ext_chart, err_handling_ctx)
            return self._convert_ad_hoc_fields_to_chart_actions(ext_chart, err_handling_ctx)

    def convert_chart_ext_to_int(
            self,
            ext_chart: ext.Chart,
            datasets_with_applied_actions: Optional[dict[str, datasets.Dataset]] = None,
    ) -> charts.Chart:
        with ConversionErrHandlingContext().cm() as err_handling_ctx:
            return self._convert_chart_ext_to_int_internal(
                ext_chart,
                datasets_with_applied_updates=datasets_with_applied_actions,
                err_handling_ctx=err_handling_ctx
            )

    def _extract_declared_dataset_instances(
            self,
            ext_chart: ext.Chart,
            err_handling_ctx: ConversionErrHandlingContext
    ) -> Sequence[datasets.DatasetInstance]:
        # Resolving dateset used in chart
        declared_dataset_instance_list: list[datasets.DatasetInstance] = []

        with err_handling_ctx.push_path("datasets"):
            for ds_name in ext_chart.datasets:
                with err_handling_ctx.postpone_error_with_path(path_elem=ds_name):
                    declared_dataset_instance_list.append(
                        self._wb_context.resolve_dataset_by_name(ds_name)
                    )

        return declared_dataset_instance_list

    def _convert_ad_hoc_fields_to_chart_actions(
            self,
            ext_chart: ext.Chart,
            err_handling_ctx: ConversionErrHandlingContext,
            map_id_dataset_with_applied_updates: Optional[dict[str, datasets.Dataset]] = None,
    ) -> Sequence[charts.ChartActionFieldAdd]:
        ad_hoc_field_converter = AdHocFieldConverter(self._wb_context, self._converter_ctx)

        fields_modification_actions: list[charts.ChartActionFieldAdd] = []

        with err_handling_ctx.push_path("ad_hoc_fields"):
            for ad_hoc_field in ext_chart.ad_hoc_fields:
                with err_handling_ctx.postpone_error_with_path(path_elem=ad_hoc_field.field.id):
                    action = ad_hoc_field_converter.convert_ad_hoc_field_to_actions(ad_hoc_field)

                    # To fill guid-formula if ext has title formula & vise-versa
                    if map_id_dataset_with_applied_updates is not None:
                        target_dataset = map_id_dataset_with_applied_updates[action.field.datasetId]
                        field_after_validation = target_dataset.get_field_by_id(action.field.guid)
                        action = ad_hoc_field_converter.post_process_with_validation_results(
                            action,
                            field_after_validation
                        )
                        DatasetFieldConverter.validate_id_formula_field(
                            ad_hoc_field.field,
                            {f.guid for f in target_dataset.result_schema},
                        )

                    fields_modification_actions.append(action)

        return fields_modification_actions

    def _convert_filters_ext_to_int(
            self,
            ext_chart: ext.Chart,
            err_handling_ctx: ConversionErrHandlingContext,
            field_resolver: MultiDatasetFieldResolver,
    ) -> Sequence[charts.FieldFilter]:
        filter_converter = FilterExtToIntConverter(dataset_field_resolver=field_resolver)
        filters = []
        with err_handling_ctx.push_path("filters"):
            for f in ext_chart.filters:
                with err_handling_ctx.postpone_error_with_path(path_elem=f.field_ref.id):
                    filters.append(filter_converter.convert(f))
        return filters

    def _convert_chart_ext_to_int_internal(
            self, ext_chart: ext.Chart,
            datasets_with_applied_updates: Optional[dict[str, datasets.Dataset]],
            err_handling_ctx: ConversionErrHandlingContext
    ) -> charts.Chart:
        # Resolving dateset used in chart
        declared_dataset_instance_list = self._extract_declared_dataset_instances(ext_chart, err_handling_ctx)

        # Converting ad-hoc fields
        fields_modification_actions = self._convert_ad_hoc_fields_to_chart_actions(
            ext_chart, err_handling_ctx,
            map_id_dataset_with_applied_updates=datasets_with_applied_updates,
        )

        field_resolver = MultiDatasetFieldResolver(
            actions=fields_modification_actions,
            wb_context=self._wb_context,
            map_id_dataset_with_applied_actions=datasets_with_applied_updates,
        )

        with err_handling_ctx.push_path("visualization"):
            int_visualization_pack = VisualizationConverterFacade.convert_visualization_ext_to_int(
                ext_chart.visualization,
                dataset_field_resolver=field_resolver,
                err_handling_ctx=err_handling_ctx,
            )

        ext_filters = self._convert_filters_ext_to_int(ext_chart, err_handling_ctx, field_resolver)

        return charts.Chart(
            filters=ext_filters,
            colors=int_visualization_pack.colors,
            colorsConfig=int_visualization_pack.colors_config,
            shapes=int_visualization_pack.shapes,
            shapesConfig=int_visualization_pack.shapes_config,
            sort=int_visualization_pack.sort,
            visualization=int_visualization_pack.vis,
            datasetsIds=[
                ds_inst.summary.id
                for ds_inst in declared_dataset_instance_list
            ],
            datasetsPartialFields=[
                field_resolver.get_partials(ds_inst.summary.name)
                for ds_inst in declared_dataset_instance_list
            ],
            updates=fields_modification_actions,
        )

    def convert_chart_int_to_ext(
            self,
            chart: charts.Chart,
            err_handling_ctx: Optional[ConversionErrHandlingContext] = None,
    ) -> ext.Chart:
        stack = ExitStack()

        effective_err_handling_ctx: ConversionErrHandlingContext

        if err_handling_ctx is None:
            effective_err_handling_ctx = stack.enter_context(ConversionErrHandlingContext().cm())
        else:
            assert err_handling_ctx.is_opened, \
                "Error handling context passed to convert_chart_int_to_ext() was not opened"
            effective_err_handling_ctx = err_handling_ctx

        with stack:
            return self._convert_chart_int_to_ext_internal(chart, err_handling_ctx=effective_err_handling_ctx)

        # https://github.com/python/mypy/issues/7726
        # noinspection PyUnreachableCode
        assert False, "unreachable"

    @staticmethod
    def _gather_dataset_ids(chart: charts.Chart) -> Sequence[str]:
        proc = IDGatheringProcessor()
        proc.process(chart)
        return tuple(ds_id for ds_id in sorted(proc.get_gathered_entry_ids(EntryScope.dataset)))

    def _convert_chart_int_to_ext_internal(
            self,
            chart: charts.Chart,
            err_handling_ctx: ConversionErrHandlingContext,
    ) -> ext.Chart:
        ad_hoc_field_converter = AdHocFieldConverter(self._wb_context, self._converter_ctx)

        ad_hoc_fields: list[ext.AdHocField] = []

        with ConversionErrHandlingContext(current_path=["actions"]).cm() as err_hdr:
            for action in chart.updates:
                assert isinstance(action, charts.ChartActionFieldAdd)
                with err_hdr.postpone_error_with_path(f"{action.field.datasetId}:{action.field.guid}"):
                    ad_hoc_fields.append(
                        ad_hoc_field_converter.convert_action_to_ad_hoc_field(action)
                    )

        field_resolver = MultiDatasetFieldResolver(
            actions=chart.updates,
            wb_context=self._wb_context,
            map_id_dataset_with_applied_actions=None,
        )

        int_vis_pack = IntVisPack(
            vis=chart.visualization,
            js=chart.js,
            sort=chart.sort or (),
            colors=chart.colors or (),
            colors_config=chart.colorsConfig,
            shapes=chart.shapes or (),
            shapes_config=chart.shapesConfig,
        )

        ext_visualization = VisualizationConverterFacade.convert_visualization_int_to_ext(
            dataset_field_resolver=field_resolver,
            int_vis_pack=int_vis_pack,
            err_handling_ctx=err_handling_ctx,
        )

        gathered_dataset_ids = self._gather_dataset_ids(chart)
        declared_dataset_ids = chart.datasetsIds

        if declared_dataset_ids is not None:
            if set(gathered_dataset_ids) != set(declared_dataset_ids):
                err_handling_ctx.log_warning(
                    "Got divergence in internal chart between declared dataset IDs and those actually used in field",
                    not_for_user=True
                )

        dataset_id_to_name = {
            ds: self._wb_context.resolve_dataset_by_id(ds).summary.name for ds in gathered_dataset_ids
        }

        filters = self._convert_filters_int_to_ext(chart, dataset_id_to_name)

        return ext.Chart(
            datasets=list(dataset_id_to_name.values()),
            ad_hoc_fields=ad_hoc_fields,
            visualization=ext_visualization,
            filters=filters
        )

    def _convert_filters_int_to_ext(
            self,
            chart: charts.Chart,
            dataset_id_to_name: dict[str, str]
    ) -> Sequence[ext.ChartFilter]:
        filter_converter = FilterIntToExtConverter(dataset_id_to_name=dataset_id_to_name)
        filters: list[ext.ChartFilter] = []
        with ConversionErrHandlingContext(current_path=["filters"]).cm() as err_hdr:
            for f in chart.filters:
                with err_hdr.postpone_error_with_path(f"{f.datasetId}:{f.guid}"):
                    filters.append(filter_converter.convert(f))
        return filters
