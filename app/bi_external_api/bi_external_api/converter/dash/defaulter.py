from typing import Optional, cast

import attr

from bi_external_api.attrs_model_mapper import Processor
from bi_external_api.attrs_model_mapper.field_processor import FieldMeta
from bi_external_api.domain import external as ext


@attr.s()
class ExtDashChartContainerDefaultWidgetIdDefaulter(Processor[ext.DashElement]):

    def _should_process(self, meta: FieldMeta) -> bool:
        return issubclass(meta.clz, ext.DashElement)

    def _process_single_object(
        self, obj: ext.DashElement, meta: FieldMeta
    ) -> Optional[ext.DashElement]:

        if not isinstance(obj, ext.DashChartsContainer):
            return obj

        obj = cast(ext.DashChartsContainer, obj)

        if obj.default_active_chart_tab_id is not None or len(obj.tabs) == 0:
            return obj

        return attr.evolve(obj, default_active_chart_tab_id=obj.tabs[0].id)


@attr.s()
class ExtDashWidgetTabDefaulter(Processor[ext.WidgetTab]):

    def _should_process(self, meta: FieldMeta) -> bool:
        return issubclass(meta.clz, ext.WidgetTab)

    def _process_single_object(
        self,
        obj: ext.WidgetTab,
        meta: FieldMeta
    ) -> Optional[ext.WidgetTab]:
        if obj.title:
            return obj

        return attr.evolve(obj, title=obj.chart_name)
