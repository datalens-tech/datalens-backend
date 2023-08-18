import abc
import logging
from typing import TypeVar, Any, ClassVar, Type, final, Optional

from bi_external_api.converter.charts.utils import convert_field_type_dataset_to_chart
from bi_external_api.converter.converter_exc import NotSupportedYet
from bi_external_api.converter.dash.dash_element_common import BaseDashElementConverter
from bi_external_api.converter.dash.utils import TabItemControlAccessor, SourceAccessor
from bi_external_api.converter.utils import convert_enum_by_name_allow_none
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import dashboards, charts
from bi_external_api.domain.internal.dashboards import ControlData
from bi_external_api.structs.mappings import FrozenMappingStrToStrOrStrSeq

_DASH_ELEMENT_TV = TypeVar("_DASH_ELEMENT_TV", bound=ext.DashControlGuided)

logger = logging.getLogger(__name__)


class BaseControlElementConverter(
    BaseDashElementConverter[dashboards.ItemControl, _DASH_ELEMENT_TV],
    metaclass=abc.ABCMeta
):
    TARGET_CLS: ClassVar[Type[_DASH_ELEMENT_TV]]

    def convert_int_control_data_to_ext_value_source(self, int_ti_ctrl_data: ControlData) -> ext.ControlValueSource:
        source = int_ti_ctrl_data.source

        if isinstance(source, dashboards.DatasetControlSource):
            ds_name = self._wb_context.resolve_dataset_by_id(source.datasetId).summary.name
            field_id = source.datasetFieldId

            return ext.ControlValueSourceDatasetField(
                dataset_name=ds_name,
                field_id=field_id,
            )
        if isinstance(source, dashboards.ManualControlData):
            raise NotSupportedYet("Manual selectors are not yet implemented in API")

        raise AssertionError(f"Can not resolve control value source for {type(source)=}")

    @classmethod
    def validate_raw_default_value_from_source_against_params_defaults(cls, tab_item: dashboards.ItemControl) -> None:
        accessor = TabItemControlAccessor(tab_item)
        expected_params_defaults = accessor.source_accessor.resolve_expected_params_defaults(
            cls.get_default_internal_operation()
        )
        # TODO FIX: BI-3005 Check if fired. If not - remove try-catch.
        try:
            assert expected_params_defaults == tab_item.defaults, (
                f"Divergence in tab item defaults {accessor.source_accessor.resolve_default_value()!r}"
                f" & selector defaults {expected_params_defaults!r} is not supported"
            )
        except AssertionError:
            logger.warning("Got divergence in selector defaults", exc_info=True)

    # TODO FIX: TypedDict
    def resolve_int_control_source_man_kwargs(self, val_src: ext.ControlValueSourceManual) -> dict[str, Any]:
        raise NotSupportedYet("Manual control value sources are not yet supported")

    def resolve_int_control_source_ds_kwargs(self, val_src: ext.ControlValueSourceDatasetField) -> dict[str, Any]:
        target_dataset = self._wb_context.resolve_dataset_by_name(val_src.dataset_name)
        target_field = target_dataset.dataset.get_field_by_id(val_src.field_id)

        return dict(
            fieldType=target_field.data_type,
            datasetFieldType=convert_field_type_dataset_to_chart(target_field.type),
            datasetFieldId=val_src.field_id,
            datasetId=self._wb_context.resolve_dataset_by_name(val_src.dataset_name).summary.id,
        )

    #
    # Control specific conversions
    #
    @abc.abstractmethod
    def convert_default_value_int_to_ext(
            self,
            tab_item: dashboards.ItemControl
    ) -> Optional[ext.Value]:
        raise NotImplementedError()

    @abc.abstractmethod
    def convert_value_source_dataset_field_ext_to_int(
            self,
            dash_element: _DASH_ELEMENT_TV,
            ext_value_source: ext.ControlValueSourceDatasetField,
    ) -> dashboards.DatasetControlSource:
        raise NotImplementedError()

    # @abc.abstractmethod
    def convert_value_source_manual_ext_to_int(
            self,
            dash_element: _DASH_ELEMENT_TV,
            ext_value_source: ext.ControlValueSourceManual,
    ) -> dashboards.ManualControlSource:
        raise NotSupportedYet("Manual control value sources are not yet supported")

    @classmethod
    @abc.abstractmethod
    def get_default_internal_operation(cls) -> charts.Operation:
        raise NotImplementedError()

    #
    # Interface implementation
    #
    @final
    def _convert_int_to_ext(self, tab_item: dashboards.ItemControl) -> _DASH_ELEMENT_TV:
        int_tab_item_accessor = TabItemControlAccessor(tab_item)

        ext_default_value = self.convert_default_value_int_to_ext(tab_item)
        self.validate_raw_default_value_from_source_against_params_defaults(tab_item)

        # TODO FIX: Validate with type-guard
        return self.TARGET_CLS(
            title=tab_item.data.title,
            show_title=int_tab_item_accessor.get_show_title(),
            source=self.convert_int_control_data_to_ext_value_source(tab_item.data),
            default_value=ext_default_value,
            comparison_operation=convert_enum_by_name_allow_none(
                int_tab_item_accessor.source_accessor.resolve_operation(),
                ext.ComparisonOperation,
            ),
        )

    @classmethod
    def make_defaults(
            cls,
            source: dashboards.CommonGuidedControlSource,
    ) -> FrozenMappingStrToStrOrStrSeq:
        return SourceAccessor(source).resolve_expected_params_defaults(cls.get_default_internal_operation())

    @final
    def convert_ext_to_int(self, dash_element: _DASH_ELEMENT_TV, id: str) -> dashboards.ItemControl:
        ext_values_source = dash_element.source
        control_item_title: str = dash_element.title

        # Outputs:
        if isinstance(ext_values_source, ext.ControlValueSourceDatasetField):
            ds_field_source = self.convert_value_source_dataset_field_ext_to_int(
                dash_element,
                ext_values_source
            )
            return dashboards.ItemControl(
                id=id,
                data=dashboards.DatasetBasedControlData(
                    title=control_item_title,
                    source=ds_field_source,
                ),
                defaults=self.make_defaults(ds_field_source),
            )

        elif isinstance(ext_values_source, ext.ControlValueSourceManual):
            manual_source = self.convert_value_source_manual_ext_to_int(
                dash_element,
                ext_values_source,
            )
            return dashboards.ItemControl(
                id=id,
                data=dashboards.ManualControlData(
                    title=control_item_title,
                    source=manual_source
                ),
                defaults=self.make_defaults(manual_source),
            )
        else:
            raise AssertionError(f"Unexpected external type of control values source: {type(ext_values_source)}")
