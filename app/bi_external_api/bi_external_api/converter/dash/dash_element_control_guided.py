import abc
from typing import (
    ClassVar,
    Optional,
    TypeVar,
    final,
)

from bi_external_api.converter.converter_exc import MalformedEntryConfig
from bi_external_api.converter.dash.dash_element_control import BaseControlElementConverter
from bi_external_api.converter.dash.utils import AnyStringValueConverter
from bi_external_api.converter.utils import convert_enum_by_name_allow_none
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import (
    charts,
    dashboards,
)
from bi_external_api.structs.singleormultistring import SingleOrMultiString


_SELECTOR_TV = TypeVar("_SELECTOR_TV", bound=ext.DashControlGuided)


class AnySelectorElementConverter(BaseControlElementConverter[_SELECTOR_TV]):
    MULTISELECTABLE: ClassVar[bool]

    @abc.abstractmethod
    def convert_default_value_ext_to_int(self, selector: _SELECTOR_TV) -> Optional[SingleOrMultiString]:
        raise NotImplementedError()

    @final
    def convert_default_value_int_to_ext(
        self,
        tab_item: dashboards.ItemControl,
    ) -> Optional[ext.Value]:
        values_source = tab_item.data.source
        assert isinstance(values_source, dashboards.FieldSetCommonControlSourceSelect)
        assert (
            bool(values_source.multiselectable) is self.MULTISELECTABLE
        ), f"Converter {type(self)=} does not support {values_source.multiselectable=}"

        assert isinstance(values_source, dashboards.CommonGuidedControlSource)
        internal_default_value = values_source.defaultValue

        if internal_default_value is None:
            return None

        if self.MULTISELECTABLE and internal_default_value.is_single:
            raise MalformedEntryConfig("Multiselect has non-multi-string default")
        if not self.MULTISELECTABLE and not internal_default_value.is_single:
            raise MalformedEntryConfig("Single value select has multi-string default")

        return AnyStringValueConverter.convert_any_string_value_int_to_ext(internal_default_value)

    @final
    def convert_value_source_dataset_field_ext_to_int(
        self,
        dash_element: _SELECTOR_TV,
        ext_value_source: ext.ControlValueSourceDatasetField,
    ) -> dashboards.DatasetControlSource:
        assert dash_element.source is ext_value_source

        int_default_value = self.convert_default_value_ext_to_int(dash_element)

        return dashboards.DatasetControlSourceSelect(
            **self.resolve_int_control_source_ds_kwargs(ext_value_source),
            operation=convert_enum_by_name_allow_none(dash_element.comparison_operation, charts.Operation),
            showTitle=dash_element.show_title,
            multiselectable=self.MULTISELECTABLE,
            defaultValue=int_default_value,
        )


class MultiSelectorElementConverter(AnySelectorElementConverter[ext.DashControlMultiSelect]):
    TARGET_CLS = ext.DashControlMultiSelect
    MULTISELECTABLE = True

    def convert_default_value_ext_to_int(self, selector: ext.DashControlMultiSelect) -> Optional[SingleOrMultiString]:
        return AnyStringValueConverter.convert_any_string_value_ext_to_int(selector.default_value)

    @classmethod
    def get_default_internal_operation(cls) -> charts.Operation:
        return charts.Operation.IN


class SingleValueSelectorElementConverter(AnySelectorElementConverter[ext.DashControlSelect]):
    TARGET_CLS = ext.DashControlSelect
    MULTISELECTABLE = False

    def convert_default_value_ext_to_int(self, selector: ext.DashControlSelect) -> Optional[SingleOrMultiString]:
        return AnyStringValueConverter.convert_any_string_value_ext_to_int(selector.default_value)

    @classmethod
    def get_default_internal_operation(cls) -> charts.Operation:
        return charts.Operation.EQ


_DATE_CONTROL_TV = TypeVar("_DATE_CONTROL_TV", bound=ext.DashControlGuided)


class AnyDateElementConverter(BaseControlElementConverter[_DATE_CONTROL_TV]):
    IS_RANGE: ClassVar[bool]

    def convert_default_value_int_to_ext(
        self,
        tab_item: dashboards.ItemControl,
    ) -> Optional[ext.Value]:
        values_source = tab_item.data.source
        assert isinstance(values_source, dashboards.FieldSetCommonControlSourceDate)
        assert (
            bool(values_source.isRange) is self.IS_RANGE
        ), f"Converter {type(self)=} does not support {values_source.isRange=}"

        assert isinstance(values_source, dashboards.CommonGuidedControlSource)
        return AnyStringValueConverter.convert_any_string_value_int_to_ext(
            values_source.defaultValue,
            # TODO FIX: BI-3005 See comment below
            empty_single_string_to_none=True,
        )

    def convert_value_source_dataset_field_ext_to_int(
        self,
        dash_element: _DATE_CONTROL_TV,
        ext_value_source: ext.ControlValueSourceDatasetField,
    ) -> dashboards.DatasetControlSource:
        assert dash_element.source is ext_value_source

        return dashboards.DatasetControlSourceDate(
            **self.resolve_int_control_source_ds_kwargs(ext_value_source),
            operation=convert_enum_by_name_allow_none(dash_element.comparison_operation, charts.Operation),
            showTitle=dash_element.show_title,
            isRange=self.IS_RANGE,
            defaultValue=AnyStringValueConverter.convert_any_string_value_ext_to_int(
                dash_element.default_value,
                # TODO FIX: BI-3005 WHY IT NOT ACCEPT NULL?!!!! IT even RETURNS NULL when is created via UI
                none_to_empty_string=True,
            ),
        )


class SingleDateElementConverter(AnyDateElementConverter[ext.DashControlDate]):
    TARGET_CLS = ext.DashControlDate
    IS_RANGE = False

    @classmethod
    def get_default_internal_operation(cls) -> charts.Operation:
        return charts.Operation.EQ


class DateRangeElementConverter(AnyDateElementConverter[ext.DashControlDateRange]):
    TARGET_CLS = ext.DashControlDateRange
    IS_RANGE = True

    @classmethod
    def get_default_internal_operation(cls) -> charts.Operation:
        return charts.Operation.BETWEEN


class TextInputElementConverter(BaseControlElementConverter[ext.DashControlTextInput]):
    TARGET_CLS = ext.DashControlTextInput

    def convert_default_value_int_to_ext(self, tab_item: dashboards.ItemControl) -> Optional[ext.Value]:
        values_source = tab_item.data.source
        assert isinstance(values_source, dashboards.FieldSetCommonControlSourceTextInput)
        assert isinstance(values_source, dashboards.CommonGuidedControlSource)
        internal_default_value = values_source.defaultValue

        if internal_default_value is None:
            return None

        if internal_default_value.is_single:
            # TODO FIX: BI-3005 WHY IT NOT ACCEPT NULL?!!!! IT even RETURNS NULL when is created via UI
            #  So to make round-trip correct with null in ext API we should do this
            if internal_default_value.as_single() == "":
                return None
            return ext.SingleStringValue(internal_default_value.as_single())

        raise MalformedEntryConfig("Text input has multi-string default")

    def convert_value_source_dataset_field_ext_to_int(
        self,
        dash_element: ext.DashControlTextInput,
        ext_value_source: ext.ControlValueSourceDatasetField,
    ) -> dashboards.DatasetControlSource:
        assert dash_element.source is ext_value_source

        return dashboards.DatasetControlSourceTextInput(
            **self.resolve_int_control_source_ds_kwargs(ext_value_source),
            operation=convert_enum_by_name_allow_none(dash_element.comparison_operation, charts.Operation),
            showTitle=dash_element.show_title,
            defaultValue=AnyStringValueConverter.convert_any_string_value_ext_to_int(
                dash_element.default_value,
                none_to_empty_string=True,
            ),
        )

    @classmethod
    def get_default_internal_operation(cls) -> charts.Operation:
        return charts.Operation.EQ
