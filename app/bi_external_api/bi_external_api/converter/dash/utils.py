from functools import cached_property
from typing import (
    ClassVar,
    FrozenSet,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
)

import attr

from bi_external_api.converter.converter_exc import NotSupportedYet
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import (
    charts,
    dashboards,
)
from bi_external_api.structs.mappings import FrozenMappingStrToStrOrStrSeq
from bi_external_api.structs.singleormultistring import SingleOrMultiString

_SOURCE_TV = TypeVar("_SOURCE_TV")


@attr.s(frozen=True)
class SourceAccessor:
    source: dashboards.CommonGuidedControlSource = attr.ib()

    def ensure_source_class(self, t: Type[_SOURCE_TV], error_msg: str) -> _SOURCE_TV:
        source = self.source
        assert isinstance(source, t), f"{error_msg}: {type(source)=}"
        return source

    def ensure_guided_source(self, err_msg: str) -> dashboards.CommonGuidedControlSource:
        return self.ensure_source_class(dashboards.CommonGuidedControlSource, err_msg)

    def resolve_operation(self) -> Optional[charts.Operation]:
        source = self.ensure_guided_source(err_msg="Can not resolve comparison operation for source")
        return source.operation

    def resolve_operation_with_fallback(self, fallback: charts.Operation) -> charts.Operation:
        op = self.resolve_operation()
        if op is None:
            return fallback
        return op

    def resolve_default_value(self) -> Optional[SingleOrMultiString]:
        source = self.ensure_guided_source(err_msg="Can not resolve default value for source")
        return source.defaultValue

    def resolve_parameter_name(self) -> str:
        source = self.source

        if isinstance(source, dashboards.DatasetControlSource):
            return source.datasetFieldId

        if isinstance(source, dashboards.ManualControlSource):
            raise NotSupportedYet("Manual selectors are not yet implemented in API")

        raise AssertionError(f"Can not resolve control parameter name for {type(source)=}")

    def resolve_expected_params_defaults(self, fallback_operation: charts.Operation) -> FrozenMappingStrToStrOrStrSeq:
        effective_operation = self.resolve_operation_with_fallback(fallback_operation)
        param_name = self.resolve_parameter_name()
        default_value = self.resolve_default_value()

        return FrozenMappingStrToStrOrStrSeq(
            {param_name: ParametersCodec.encode_any_parameter(effective_operation, default_value)}
        )


@attr.s(frozen=True)
class TabItemControlAccessor:
    tab_item: dashboards.ItemControl = attr.ib()

    @cached_property
    def source_accessor(self) -> SourceAccessor:
        return SourceAccessor(self.tab_item.data.source)

    def get_show_title(self) -> bool:
        data = self.tab_item.data

        if isinstance(data, (dashboards.ManualControlData, dashboards.DatasetBasedControlData)):
            show_title = data.source.showTitle
            return show_title if show_title is not None else False
        raise AssertionError(f"Control data {type(data)} has no 'show title' info")


class AnyStringValueConverter:
    @classmethod
    def convert_any_string_value_int_to_ext(
        cls, int_value: Optional[SingleOrMultiString], *, empty_single_string_to_none: bool = False
    ) -> Optional[ext.Value]:
        if int_value is None:
            return None
        if int_value.is_single:
            if empty_single_string_to_none and int_value.as_single() == "":
                return None
            return ext.SingleStringValue(int_value.as_single())
        return ext.MultiStringValue(int_value.as_sequence())

    @classmethod
    def convert_any_string_value_ext_to_int(
        cls, ext_value: Optional[ext.Value], *, none_to_empty_string: bool = False
    ) -> Optional[SingleOrMultiString]:
        if ext_value is None:
            return SingleOrMultiString.from_string("") if none_to_empty_string else None
        if isinstance(ext_value, ext.MultiStringValue):
            return SingleOrMultiString.from_sequence(ext_value.values)
        if isinstance(ext_value, ext.SingleStringValue):
            return SingleOrMultiString.from_string(ext_value.value)
        raise AssertionError(f"Type {type(ext_value)} can not be converter to SingleOrMultiString")


class ParametersCodec:
    multi_string_ops: ClassVar[FrozenSet[charts.Operation]] = frozenset(
        {
            charts.Operation.IN,
            charts.Operation.NIN,
        }
    )

    @classmethod
    def encode_single_param_value(cls, value: str, op: charts.Operation) -> str:
        if op in (charts.Operation.IN, charts.Operation.EQ, charts.Operation.BETWEEN):
            return value
        else:
            return f"__{op.name.lower()}_{value}"

    @classmethod
    def encode_any_parameter(
        cls,
        int_operation: charts.Operation,
        value: Optional[SingleOrMultiString],
    ) -> Union[str, Sequence[str]]:
        if value is None:
            return ""

        # Empty string also treated as `no value`
        if value.is_single and value.as_single() == "":
            return ""

        if int_operation in cls.multi_string_ops:
            all_options_values: Sequence[str]

            assert not value.is_single, f"Got single string value as arg for multi-string operation: {int_operation}"

            return tuple(
                cls.encode_single_param_value(option_value, int_operation) for option_value in value.as_sequence()
            )

        else:
            assert value.is_single, f"Got multi string value as arg for single-string operation: {int_operation}"

            return cls.encode_single_param_value(
                value.as_single(),
                int_operation,
            )
