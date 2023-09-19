import abc
from typing import (
    Generic,
    Optional,
    Sequence,
    TypeVar,
    final,
)

import attr

from bi_external_api.converter.charts.chart_converter import BaseChartConverter
from bi_external_api.converter.main import DatasetConverter
from bi_external_api.domain import external as ext


_INSTANCE_TV = TypeVar("_INSTANCE_TV", bound=ext.EntryInstance)
_DATA_TV = TypeVar("_DATA_TV")
_BUILDER_TV = TypeVar("_BUILDER_TV", bound="EntryBuilder")


@attr.s(frozen=True)
class EntryBuilder(Generic[_INSTANCE_TV, _DATA_TV], metaclass=abc.ABCMeta):
    entry_name: Optional[str] = attr.ib(default=None, kw_only=True)
    fill_defaults: bool = attr.ib(default=False, kw_only=True)

    @property
    def entry_name_strict(self) -> str:
        name = self.entry_name
        assert name is not None
        return name

    def with_fill_defaults(self: _BUILDER_TV, do: bool = True) -> _BUILDER_TV:
        return attr.evolve(self, fill_defaults=do)

    def with_entry_name(self: _BUILDER_TV, name: Optional[str]) -> _BUILDER_TV:
        return attr.evolve(self, entry_name=name)

    @abc.abstractmethod
    def _build_instance_with_effective_name(self, name: str) -> _INSTANCE_TV:
        raise NotImplementedError()

    @abc.abstractmethod
    def _fill_data_defaults(self, data: _DATA_TV) -> _DATA_TV:
        raise NotImplementedError()

    @abc.abstractmethod
    def build_internal(self) -> _DATA_TV:
        raise NotImplementedError()

    @final
    def build(self) -> _DATA_TV:
        data = self.build_internal()
        if self.fill_defaults:
            return self._fill_data_defaults(data)
        return data

    @final
    def build_instance(self, name: Optional[str] = None) -> _INSTANCE_TV:
        effective_name = name if name is not None else self.entry_name
        assert effective_name is not None, "Can not build instance due to name was not provided"
        return self._build_instance_with_effective_name(effective_name)


@attr.s(frozen=True)
class DatasetBuilder(EntryBuilder[ext.DatasetInstance, ext.Dataset], metaclass=abc.ABCMeta):
    def _build_instance_with_effective_name(self, name: str) -> ext.DatasetInstance:
        return ext.DatasetInstance(
            name=name,
            dataset=self.build(),
        )

    def _fill_data_defaults(self, data: ext.Dataset) -> ext.Dataset:
        return DatasetConverter.fill_defaults(data)


_CHART_BUILDER_TV = TypeVar("_CHART_BUILDER_TV", bound="ChartBuilder")


@attr.s(frozen=True)
class ChartBuilder(EntryBuilder[ext.ChartInstance, ext.Chart], metaclass=abc.ABCMeta):
    source_dataset: Optional[ext.Dataset] = attr.ib(default=None, kw_only=True)

    def with_source_dataset(self: _CHART_BUILDER_TV, dataset: ext.Dataset) -> _CHART_BUILDER_TV:
        return attr.evolve(self, source_dataset=dataset)

    def resolve_dataset_field(self, f_id: str, ad_hoc_fields: Sequence[ext.AdHocField] = ()) -> ext.DatasetField:
        dataset = self.source_dataset
        assert dataset is not None, "Source dataset was not set in ChartBuilder"
        try:
            return next(f for f in dataset.fields if f.id == f_id)
        except StopIteration:
            try:
                return next(f.field for f in ad_hoc_fields if f.field.id == f_id)
            except StopIteration:
                raise AssertionError(f"Field {f_id} was not found neither in source dataset nor in ad-hoc fields")

    def _build_instance_with_effective_name(self, name: str) -> ext.ChartInstance:
        return ext.ChartInstance(
            name=name,
            chart=self.build(),
        )

    def _fill_data_defaults(self, data: ext.Chart) -> ext.Chart:
        return BaseChartConverter.fill_defaults(data)


_DASH_BUILDER_TV = TypeVar("_DASH_BUILDER_TV", bound="DashBuilder")


@attr.s(frozen=True)
class DashBuilder(EntryBuilder[ext.DashInstance, ext.Dashboard], metaclass=abc.ABCMeta):
    def _build_instance_with_effective_name(self, name: str) -> ext.DashInstance:
        return ext.DashInstance(
            name=name,
            dashboard=self.build(),
        )

    def _fill_data_defaults(self, data: ext.Dashboard) -> ext.Dashboard:
        # TODO FIX: BI-3005 apply defaulting in when dash defaulting will be ready
        return data
