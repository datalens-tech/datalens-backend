import abc
from typing import TypeVar, Optional, final, Sequence, Any

import attr
from copy import deepcopy

_BUILDER_TV = TypeVar("_BUILDER_TV", bound="EntryJSONBuilder")


@attr.s(frozen=True)
class EntryJSONBuilder(metaclass=abc.ABCMeta):
    entry_name: Optional[str] = attr.ib(default=None, kw_only=True)
    fill_defaults: bool = attr.ib(default=False, kw_only=True)

    @property
    def entry_name_strict(self) -> str:
        name = self.entry_name
        assert name is not None
        return name

    def with_fill_defaults(self: _BUILDER_TV, do: bool = True) -> _BUILDER_TV:
        return attr.evolve(self, fill_defaults=do)

    @abc.abstractmethod
    def _build_instance_with_effective_name(self, name: str) -> dict[str, Any]:
        raise NotImplementedError()

    @abc.abstractmethod
    def build_internal(self) -> dict[str, Any]:
        raise NotImplementedError()

    @final
    def build(self) -> dict[str, Any]:
        data = self.build_internal()
        return data

    @final
    def build_instance(self, name: Optional[str] = None) -> dict[str, Any]:
        effective_name = name if name is not None else self.entry_name
        assert effective_name is not None, "Can not build instance due to name was not provided"
        return self._build_instance_with_effective_name(effective_name)


@attr.s(frozen=True)
class DatasetJSONBuilder(EntryJSONBuilder, metaclass=abc.ABCMeta):
    def _build_instance_with_effective_name(self, name: str) -> dict[str, Any]:
        return dict(
            name=name,
            dataset=self.build(),
        )


_CHART_BUILDER_TV = TypeVar("_CHART_BUILDER_TV", bound="ChartJSONBuilder")


@attr.s(frozen=True)
class ChartJSONBuilder(EntryJSONBuilder, metaclass=abc.ABCMeta):
    source_dataset: Optional[dict[str, Any]] = attr.ib(
        default=None,
        kw_only=True,
        converter=deepcopy,  # type: ignore
    )

    def with_source_dataset(self: _CHART_BUILDER_TV, dataset: dict[str, Any]) -> _CHART_BUILDER_TV:
        return attr.evolve(self, source_dataset=dataset)

    def resolve_dataset_field(self, f_id: str, ad_hoc_fields: Sequence[dict[str, Any]] = ()) -> dict[str, Any]:
        dataset = self.source_dataset
        assert dataset is not None, "Source dataset was not set in ChartBuilder"
        try:
            return deepcopy(next(f for f in dataset["fields"] if f["id"] == f_id))
        except StopIteration:
            try:
                return deepcopy(next(f["field"] for f in ad_hoc_fields if f["field"]["id"] == f_id))
            except StopIteration:
                raise AssertionError(f"Field {f_id} was not found neither in source dataset nor in ad-hoc fields")

    def _build_instance_with_effective_name(self, name: str) -> dict[str, Any]:
        return dict(
            name=name,
            chart=self.build(),
        )


@attr.s(frozen=True)
class DashJSONBuilder(EntryJSONBuilder, metaclass=abc.ABCMeta):
    def _build_instance_with_effective_name(self, name: str) -> dict[str, Any]:
        return dict(
            name=name,
            dashboard=self.build(),
        )
