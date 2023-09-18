from copy import deepcopy
from typing import (
    Any,
    Optional,
    Sequence,
    TypeVar,
)

import attr

from bi_external_api.domain.utils import ensure_tuple

from .common import ChartJSONBuilder
from .dataset import BaseDatasetJSONBuilder

_BUILDER_TV = TypeVar("_BUILDER_TV", bound="ChartJSONBuilderSingleDataset")


@attr.s(frozen=True)
class ChartJSONBuilderSingleDataset(ChartJSONBuilder):
    visualization: Optional[dict[str, Any]] = attr.ib(default=None, converter=deepcopy)  # type: ignore
    ad_hoc_aggregation_replace_data: Sequence[tuple[str, str]] = attr.ib(
        default=(),
        converter=ensure_tuple,  # type: ignore
    )
    ad_hoc_formula_data: Sequence[tuple[str, str, str]] = attr.ib(
        default=(),
        converter=ensure_tuple,  # type: ignore
    )
    ds_name: Optional[str] = attr.ib(default=None)

    def with_ds_name(self: _BUILDER_TV, ds_name: str) -> _BUILDER_TV:
        return attr.evolve(self, ds_name=ds_name)

    def with_visualization(self: _BUILDER_TV, visualization: dict[str, Any]) -> _BUILDER_TV:
        return attr.evolve(self, visualization=deepcopy(visualization))

    def add_aggregated_field(self: _BUILDER_TV, orig_field_id: str, new_agg: str) -> _BUILDER_TV:
        return attr.evolve(
            self,
            ad_hoc_aggregation_replace_data=[*self.ad_hoc_aggregation_replace_data, (orig_field_id, new_agg)],
        )

    @classmethod
    def generate_agg_field_id(cls, orig_id: str, *, agg: str) -> str:
        return f"{orig_id}_{agg}"

    def add_formula_field(self: _BUILDER_TV, f_id: str, *, cast: str, formula: str) -> _BUILDER_TV:
        return attr.evolve(
            self,
            ad_hoc_formula_data=[*self.ad_hoc_formula_data, (f_id, cast, formula)],
        )

    def build_internal(self) -> dict[str, Any]:
        agg_replace_ad_hoc_fields = [
            dict(
                field={
                    **self.resolve_dataset_field(f_id),
                    "id": self.generate_agg_field_id(f_id, agg=new_agg),
                    "title": f"The {f_id}_{new_agg}",
                    "aggregation": new_agg,
                },
                dataset_name=self.ds_name,
            )
            for f_id, new_agg in self.ad_hoc_aggregation_replace_data
        ]
        formula_ad_hoc_fields = [
            dict(
                field=BaseDatasetJSONBuilder.field_id_formula(formula, id=f_id, cast=cast),
                dataset_name=self.ds_name,
            )
            for f_id, cast, formula in self.ad_hoc_formula_data
        ]
        dataset_name = self.ds_name
        assert dataset_name is not None, "Can not build chart due to dataset name was not provided"

        visualization = self.visualization
        assert visualization is not None, "Can not build chart due to visualization was not provided"

        all_ad_hoc_fields = [*agg_replace_ad_hoc_fields, *formula_ad_hoc_fields]

        if not self.fill_defaults:
            for f in all_ad_hoc_fields:
                f.pop("dataset_name")

        chart = dict(
            datasets=[dataset_name],
            ad_hoc_fields=all_ad_hoc_fields,
            visualization=visualization,
            filters=[],  # TODO: more customization for acceptance tests
        )
        return chart
