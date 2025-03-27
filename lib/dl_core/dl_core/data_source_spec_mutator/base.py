import attr

from dl_constants.enums import CalcMode
from dl_core.data_source_spec import DataSourceSpec
from dl_core.us_dataset import Dataset
from dl_model_tools.typed_values import BIValue


@attr.s(frozen=True)
class DataSourceSpecMutator:
    _dataset: Dataset = attr.ib(kw_only=True)

    def _get_parameter_values(self) -> dict[str, BIValue]:
        return {
            field.title: field.default_value
            for field in self._dataset.result_schema.fields
            if field.calc_mode == CalcMode.parameter and field.default_value is not None
        }

    def _template_str(self, string: str) -> str:
        parameter_values = self._get_parameter_values()

        for parameter, bi_value in parameter_values.items():
            string = string.replace(f"{{{parameter}}}", str(bi_value.value))

        return string

    def mutate(self, spec: DataSourceSpec) -> DataSourceSpec:
        return spec


__all__ = [
    "DataSourceSpecMutator",
]
