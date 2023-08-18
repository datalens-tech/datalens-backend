import enum
from typing import ClassVar, Optional

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor, AttribDescriptor
from bi_external_api.attrs_model_mapper.utils import MText
from .model_tags import ExtModelTags


class ChartFieldSourceKind(enum.Enum):
    ref = enum.auto()
    measure_names = enum.auto()
    measure_values = enum.auto()


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True, kw_only=True)
class ChartFieldSource:
    kind: ClassVar[ChartFieldSourceKind]


@ModelDescriptor()
@attr.s(frozen=True)
class ChartFieldRef(ChartFieldSource):
    kind = ChartFieldSourceKind.ref

    id: str = attr.ib()
    dataset_name: Optional[str] = attr.ib(
        kw_only=True, default=None,
        metadata=AttribDescriptor(
            tags=frozenset({ExtModelTags.dataset_name}),
            description=MText(
                ru="Имя датасета, в котором нужно найти поле."
                   " Если чарт строится по одному датасету, имя можно не указывать.",
                en="The dataset name in which you need to find a field."
                    " If the chart is built from a single dataset, you may skip specifying its name.",
            )
        ).to_meta(),
    )

    @property
    def strict_dataset_name(self) -> str:
        ds_name = self.dataset_name
        assert ds_name is not None, "ChartFieldRef.strict_dataset_name was called with dataset_name=None"
        return ds_name


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class MeasureNames(ChartFieldSource):
    kind = ChartFieldSourceKind.measure_names


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class MeasureValues(ChartFieldSource):
    kind = ChartFieldSourceKind.measure_values
