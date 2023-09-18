import enum
from typing import ClassVar

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.attrs_model_mapper.base import AttribDescriptor
from bi_external_api.domain.internal.datasets import ResultSchemaField
from bi_external_api.domain.internal.dl_common import (
    DatasetAPIBaseModel,
    IntModelTags,
)


@enum.unique
class ChartActionType(enum.Enum):
    """Reduced copy of dataset action"""

    add_field = "add_field"
    clone_field = "clone_field"


@ModelDescriptor()
@attr.s(kw_only=True)
class ChartActionField(ResultSchemaField):
    datasetId: str = attr.ib(metadata=AttribDescriptor(tags=frozenset({IntModelTags.dataset_id})).to_meta())

    ignore_not_declared_fields = True
    # ignored_keys = ResultSchemaField.ignored_keys | {"datasetName"}


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="action")
@attr.s()
class ChartAction(DatasetAPIBaseModel):
    action: ClassVar[ChartActionType]


@ModelDescriptor()
@attr.s()
class ChartActionFieldAdd(ChartAction):
    action = ChartActionType.add_field

    field: ChartActionField = attr.ib()

    ignore_not_declared_fields = True
