from typing import (
    Any,
    ClassVar,
    Optional,
    Sequence,
)

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.attrs_model_mapper.base import AttribDescriptor
from bi_external_api.domain.internal.charts import DatasetFieldType
from bi_external_api.structs.singleormultistring import SingleOrMultiString
from dl_constants.enums import BIType

from .. import charts
from ..dl_common import (
    DatasetAPIBaseModel,
    IntModelTags,
)
from .enums import (
    ControlSelectType,
    ControlType,
)
from .tab_item_data import TabItemData


@attr.s(kw_only=True)
class CommonGuidedControlSource(DatasetAPIBaseModel):
    operation: Optional[charts.Operation] = attr.ib(default=None)
    defaultValue: Optional[SingleOrMultiString] = attr.ib(default=None)

    ignored_keys = {
        "innerTitle",
        "showInnerTitle",
    }


@attr.s(kw_only=True)
class FieldSetCommonControlSourceSelect:
    multiselectable: Optional[bool] = attr.ib(default=None)


@attr.s(kw_only=True)
class FieldSetCommonControlSourceDate:
    isRange: Optional[bool] = attr.ib(default=None)


@attr.s(kw_only=True)
class FieldSetCommonControlSourceTextInput:
    pass


#
#  Dataset based control
#


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="elementType")
@attr.s(kw_only=True, frozen=True)
class DatasetControlSource(CommonGuidedControlSource):
    elementType: ClassVar[ControlSelectType]

    # Actually showTitle should be filled by ext API. But sometimes they are non returned by dash-api
    showTitle: Optional[bool] = attr.ib(default=False)
    datasetId: str = attr.ib(metadata=AttribDescriptor(tags=frozenset({IntModelTags.dataset_id})).to_meta())
    datasetFieldId: str = attr.ib()
    # Actually next fields should be filled by ext API. But sometimes they are non returned by dash-api
    datasetFieldType: Optional[DatasetFieldType] = attr.ib(default=None)
    fieldType: Optional[BIType] = attr.ib(default=None)

    ignore_not_declared_fields = True


@ModelDescriptor()
@attr.s(kw_only=True, frozen=True)
class DatasetControlSourceSelect(DatasetControlSource, FieldSetCommonControlSourceSelect):
    elementType = ControlSelectType.select


@ModelDescriptor()
@attr.s(kw_only=True, frozen=True)
class DatasetControlSourceDate(DatasetControlSource, FieldSetCommonControlSourceDate):
    elementType = ControlSelectType.date


@ModelDescriptor()
@attr.s(kw_only=True, frozen=True)
class DatasetControlSourceTextInput(DatasetControlSource, FieldSetCommonControlSourceTextInput):
    elementType = ControlSelectType.input


#
# Manual control source
#
@ModelDescriptor()
@attr.s(kw_only=True, frozen=True)
class SelectorItem:
    title: str = attr.ib()
    value: str = attr.ib()


@ModelDescriptor()
@attr.s(kw_only=True, frozen=True)
class AcceptableDates:
    from_: str = attr.ib(metadata=AttribDescriptor(serialization_key="from").to_meta())
    to: str = attr.ib()


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="elementType")
@attr.s(kw_only=True, frozen=True)
class ManualControlSource(CommonGuidedControlSource):
    elementType: ClassVar[ControlSelectType]

    # Actually showTitle should be filled by ext API. But sometimes they are non returned by dash-api
    showTitle: Optional[bool] = attr.ib(default=False)
    fieldName: str = attr.ib()

    ignore_not_declared_fields = True


@ModelDescriptor()
@attr.s()
class ManualControlSourceSelect(ManualControlSource, FieldSetCommonControlSourceSelect):
    elementType = ControlSelectType.select

    # TODO FIX: BI-3005 Ask frontend why here `multiselectable` is strict bool,
    #  but in DatasetControlSourceSelect is optional?
    #  moreover if multiselect is disabled in UI - in DatasetControlSourceSelect we got multiselectable=undefined
    # If values not set - dash-api does not send this field. This is why default set to empty tuple
    acceptableValues: Sequence[SelectorItem] = attr.ib(default=())


@ModelDescriptor()
@attr.s()
class ManualControlSourceDate(ManualControlSource, FieldSetCommonControlSourceDate):
    elementType = ControlSelectType.date

    isRange: bool = attr.ib()
    acceptableValues: AcceptableDates = attr.ib(default=None)


@ModelDescriptor()
@attr.s()
class ManualControlSourceTextInput(ManualControlSource, FieldSetCommonControlSourceTextInput):
    elementType = ControlSelectType.input


#
# External control source
#
@ModelDescriptor()
@attr.s()
class ExternalControlSource:
    chartId: str = attr.ib()


#
# Composition
#
@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="sourceType")
@attr.s(kw_only=True, frozen=True)
class ControlData(TabItemData):
    sourceType: ClassVar[ControlType]

    title: str = attr.ib()
    source: Any  # this is not an attr.ib consciously


@ModelDescriptor()
@attr.s()
class DatasetBasedControlData(ControlData):
    sourceType = ControlType.dataset

    source: DatasetControlSource = attr.ib()


@ModelDescriptor()
@attr.s()
class ManualControlData(ControlData):
    sourceType = ControlType.manual

    source: ManualControlSource = attr.ib()


@ModelDescriptor()
@attr.s()
class ExternalControlData(ControlData):
    sourceType = ControlType.external

    source: ExternalControlSource = attr.ib()
