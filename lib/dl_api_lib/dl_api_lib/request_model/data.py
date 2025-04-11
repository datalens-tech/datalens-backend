from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Optional,
    TypeVar,
)

import attr

from dl_api_lib.enums import DatasetAction
from dl_api_lib.query.formalization.raw_pivot_specs import RawPivotSpec
from dl_api_lib.query.formalization.raw_specs import (
    RawFilterFieldSpec,
    RawQuerySpecUnion,
    RawResultSpec,
)
from dl_constants.enums import (
    AggregationFunction,
    CalcMode,
    FieldType,
    ManagedBy,
    UserDataType,
)
from dl_core.fields import CalculationSpec
from dl_model_tools.typed_values import BIValue


@attr.s(frozen=True, kw_only=True)
class Action:
    action: DatasetAction = attr.ib()
    order_index: int = attr.ib()
    managed_by: Optional[ManagedBy] = attr.ib(default=None)


@attr.s(frozen=True, kw_only=True)
class FieldBase:
    guid: Optional[str] = attr.ib(default=None)
    strict: Optional[bool] = attr.ib(default=None)

    def as_dict(self) -> Dict:
        return attr.asdict(self, recurse=False)


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class FieldAction(Action):
    field: FieldBase

    @property
    def serialized(self) -> Dict[str, Any]:
        serializer = lambda t, at, val: val.name if isinstance(val, Enum) else val  # noqa
        return attr.asdict(self, recurse=True, value_serializer=serializer)


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class SourceActionBase(Action):
    source: dict


@attr.s(frozen=True, kw_only=True)
class AddUpdateSourceAction(SourceActionBase):
    pass


@attr.s(frozen=True, kw_only=True)
class DeleteField(FieldBase):
    pass


@attr.s(frozen=True, kw_only=True)
class UpdateField(FieldBase):
    title: Optional[str] = attr.ib(default=None)
    source: Optional[str] = attr.ib(default=None)
    calc_mode: Optional[CalcMode] = attr.ib(default=None)
    calc_spec: Optional[CalculationSpec] = attr.ib(default=None)
    hidden: Optional[bool] = attr.ib(default=None)
    description: Optional[str] = attr.ib(default=None)
    aggregation: Optional[AggregationFunction] = attr.ib(default=None)
    has_auto_aggregation: Optional[bool] = attr.ib(default=None)
    lock_aggregation: Optional[bool] = attr.ib(default=None)
    formula: Optional[str] = attr.ib(default=None)
    guid_formula: Optional[str] = attr.ib(default=None)
    cast: Optional[UserDataType] = attr.ib(default=None)
    avatar_id: Optional[str] = attr.ib(default=None)
    new_id: Optional[str] = attr.ib(default=None)
    default_value: Optional[BIValue] = attr.ib(default=None)
    value_constraint: dict = attr.ib(default=None)
    template_enabled: Optional[bool] = attr.ib(default=None)


@attr.s(frozen=True, kw_only=True)
class AddField(UpdateField):
    title: str = attr.ib()
    type: Optional[FieldType] = attr.ib(default=None)


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class CloneField(UpdateField):
    title: str
    from_guid: str


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class SourceBase:
    id: str


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class ObligatoryFilterBase:
    id: str


@attr.s(frozen=True, kw_only=True)
class AddUpdateObligatoryFilter(ObligatoryFilterBase):
    field_guid: Optional[str] = attr.ib(default=None)
    default_filters: List[RawFilterFieldSpec] = attr.ib(default=[])
    managed_by: Optional[ManagedBy] = attr.ib(default=None)
    valid: Optional[bool] = attr.ib(default=None)


@attr.s(frozen=True, kw_only=True)
class DeleteObligatoryFilter(ObligatoryFilterBase):
    pass


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class ObligatoryFilterActionBase(Action):
    obligatory_filter: ObligatoryFilterBase


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class AddUpdateObligatoryFilterAction(ObligatoryFilterActionBase):
    obligatory_filter: AddUpdateObligatoryFilter


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class DeleteObligatoryFilterAction(ObligatoryFilterActionBase):
    obligatory_filter: DeleteObligatoryFilter


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class ReplaceConnection:
    id: str
    new_id: str


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class ReplaceConnectionAction(Action):
    connection: ReplaceConnection


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class RelationActionBase(Action):
    avatar_relation: dict


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class SourceAvatarBase(Action):
    id: str


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class SourceAvatar(SourceAvatarBase):
    source_id: Optional[str] = attr.ib(default=None)
    title: Optional[str] = attr.ib(default=None)
    is_root: Optional[bool] = attr.ib(default=None)
    managed_by: Optional[ManagedBy] = attr.ib(default=None)


@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class AvatarActionBase(Action):
    source_avatar: SourceAvatar
    disable_fields_update: bool


_DRM_TV = TypeVar("_DRM_TV", bound="DataRequestModel")


@attr.s()
class DataRequestModel:
    # Dataset state
    dataset: Optional[Dict[str, Any]] = attr.ib(kw_only=True, default=None)  # TODO: schematize
    # Updates to apply to the given dataset state
    updates: List[Action] = attr.ib(kw_only=True, factory=list)

    # Specifies what the data query will consist of
    raw_query_spec_union: RawQuerySpecUnion = attr.ib(kw_only=True, factory=RawQuerySpecUnion)

    # Other parameters controlling the request
    add_fields_data: bool = attr.ib(kw_only=True, default=False)
    with_totals: bool = attr.ib(kw_only=True, default=False)
    autofill_legend: bool = attr.ib(kw_only=True, default=False)
    dataset_revision_id: Optional[str] = attr.ib(kw_only=True, default=None)

    def clone(self: _DRM_TV, **updates: Any) -> _DRM_TV:
        return attr.evolve(self, **updates)


@attr.s()
class ResultDataRequestModel(DataRequestModel):
    result: Optional[RawResultSpec] = attr.ib(kw_only=True, default=None)


@attr.s()
class PivotDataRequestModel(DataRequestModel):
    pivot: RawPivotSpec = attr.ib(kw_only=True, factory=RawPivotSpec)
