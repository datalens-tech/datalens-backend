from __future__ import annotations

from datetime import (
    date,
    datetime,
)
from enum import Enum
import json
from typing import (
    Any,
    ClassVar,
    Collection,
    Generator,
    Generic,
    Optional,
    TypeVar,
    Union,
)
import uuid

import attr

from dl_constants.enums import (
    AggregationFunction,
    BinaryJoinOperator,
    CalcMode,
    ComponentErrorLevel,
    ComponentType,
    ConditionPartCalcMode,
    DataSourceType,
    FieldRole,
    FieldType,
    FieldVisibility,
    JoinType,
    LegendItemType,
    ManagedBy,
    OrderDirection,
    ParameterValueConstraintType,
    PivotHeaderRole,
    PivotRole,
    QueryBlockPlacementType,
    QueryItemRefType,
    RangeType,
    UserDataType,
    WhereClauseOperation,
)
from dl_rls.models import RLSEntry


class Action(Enum):
    add = "add"
    update = "update"
    delete = "delete"
    refresh = "refresh"


_API_OBJ_TV = TypeVar("_API_OBJ_TV", bound="ApiProxyObject")


@attr.s
class ApiProxyObject:
    id: str = attr.ib(kw_only=True, factory=lambda: str(uuid.uuid4()))
    # special attributes, not part of object data, postfixed with `_`
    created_: bool = attr.ib(kw_only=True, default=False)  # True means that the item has already been added via API

    def clone(self: _API_OBJ_TV, **kwargs: Any) -> _API_OBJ_TV:
        return attr.evolve(self, **kwargs)

    def prepare(self) -> None:
        """Custom initialization of properties"""

    def _make_action(self, action: Action, data: Optional[dict] = None) -> UpdateAction:
        return UpdateAction(
            action=action,
            item=self,
            custom_data=data,
        )

    def add(self) -> UpdateAction:
        """Generate ``add_*`` action"""
        return self._make_action(Action.add)

    def update(self, **data) -> UpdateAction:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        """Generate ``update_*`` action"""
        return self._make_action(Action.update, data=data)

    def delete(self) -> UpdateAction:
        """Generate ``delete_*`` action"""
        return self._make_action(Action.delete)

    def set_name(self, name: str) -> None:
        """
        Called when ``self`` is added to a container under alias ``name``.
        Can be used to define item's name /title.
        """


@attr.s(auto_attribs=True)
class UpdateAction:
    action: Action
    item: ApiProxyObject
    custom_data: Optional[dict]
    order_index: int = 0


_ITEM_TV = TypeVar("_ITEM_TV", bound=ApiProxyObject)


class Container(Generic[_ITEM_TV]):
    """
    A partially dict-like container for nested items where each item has its own string alias.
    Items can be fetched both by integer (by index) and string (by alias) keys.
    """

    def __init__(self, data: Union[list, tuple, dict, "Container"] = None):  # type: ignore  # 2024-01-24 # TODO: Incompatible default for argument "data" (default has type "None", argument has type "list[Any] | tuple[Any, ...] | dict[Any, Any] | Container[Any]")  [assignment]
        self._item_ids: list[str] = []  # order list of object IDs
        self._items: dict[str, _ITEM_TV] = {}  # objects by ID
        self._id_by_alias: dict[str, str] = {}  # alias -> ID

        if data:
            self.__iadd__(data)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._items})"

    def __str__(self) -> str:
        return self.__repr__()

    def __iter__(self) -> Generator[_ITEM_TV, None, None]:
        """
        Iterate over items in container.
        Note that this is different from ``dict`` in that it yields values, not keys.
        """
        for id in self._item_ids:
            yield self._items[id]

    def __setitem__(self, alias: str, item: _ITEM_TV):  # type: ignore  # TODO: fix
        """
        Add item to container under given alias.
        Item order is preserved.
        """
        if not isinstance(alias, str):
            raise TypeError(f"Invalid key type for item assignment in container: {type(alias)}")

        self._items[item.id] = item
        self._item_ids.append(item.id)
        self._id_by_alias[alias] = item.id
        if hasattr(item, "set_name"):
            item.set_name(alias)

    def __getitem__(self, key: Union[str, int]) -> _ITEM_TV:
        """Return item either by index (if ``key`` is ``int``) or alias (``key`` is ``str``)"""
        if isinstance(key, str):
            # key is an alias
            return self._items[self._id_by_alias[key]]
        if isinstance(key, int):
            # key is an index
            return self._items[self._item_ids[key]]

        raise TypeError(f"Invalid key type for container: {type(key)}")

    def items(self) -> Generator[tuple[str, _ITEM_TV], None, None]:
        """Just like ``dict.items()`` - iterate over key-value tuples."""
        alias_by_id = {id: alias for alias, id in self._id_by_alias.items()}
        for id in self._item_ids:
            yield alias_by_id[id], self._items[id]

    def __iadd__(self, other: Union[list, tuple, dict, Container]) -> Container:
        """
        Extend by adding items from another collection.
        If items don't have aliases, then use IDs as aliases
        """
        if isinstance(other, (list, tuple)):
            other = {item.id: item for item in other}
        if isinstance(other, (dict, Container)):
            for alias, item in other.items():
                self[alias] = item
        else:
            raise KeyError(type(other))
        return self

    def __add__(self, other: Union[list, tuple, dict, Container]) -> Container:
        """
        Return new container that consists of items from ``self`` and from ``other``.
        The new container is constructed using the logic in ``__iadd__``.
        """
        new_cont = self.__class__()
        new_cont += other
        return new_cont

    def __len__(self) -> int:
        """Return the number of items in the container."""
        return len(self._items)

    def get_alias(self, id: str) -> Optional[str]:
        """Find item by its ID and return its alias."""
        alias_by_id = {id: alias for alias, id in self._id_by_alias.items()}
        return alias_by_id.get(id)

    def __bool__(self) -> bool:
        return bool(self._items)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Container):
            return False
        return (
            other._items == self._items
            and other._item_ids == self._item_ids
            and other._id_by_alias == self._id_by_alias
        )


@attr.s
class DataSource(ApiProxyObject):
    connection_id: str = attr.ib(default=None)
    source_type: Optional[DataSourceType] = attr.ib(default=None, converter=DataSourceType.normalize)  # type: ignore  # 2024-01-24 # TODO: Unsupported converter, only named functions, types and lambdas are currently supported  [misc]
    parameters: dict = attr.ib(default=None)
    raw_schema: list = attr.ib(factory=list)
    index_info_set: Optional[list] = attr.ib(default=None)
    title: str = attr.ib(default=None)
    managed_by: ManagedBy = attr.ib(default=ManagedBy.user)
    valid: bool = attr.ib(default=True)

    def set_name(self, name: str) -> None:
        if self.title is None:
            self.title = name

    def prepare(self) -> None:
        super().prepare()
        if self.title is None:
            self.title = f"Source {self.id}"

    def avatar(self, **kwargs: Any) -> SourceAvatar:
        return SourceAvatar(source_id=self.id, **kwargs)

    def refresh(self, force_update_fields: bool = False) -> "UpdateAction":
        """Generate ``refresh_source`` action"""
        return self._make_action(Action.refresh, data={"force_update_fields": force_update_fields})


@attr.s
class SourceAvatar(ApiProxyObject):
    source_id: str = attr.ib(default=None)
    title: str = attr.ib(default=None)
    is_root: bool = attr.ib(default=False)
    managed_by: ManagedBy = attr.ib(default=ManagedBy.user)
    valid: bool = attr.ib(default=True)

    def prepare(self) -> None:
        super().prepare()
        if self.title is None:
            self.title = f"Avatar {self.id}"

    def set_name(self, name: str) -> None:
        if self.title is None:
            self.title = name

    def join(
        self,
        other: "SourceAvatar",
        conditions: list["JoinCondition"] = None,  # type: ignore  # 2024-01-24 # TODO: Incompatible default for argument "conditions" (default has type "None", argument has type "list[JoinCondition]")  [assignment]
        join_type: JoinType = JoinType.inner,
    ) -> "AvatarRelation":
        return AvatarRelation(
            left_avatar_id=self.id,
            right_avatar_id=other.id,
            conditions=conditions or [],
            join_type=join_type,
        )

    def field(self, **kwargs) -> "ResultField":  # type: ignore  # TODO: fix
        return ResultField(avatar_id=self.id, **kwargs)


class JoinPart:
    calc_mode: ConditionPartCalcMode


@attr.s
class DirectJoinPart(JoinPart):
    calc_mode: ConditionPartCalcMode = ConditionPartCalcMode.direct
    source: str = attr.ib(default=None)


@attr.s
class ResultFieldJoinPart(JoinPart):
    calc_mode: ConditionPartCalcMode = ConditionPartCalcMode.result_field
    field_id: str = attr.ib(default=None)


@attr.s
class FormulaJoinPart(JoinPart):
    calc_mode: ConditionPartCalcMode = ConditionPartCalcMode.formula
    formula: str = attr.ib(default=None)


class ConditionMakerMixin:
    """Mixin that enables construction of join conditions using Python's comparison operators"""

    def _simple_condition(self, other: "ConditionMakerMixin", operator: BinaryJoinOperator) -> "JoinCondition":
        return JoinCondition(
            left_part=self.get_cpart_from_self(), right_part=other.get_cpart_from_self(), operator=operator
        )

    def __eq__(self, other: "ConditionMakerMixin"):  # type: ignore  # TODO: fix
        return self._simple_condition(other, BinaryJoinOperator.eq)

    def __ne__(self, other: "ConditionMakerMixin"):  # type: ignore  # TODO: fix
        return self._simple_condition(other, BinaryJoinOperator.ne)

    def __lt__(self, other: "ConditionMakerMixin"):  # type: ignore  # TODO: fix
        return self._simple_condition(other, BinaryJoinOperator.lt)

    def __le__(self, other: "ConditionMakerMixin"):  # type: ignore  # TODO: fix
        return self._simple_condition(other, BinaryJoinOperator.lte)

    def __gt__(self, other: "ConditionMakerMixin"):  # type: ignore  # TODO: fix
        return self._simple_condition(other, BinaryJoinOperator.gt)

    def __ge__(self, other: "ConditionMakerMixin"):  # type: ignore  # TODO: fix
        return self._simple_condition(other, BinaryJoinOperator.gte)

    def get_cpart_from_self(self) -> JoinPart:
        raise NotImplementedError


@attr.s
class _Column:
    # use attr.s on superclass of the "real" class so that comparison methods from ConditionMakerMixin are used
    title: str = attr.ib(default=None)
    name: str = attr.ib(default=None)
    user_type: Optional[UserDataType] = attr.ib(default=None, converter=UserDataType.normalize)  # type: ignore  # 2024-01-24 # TODO: Unsupported converter, only named functions, types and lambdas are currently supported  [misc]
    native_type: Optional[dict] = attr.ib(default=None)
    nullable: bool = attr.ib(default=True)
    description: str = attr.ib(default="")
    has_auto_aggregation: bool = attr.ib(default=False)
    lock_aggregation: bool = attr.ib(default=False)


class Column(ConditionMakerMixin, _Column):  # type: ignore  # TODO: fix
    def get_cpart_from_self(self) -> JoinPart:
        return DirectJoinPart(source=self.name)


@attr.s(auto_attribs=True)
class JoinCondition:
    operator: BinaryJoinOperator
    left_part: JoinPart
    right_part: JoinPart


@attr.s
class AvatarRelation(ApiProxyObject):
    left_avatar_id: str = attr.ib(default=None)
    right_avatar_id: str = attr.ib(default=None)
    conditions: list[JoinCondition] = attr.ib(factory=list)
    join_type: JoinType = attr.ib(default=JoinType.inner)
    managed_by: ManagedBy = attr.ib(default=ManagedBy.user)
    required: bool = attr.ib(default=False)

    def on(self, *conditions: JoinCondition) -> AvatarRelation:
        self.conditions += conditions
        return self

    def require(self) -> AvatarRelation:
        self.required = True
        return self


_INNER_TYPE = TypeVar("_INNER_TYPE")


@attr.s
class ParameterValue(Generic[_INNER_TYPE]):
    type: UserDataType
    value: _INNER_TYPE = attr.ib()


@attr.s
class StringParameterValue(ParameterValue[str]):
    type: UserDataType = UserDataType.string


@attr.s
class IntegerParameterValue(ParameterValue[int]):
    type: UserDataType = UserDataType.integer


@attr.s
class FloatParameterValue(ParameterValue[float]):
    type: UserDataType = UserDataType.float


@attr.s
class DateParameterValue(ParameterValue[date]):
    type: UserDataType = UserDataType.date


@attr.s
class DateTimeParameterValue(ParameterValue[datetime]):
    type: UserDataType = UserDataType.datetime


@attr.s
class DateTimeTZParameterValue(ParameterValue[datetime]):
    type: UserDataType = UserDataType.datetimetz


@attr.s
class GenericDateTimeParameterValue(ParameterValue[datetime]):
    type: UserDataType = UserDataType.genericdatetime


@attr.s
class BooleanParameterValue(ParameterValue[bool]):
    type: UserDataType = UserDataType.boolean


@attr.s
class GeoPointParameterValue(ParameterValue[list[Union[int, float]]]):
    type: UserDataType = UserDataType.geopoint


@attr.s
class GeoPolygonParameterValue(ParameterValue[list[list[list[Union[int, float]]]]]):
    type: UserDataType = UserDataType.geopolygon


@attr.s
class UuidParameterValue(ParameterValue[str]):
    type: UserDataType = UserDataType.uuid


@attr.s
class MarkupParameterValue(ParameterValue[str]):
    type: UserDataType = UserDataType.markup


@attr.s
class ArrayStrParameterValue(ParameterValue[list[str]]):
    type: UserDataType = UserDataType.array_str


@attr.s
class ArrayIntParameterValue(ParameterValue[list[int]]):
    type: UserDataType = UserDataType.array_int


@attr.s
class ArrayFloatParameterValue(ParameterValue[list[float]]):
    type: UserDataType = UserDataType.array_float


@attr.s
class TreeStrParameterValue(ParameterValue[list[str]]):
    type: UserDataType = UserDataType.tree_str


@attr.s
class BaseParameterValueConstraint:
    type: ParameterValueConstraintType


@attr.s
class NullParameterValueConstraint(BaseParameterValueConstraint):
    type: ParameterValueConstraintType = ParameterValueConstraintType.null


@attr.s
class RangeParameterValueConstraint(BaseParameterValueConstraint):
    type: ParameterValueConstraintType = ParameterValueConstraintType.range
    min: Optional[ParameterValue] = attr.ib(default=None)
    max: Optional[ParameterValue] = attr.ib(default=None)


@attr.s
class SetParameterValueConstraint(BaseParameterValueConstraint):
    type: ParameterValueConstraintType = ParameterValueConstraintType.set
    values: list[ParameterValue] = attr.ib(factory=list)


@attr.s
class EqualsParameterValueConstraint(BaseParameterValueConstraint):
    type: ParameterValueConstraintType = ParameterValueConstraintType.equals
    value: ParameterValue = attr.ib()


@attr.s
class NotEqualsParameterValueConstraint(BaseParameterValueConstraint):
    type: ParameterValueConstraintType = ParameterValueConstraintType.not_equals
    value: ParameterValue = attr.ib()


@attr.s
class RegexParameterValueConstraint(BaseParameterValueConstraint):
    type: ParameterValueConstraintType = ParameterValueConstraintType.regex
    pattern: str = attr.ib()


@attr.s
class DefaultParameterValueConstraint(BaseParameterValueConstraint):
    type: ParameterValueConstraintType = ParameterValueConstraintType.default


class CollectionParameterValueConstraint(BaseParameterValueConstraint):
    type: ParameterValueConstraintType = ParameterValueConstraintType.collection
    constraints: list[BaseParameterValueConstraint]


def _make_pivot_role_spec(
    role: PivotRole,
    annotation_type: Optional[str] = None,
    target_legend_item_ids: Optional[list[int]] = None,
    direction: OrderDirection = OrderDirection.asc,
    measure_sorting_settings: Optional[PivotMeasureSorting] = None,
    allow_roles: Optional[Collection[PivotRole]] = None,
) -> PivotRoleSpec:
    if allow_roles is not None:
        assert role in allow_roles

    pivot_role_spec: PivotRoleSpec
    if role == PivotRole.pivot_annotation:
        pivot_role_spec = AnnotationPivotRoleSpec(
            role=role,
            annotation_type=annotation_type,  # type: ignore  # 2024-01-24 # TODO: Argument "annotation_type" to "AnnotationPivotRoleSpec" has incompatible type "str | None"; expected "str"  [arg-type]
            target_legend_item_ids=target_legend_item_ids,
        )
    elif role == PivotRole.pivot_measure:
        pivot_role_spec = PivotMeasureRoleSpec(
            role=role,
            sorting=measure_sorting_settings,
        )
    elif role in (PivotRole.pivot_row, PivotRole.pivot_column):
        pivot_role_spec = DimensionPivotRoleSpec(
            role=role,
            direction=direction,
        )
    else:
        raise ValueError(role)

    return pivot_role_spec


@attr.s
class _ResultField(ApiProxyObject):
    title: Optional[str] = attr.ib(default=None)
    calc_mode: CalcMode = attr.ib(default=CalcMode.direct, converter=CalcMode.normalize)  # type: ignore  # 2024-01-24 # TODO: Unsupported converter, only named functions, types and lambdas are currently supported  [misc]
    aggregation: AggregationFunction = attr.ib(
        default=AggregationFunction.none, converter=AggregationFunction.normalize  # type: ignore  # 2024-01-24 # TODO: Unsupported converter, only named functions, types and lambdas are currently supported  [misc]
    )
    type: FieldType = attr.ib(default=FieldType.DIMENSION, converter=FieldType.normalize)  # type: ignore  # 2024-01-24 # TODO: Unsupported converter, only named functions, types and lambdas are currently supported  [misc]
    source: Optional[str] = attr.ib(default=None)
    hidden: bool = attr.ib(default=False)
    description: str = attr.ib(default="")
    formula: str = attr.ib(default="")
    initial_data_type: Optional[UserDataType] = attr.ib(default=None, converter=UserDataType.normalize)  # type: ignore  # 2024-01-24 # TODO: Unsupported converter, only named functions, types and lambdas are currently supported  [misc]
    cast: Optional[UserDataType] = attr.ib(default=None, converter=UserDataType.normalize)  # type: ignore  # 2024-01-24 # TODO: Unsupported converter, only named functions, types and lambdas are currently supported  [misc]
    data_type: Optional[UserDataType] = attr.ib(default=None, converter=UserDataType.normalize)  # type: ignore  # 2024-01-24 # TODO: Unsupported converter, only named functions, types and lambdas are currently supported  [misc]
    valid: bool = attr.ib(default=True)
    has_auto_aggregation: bool = attr.ib(default=False)
    lock_aggregation: bool = attr.ib(default=False)
    avatar_id: Optional[str] = attr.ib(default=None)
    managed_by: ManagedBy = attr.ib(default=ManagedBy.user, converter=ManagedBy.normalize)  # type: ignore  # 2024-01-24 # TODO: Unsupported converter, only named functions, types and lambdas are currently supported  [misc]
    default_value: Optional[ParameterValue] = attr.ib(default=None)
    value_constraint: Optional[BaseParameterValueConstraint] = attr.ib(default=None)
    template_enabled: Optional[bool] = attr.ib(default=None)
    ui_settings: str = attr.ib(default="")

    def set_name(self, name: str) -> None:
        if self.title is None:
            self.title = name

    def prepare(self) -> None:
        super().prepare()
        if self.source and not self.formula and self.default_value is None:
            self.calc_mode = CalcMode.direct
            self.formula = ""
            self.default_value = None
            self.value_constraint = None
            self.template_enabled = None
        elif self.formula and not self.source and self.default_value is None:
            self.calc_mode = CalcMode.formula
            self.source = ""
            self.default_value = None
            self.value_constraint = None
            self.template_enabled = None
        elif self.default_value is not None and not self.source and not self.formula:
            self.calc_mode = CalcMode.parameter
            self.source = ""
            self.formula = ""

        if self.title is None:
            if self.calc_mode == CalcMode.direct:
                self.title = self.source
            else:
                self.title = f"Formula {self.id}"

        self.avatar_id = self.avatar_id if self.calc_mode == CalcMode.direct else None

    def filter(self, op: Union[str, WhereClauseOperation], values: list) -> WhereClause:
        if isinstance(op, str):
            op = WhereClauseOperation[op]
        return WhereClause(column=self.id if self.id is not None else self.title, operation=op, values=values)

    @property
    def asc(self) -> OrderedField:
        return OrderedField(field_id=self.id, direction=OrderDirection.asc)

    @property
    def desc(self) -> OrderedField:
        return OrderedField(field_id=self.id, direction=OrderDirection.desc)

    def parameter_value(self, value: Any = None) -> ParameterFieldValue:
        return ParameterFieldValue(field_id=self.id, value=value if value is not None else self.default_value.value)  # type: ignore  # 2024-01-24 # TODO: Item "None" of "ParameterValue[Any] | None" has no attribute "value"  [union-attr]

    def as_req_legend_item(
        self,
        role: FieldRole = FieldRole.row,
        range_type: Optional[RangeType] = None,
        dimension_values: Optional[dict[int, Any]] = None,
        tree_prefix: Optional[list] = None,
        tree_level: Optional[int] = None,
        legend_item_id: Optional[int] = None,
        block_id: Optional[int] = None,
    ) -> RequestLegendItem:
        role_spec: RoleSpec  # TODO: Move role_spec creation to a separate func
        if role == FieldRole.range:
            assert range_type is not None
            role_spec = RangeRoleSpec(
                role=role,
                range_type=range_type,
            )
        elif role == FieldRole.tree:
            assert dimension_values is not None
            assert tree_prefix is not None
            if tree_level is None:
                tree_level = len(tree_prefix) + 1
            assert tree_level is not None
            role_spec = TreeRoleSpec(
                role=role,
                level=tree_level,
                prefix=json.dumps(tree_prefix),
                dimension_values=[
                    DimensionValueSpec(legend_item_id=dim_liid, value=dim_value)
                    for dim_liid, dim_value in dimension_values.items()
                ],
            )
        elif role == FieldRole.row:
            role_spec = RowRoleSpec(role=role)
        else:
            role_spec = RoleSpec(role=role)
        assert isinstance(self, ResultField)
        return RequestLegendItem(
            ref=IdRequestLegendItemRef(
                type=QueryItemRefType.id,
                id=self.id,
            ),
            role_spec=role_spec,
            legend_item_id=legend_item_id,
            block_id=block_id,
        )


@attr.s(frozen=True)
class WhereClause:
    operation: WhereClauseOperation = attr.ib(kw_only=True)
    column: str = attr.ib(kw_only=True)
    values: list = attr.ib(kw_only=True)
    block_id: Optional[int] = attr.ib(kw_only=True, default=None)
    ref: RequestLegendItemRef = attr.ib(kw_only=True)

    @ref.default
    def _make_ref(self) -> RequestLegendItemRef:
        return IdRequestLegendItemRef(id=self.column)

    def for_block(self, block_id: int) -> WhereClause:
        return attr.evolve(self, block_id=block_id)


@attr.s
class ObligatoryFilter(ApiProxyObject):
    field_guid: str = attr.ib(default="")
    default_filters: list[WhereClause] = attr.ib(factory=list)
    managed_by: ManagedBy = attr.ib(default=ManagedBy.user)
    valid: bool = attr.ib(default=True)


class ResultField(ConditionMakerMixin, _ResultField):  # type: ignore  # TODO: fix
    def get_cpart_from_self(self) -> JoinPart:
        return ResultFieldJoinPart(field_id=self.id)


@attr.s
class RoleSpec:  # noqa
    role: FieldRole = attr.ib(kw_only=True)


@attr.s
class DimensionRoleSpec(RoleSpec):
    visibility: FieldVisibility = attr.ib(kw_only=True, default=FieldVisibility.visible)


@attr.s
class RowRoleSpec(DimensionRoleSpec):
    pass


@attr.s
class TemplateRoleSpec(DimensionRoleSpec):  # noqa
    template: str = attr.ib(kw_only=True)


@attr.s
class TreeRoleSpec(DimensionRoleSpec):
    level: int = attr.ib(kw_only=True)
    prefix: str = attr.ib(kw_only=True)
    dimension_values: list[DimensionValueSpec] = attr.ib(kw_only=True, factory=list)


@attr.s
class RangeRoleSpec(RoleSpec):
    range_type: RangeType = attr.ib(kw_only=True)


@attr.s
class OrderByRoleSpec(RoleSpec):
    direction: OrderDirection = attr.ib(kw_only=True)


@attr.s
class LegendItemBase:  # noqa
    role_spec: RoleSpec = attr.ib(kw_only=True)
    legend_item_id: Optional[int] = attr.ib(kw_only=True, default=None)
    block_id: Optional[int] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class RequestLegendItemRef:
    type: QueryItemRefType = attr.ib(kw_only=True, default=QueryItemRefType.id)


@attr.s
class IdRequestLegendItemRef(RequestLegendItemRef):
    # Set defaults
    type: QueryItemRefType = attr.ib(kw_only=True, default=QueryItemRefType.id)
    id: str = attr.ib(kw_only=True)


@attr.s
class MeasureNameRequestLegendItemRef(RequestLegendItemRef):
    # Set defaults
    type: QueryItemRefType = attr.ib(kw_only=True, default=QueryItemRefType.measure_name)


@attr.s
class PlaceholderRequestLegendItemRef(RequestLegendItemRef):
    # Set defaults
    type: QueryItemRefType = attr.ib(kw_only=True, default=QueryItemRefType.placeholder)


@attr.s
class RequestLegendItem(LegendItemBase):
    ref: RequestLegendItemRef = attr.ib(kw_only=True)


@attr.s
class LegendItem(LegendItemBase):  # noqa
    legend_item_id: int = attr.ib(kw_only=True)  # redefine as strictly not None
    id: str = attr.ib(kw_only=True)
    title: str = attr.ib(kw_only=True)
    data_type: UserDataType = attr.ib(kw_only=True)
    field_type: FieldType = attr.ib(kw_only=True)
    item_type: LegendItemType = attr.ib(kw_only=True)


@attr.s(frozen=True)
class BlockPlacement:
    type: ClassVar[QueryBlockPlacementType]


@attr.s(frozen=True)
class RootBlockPlacement(BlockPlacement):
    type = QueryBlockPlacementType.root


@attr.s(frozen=True)
class DimensionValueSpec:
    legend_item_id: int = attr.ib(kw_only=True)
    value: Any = attr.ib(kw_only=True)


@attr.s(frozen=True)
class AfterBlockPlacement(BlockPlacement):
    type = QueryBlockPlacementType.after

    dimension_values: Optional[list[DimensionValueSpec]] = attr.ib(default=None)


@attr.s
class BlockSpec:
    block_id: int = attr.ib(kw_only=True)
    parent_block_id: Optional[int] = attr.ib(kw_only=True, default=None)
    placement: BlockPlacement = attr.ib(kw_only=True)


@attr.s(frozen=True)
class PivotPagination:
    offset_rows: Optional[int] = attr.ib(kw_only=True, default=None)
    limit_rows: Optional[int] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class PivotTotalItem:
    level: int = attr.ib(kw_only=True)


@attr.s(frozen=True)
class PivotTotals:
    rows: list[PivotTotalItem] = attr.ib(kw_only=True, factory=list)
    columns: list[PivotTotalItem] = attr.ib(kw_only=True, factory=list)

    @staticmethod
    def item(level: int) -> PivotTotalItem:
        return PivotTotalItem(level=level)


@attr.s
class PivotRoleSpec:
    role: PivotRole = attr.ib(kw_only=True)


@attr.s
class DimensionPivotRoleSpec(PivotRoleSpec):
    direction: OrderDirection = attr.ib(kw_only=True)


@attr.s
class AnnotationPivotRoleSpec(PivotRoleSpec):
    annotation_type: str = attr.ib(kw_only=True)
    target_legend_item_ids: Optional[list[int]] = attr.ib(kw_only=True)


@attr.s
class PivotMeasureRoleSpec(PivotRoleSpec):
    sorting: Optional[PivotMeasureSorting] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class PivotItemBase:
    legend_item_ids: list[int] = attr.ib(kw_only=True)
    role_spec: PivotRoleSpec = attr.ib(kw_only=True)


@attr.s(frozen=True)
class RequestPivotItem(PivotItemBase):
    title: Optional[str] = attr.ib(kw_only=True, default=None)


@attr.s(frozen=True)
class PivotItem(PivotItemBase):
    pivot_item_id: int = attr.ib(kw_only=True)
    title: str = attr.ib(kw_only=True)


@attr.s
class PivotHeaderRoleSpec:
    role: PivotHeaderRole = attr.ib(kw_only=True, default=PivotHeaderRole.data)


@attr.s(frozen=True)
class PivotHeaderValue:
    value: str = attr.ib(kw_only=True)


@attr.s(slots=True)
class PivotHeaderInfo:
    sorting_direction: Optional[OrderDirection] = attr.ib(kw_only=True, default=None)
    role_spec: PivotHeaderRoleSpec = attr.ib(kw_only=True, factory=PivotHeaderRoleSpec)


@attr.s(frozen=True)
class PivotMeasureSortingSettings:
    header_values: list[PivotHeaderValue] = attr.ib(kw_only=True)
    direction: OrderDirection = attr.ib(kw_only=True, default=OrderDirection.asc)
    role_spec: PivotHeaderRoleSpec = attr.ib(kw_only=True, factory=PivotHeaderRoleSpec)


@attr.s(frozen=True)
class PivotMeasureSorting:
    column: Optional[PivotMeasureSortingSettings] = attr.ib(kw_only=True, default=None)
    row: Optional[PivotMeasureSortingSettings] = attr.ib(kw_only=True, default=None)


@attr.s
class OrderedField:
    field_id: str = attr.ib(kw_only=True)
    direction: OrderDirection = attr.ib(kw_only=True)
    block_id: Optional[int] = attr.ib(kw_only=True, default=None)

    def for_block(self, block_id: int) -> OrderedField:
        return attr.evolve(self, block_id=block_id)


@attr.s
class ParameterFieldValue:
    field_id: str = attr.ib(kw_only=True)
    value: Any = attr.ib(kw_only=True)
    block_id: Optional[int] = attr.ib(kw_only=True, default=None)


@attr.s
class ComponentError:
    level: ComponentErrorLevel = attr.ib()
    message: str = attr.ib()
    code: str = attr.ib(default="")
    details: dict = attr.ib(factory=dict)


@attr.s
class ComponentErrorPack:
    id: str = attr.ib()
    type: ComponentType = attr.ib()
    errors: list[ComponentError] = attr.ib(factory=list)


@attr.s
class ComponentErrorRegistry:
    items: list[ComponentErrorPack] = attr.ib(factory=list)

    def get_pack(self, id: str) -> Optional[ComponentErrorPack]:  # type: ignore  # TODO: fix
        for item in self.items:
            if item.id == id:
                return item


@attr.s
class ResultSchemaAux:
    inter_dependencies: dict = attr.ib(factory=dict)  # No need to go into details


@attr.s
class Dataset(ApiProxyObject):
    name: str = attr.ib(default=None)
    revision_id: Optional[str] = attr.ib(default=None)
    load_preview_by_default: Optional[bool] = attr.ib(default=True)
    template_enabled: bool = attr.ib(default=False)
    data_export_forbidden: Optional[bool] = attr.ib(default=False)
    sources: Container[DataSource] = attr.ib(factory=Container, converter=Container)
    source_avatars: Container[SourceAvatar] = attr.ib(factory=Container, converter=Container)
    avatar_relations: Container[AvatarRelation] = attr.ib(factory=Container, converter=Container)
    result_schema: Container[ResultField] = attr.ib(factory=Container, converter=Container)
    result_schema_aux: ResultSchemaAux = attr.ib(factory=ResultSchemaAux)
    rls: dict = attr.ib(factory=dict)
    rls2: dict[str, list[RLSEntry]] = attr.ib(factory=dict)
    component_errors: ComponentErrorRegistry = attr.ib(factory=ComponentErrorRegistry)
    obligatory_filters: list[ObligatoryFilter] = attr.ib(default=attr.Factory(list))

    def prepare(self) -> None:
        super().prepare()
        if self.name is None:
            self.name = f"My Dataset {str(uuid.uuid4())}"

    @staticmethod
    def source(**kwargs: Any) -> DataSource:
        return DataSource(**kwargs)

    @staticmethod
    def col(name: str, **kwargs: Any) -> Column:
        if "title" not in kwargs:
            kwargs["title"] = name
        return Column(name=name, **kwargs)

    def field(self, avatar: SourceAvatar = None, **kwargs) -> "ResultField":  # type: ignore  # TODO: fix
        avatar_id = avatar.id if avatar is not None else kwargs.pop("avatar_id", None)
        return ResultField(avatar_id=avatar_id, **kwargs)

    def find_field(self, title: Optional[str] = None, id: Optional[str] = None) -> ResultField:
        for field in self.result_schema:
            if id is not None and field.id == id:
                return field
            if title is not None and field.title == title:
                return field
        raise KeyError("Field not found")

    def measure_name_as_req_legend_item(
        self,
        role: FieldRole = FieldRole.row,
        legend_item_id: Optional[int] = None,
        block_id: Optional[int] = None,
    ) -> RequestLegendItem:
        assert role in (FieldRole.row, FieldRole.info)
        if role == FieldRole.row:
            role_spec = RowRoleSpec(role=role)
        else:
            role_spec = RoleSpec(role=role)  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "RoleSpec", variable has type "RowRoleSpec")  [assignment]
        return RequestLegendItem(
            ref=MeasureNameRequestLegendItemRef(),
            role_spec=role_spec,
            legend_item_id=legend_item_id,
            block_id=block_id,
        )

    def placeholder_as_req_legend_item(
        self,
        role: FieldRole = FieldRole.row,
        template: Optional[str] = None,
        legend_item_id: Optional[int] = None,
        block_id: Optional[int] = None,
    ) -> RequestLegendItem:
        role_spec: RoleSpec
        if role == FieldRole.template:
            assert template is not None
            role_spec = TemplateRoleSpec(
                role=FieldRole.template,
                template=template,
            )
        elif role == FieldRole.row:
            role_spec = RowRoleSpec(role=role)
        else:
            role_spec = RoleSpec(role=role)
        return RequestLegendItem(
            ref=PlaceholderRequestLegendItemRef(),
            role_spec=role_spec,
            legend_item_id=legend_item_id,
            block_id=block_id,
        )

    def make_req_pivot_item(
        self,
        legend_item_ids: list[int],
        role: PivotRole = PivotRole.pivot_row,
        title: Optional[str] = None,
        annotation_type: Optional[str] = None,
        target_legend_item_ids: Optional[list[int]] = None,
        direction: OrderDirection = OrderDirection.asc,
        measure_sorting_settings: Optional[PivotMeasureSorting] = None,
    ) -> RequestPivotItem:
        role_spec = _make_pivot_role_spec(
            role=role,
            annotation_type=annotation_type,
            target_legend_item_ids=target_legend_item_ids,
            direction=direction,
            measure_sorting_settings=measure_sorting_settings,
        )
        return RequestPivotItem(
            role_spec=role_spec,
            legend_item_ids=legend_item_ids,
            title=title,
        )
