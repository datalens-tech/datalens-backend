# TODO: Move to dl_core.components.* (separate into 2 files: field and result_schema)

from __future__ import annotations

import logging
import re
from typing import (
    Any,
    ClassVar,
    Collection,
    Dict,
    List,
    NamedTuple,
    Optional,
    Type,
    TypeVar,
    Union,
)

import attr

from dl_constants.enums import (
    AggregationFunction,
    CalcMode,
    FieldType,
    ManagedBy,
    ParameterValueConstraintType,
    TopLevelComponentId,
    UserDataType,
)
from dl_core.components.ids import FieldId
from dl_core.exc import FieldNotFound
from dl_core.values import BIValue


LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class ParameterValueConstraint:
    type: ParameterValueConstraintType = attr.ib(default=ParameterValueConstraintType.all)

    def is_valid(self, value: Any) -> bool:
        if isinstance(value, BIValue):
            value = value.value
        try:
            return self._is_valid(value)
        except TypeError:
            return False

    def _is_valid(self, value: Any) -> bool:
        return True


@attr.s(frozen=True)
class RangeParameterValueConstraint(ParameterValueConstraint):
    min: Optional[BIValue] = attr.ib(default=None)
    max: Optional[BIValue] = attr.ib(default=None)
    type: ParameterValueConstraintType = attr.ib(default=ParameterValueConstraintType.range)

    def _is_valid(self, value: Any) -> bool:
        return (self.min is None or self.min.value <= value) and (self.max is None or value <= self.max.value)


@attr.s(frozen=True)
class SetParameterValueConstraint(ParameterValueConstraint):
    values: List[BIValue] = attr.ib(factory=list)
    type: ParameterValueConstraintType = attr.ib(default=ParameterValueConstraintType.set)

    def _is_valid(self, value: Any) -> bool:
        return any([v.value == value for v in self.values])


_CALC_SPEC_TV = TypeVar("_CALC_SPEC_TV", bound="CalculationSpec")


@attr.s(frozen=True)
class CalculationSpec:
    """Defines how the field's expression is constructed"""

    mode: ClassVar[CalcMode]  # Different enum value for each subclass

    def clone(self: _CALC_SPEC_TV, **kwargs: Any) -> _CALC_SPEC_TV:
        return attr.evolve(self, **kwargs)

    def depends_on(self, field: BIField) -> bool:
        return False


@attr.s(frozen=True)
class DirectCalculationSpec(CalculationSpec):
    """
    Describes direct fields.
    Such fields are created from data source (db table) columns
    and inherit some of their properties.
    """

    mode = CalcMode.direct

    # ID of the avatar (table) this field refers to
    avatar_id: Optional[str] = attr.ib(kw_only=True, default=None)
    # Name of the table column this field refers to
    # (name attribute of a raw_Schema column).
    # Use empty string (`''`) for non-direct fields.
    source: str = attr.ib(kw_only=True, default="")


@attr.s(frozen=True)
class FormulaCalculationSpec(CalculationSpec):
    """
    Field is defined by a formula.
    """

    mode = CalcMode.formula

    # The formula itself. Parsed and handled mostly by the dl_formula package.
    # In this formula other fields are referred to exclusively by title,
    # Use empty string (`''`) for non-formula fields.
    formula: str = attr.ib(kw_only=True, default="")

    # In this formula other fields are referred to exclusively by guid,
    # Use empty string (`''`) for non-formula fields.
    guid_formula: str = attr.ib(kw_only=True, default="")

    def depends_on(self, field: BIField) -> bool:
        return f"[{field.title}]" in self.formula or f"[{field.guid}]" in self.guid_formula


@attr.s(frozen=True)
class ParameterCalculationSpec(CalculationSpec):
    """
    Field is a parameter.
    The parameter value is specified in data API.
    After value substitution, the field acts as a formula consisting of a single literal.
    """

    mode = CalcMode.parameter

    # Default value of the parameter.
    default_value: Optional[BIValue] = attr.ib(kw_only=True, default=None)
    # Value constraint of the parameter
    # (defines the restrictions and origins of possible values).
    value_constraint: Optional[ParameterValueConstraint] = attr.ib(kw_only=True, default=None)


_CALCULATION_SPECS_BY_MODE = {
    CalcMode.direct.name: DirectCalculationSpec,
    CalcMode.formula.name: FormulaCalculationSpec,
    CalcMode.parameter.name: ParameterCalculationSpec,
}


def filter_calc_spec_kwargs(mode: str, kwargs: Dict[str, Any]) -> Dict[str, Any]:
    spec_cls = _CALCULATION_SPECS_BY_MODE[mode]
    field_keys = {f.name for f in attr.fields(spec_cls)}
    return {key: value for key, value in kwargs.items() if key in field_keys}


def create_calc_spec_from(kwargs: Dict[str, Any]) -> CalculationSpec:
    mode = kwargs["calc_mode"]
    spec_cls = _CALCULATION_SPECS_BY_MODE[mode.name]
    return spec_cls(**filter_calc_spec_kwargs(mode.name, kwargs))


def del_calc_spec_kwargs_from(
    kwargs: Dict[str, Any], spec_cls: Optional[Type[CalculationSpec]] = None
) -> Dict[str, Any]:
    spec_classes = _CALCULATION_SPECS_BY_MODE.values() if spec_cls is None else [spec_cls]
    keys = [f.name for s in spec_classes for f in attr.fields(s)] + ["calc_mode"]
    return {key: value for key, value in kwargs.items() if key not in keys}


class BIField(NamedTuple):  # TODO: Convert to attr.s
    """Dataset field, contained in dataset's result_schema"""

    # The ID of the field. The main means of identifying
    # or referring to the field.
    guid: str

    # A human-readable name of the field (used in UI and formulas).
    title: str

    # Describes how the field was created
    # (directly by the user or various parts of the backend)
    # and is basically a primitive implementation of access control.
    managed_by: ManagedBy

    # Flag indicates whether this field is always shown in UI or not.
    hidden: bool

    # A plain-text human-readable description of the field.
    # Not used for any internal purposes.
    description: str

    # Flag indicates whether the field contains any errors.
    valid: bool

    # The earliest identifiable data type of the field
    # in the expression construction pipeline.
    # Works a bit differently depending on the calc mode:
    # - direct: it corresponds to the user_type of the referenced raw_schema column;
    # - formula: automatically derived from the formula;
    # - parameter: defined by the user.
    initial_data_type: Optional[UserDataType]

    # redefines the data type in `initial_data_type`, is set by the user.
    # For parameter calc_mode, it is the same as `initial_data_type`.
    cast: Optional[UserDataType]

    # An explicitly set aggregation (via UI).
    # Value "none" corresponds to no aggregation.
    # Is not the final data type (see `data_type`)!
    aggregation: AggregationFunction

    # The data type automatically determined after the aggregation is applied
    data_type: Optional[UserDataType]

    # Flag indicates that the field is automatically aggregated
    # and an explicit aggregation (`aggregation` attribute) is not applicable.
    # This can be in one of the following cases:
    # - field is a formula that contains an aggregation
    # - field is in direct mode and references a column
    #   that is pre-aggregated in the data source
    has_auto_aggregation: bool

    # Flag that enables or disables the explicit aggregation in UI
    # for direct fields if their (pre)aggregation is controlled by the data source.
    # FIXME: Looks like this should be part of `DirectCalculationSpec`,
    #   but its usage is complicated and a bit unclear,
    #   so it requires some research and (probably) preliminary fixes
    lock_aggregation: bool

    # Type of field in terms of measure/dimension.
    # If the field contains an aggregation
    # (aggregation attribute, formula, or direct field with has_auto_aggregation),
    # then it is a measure. In all other cases it id a dimension.
    type: FieldType

    # Specified
    calc_spec: CalculationSpec

    @classmethod
    def make(
        cls,
        guid: FieldId,
        title: Optional[str],
        calc_spec: CalculationSpec,
        aggregation: Union[AggregationFunction, str, None] = None,
        type: Union[FieldType, str, None] = None,
        hidden: bool = False,
        description: str = "",
        cast: Union[UserDataType, str, None] = None,
        initial_data_type: Union[UserDataType, str, None] = None,
        data_type: Union[UserDataType, str, None] = None,
        valid: Optional[bool] = None,
        has_auto_aggregation: bool = False,
        lock_aggregation: bool = False,
        managed_by: Optional[ManagedBy] = None,
    ) -> BIField:
        """
        Normalize attributes and create a BIField object.
        """

        if isinstance(calc_spec, DirectCalculationSpec):
            title = title or calc_spec.source or ""
            calc_spec = calc_spec.clone(source=calc_spec.source or title or "")
        assert title is not None

        aggregation = AggregationFunction.normalize(aggregation)
        if aggregation is None:
            aggregation = AggregationFunction.none
        assert aggregation is not None

        type = FieldType.normalize(type)
        if type is None:
            type = FieldType.DIMENSION
        assert type is not None

        cast = UserDataType.normalize(cast)
        initial_data_type = UserDataType.normalize(initial_data_type)
        data_type = UserDataType.normalize(data_type)
        valid = valid if valid is not None else True
        managed_by = ManagedBy.normalize(managed_by) or ManagedBy.user

        return cls(
            title=title,
            guid=guid,
            aggregation=aggregation,
            hidden=hidden,
            description=description,
            type=type,
            cast=cast,
            initial_data_type=initial_data_type,
            data_type=data_type,
            valid=valid,
            has_auto_aggregation=has_auto_aggregation,
            lock_aggregation=lock_aggregation,
            managed_by=managed_by,
            calc_spec=calc_spec,
        )

    @property
    def calc_mode(self) -> CalcMode:
        return self.calc_spec.mode

    @property
    def avatar_id(self) -> Optional[str]:
        assert isinstance(self.calc_spec, DirectCalculationSpec)
        return self.calc_spec.avatar_id

    @property
    def source(self) -> str:
        assert isinstance(self.calc_spec, DirectCalculationSpec)
        return self.calc_spec.source

    @property
    def formula(self) -> str:
        assert isinstance(self.calc_spec, FormulaCalculationSpec)
        return self.calc_spec.formula

    @property
    def guid_formula(self) -> str:
        assert isinstance(self.calc_spec, FormulaCalculationSpec)
        return self.calc_spec.guid_formula

    @property
    def default_value(self) -> Optional[BIValue]:
        assert isinstance(self.calc_spec, ParameterCalculationSpec)
        return self.calc_spec.default_value

    @property
    def value_constraint(self) -> Optional[ParameterValueConstraint]:
        assert isinstance(self.calc_spec, ParameterCalculationSpec)
        return self.calc_spec.value_constraint

    @property
    def autoaggregated(self) -> bool:
        return (
            self.has_auto_aggregation
            # TODO: Remove this second condition after dataset migration #11 is applied
            or self.type == FieldType.MEASURE
            and self.aggregation == AggregationFunction.none
        )

    @property
    def aggregation_locked(self) -> bool:
        return (
            # TODO: Remove the first condition after aggregation handling is enabled for formula fields in bi-api
            self.calc_mode == CalcMode.formula
            or self.calc_mode == CalcMode.parameter
            or self.has_auto_aggregation
            or self.lock_aggregation
        )

    @staticmethod
    def rename_in_formula(formula: str, key_map: Dict[str, str]) -> str:
        found_keys = FIELD_RE.findall(formula)

        for key in found_keys:
            try:
                value = key_map[key]
            except KeyError:
                LOGGER.warning("Unknown field: %s", key)
                continue

            formula = formula.replace("[{}]".format(key), "[{}]".format(value))

        return formula

    def depends_on(self, field: BIField) -> bool:
        return self.calc_spec.depends_on(field)

    def clone(self, **kwargs: Any) -> BIField:
        if "calc_spec" not in kwargs:
            new_calc_spec = None
            mode = kwargs.get("calc_mode", self.calc_mode)
            if mode != self.calc_mode:
                new_calc_spec = create_calc_spec_from(kwargs)
            else:
                calc_spec_kwargs = filter_calc_spec_kwargs(mode.name, kwargs)
                if calc_spec_kwargs:
                    new_calc_spec = self.calc_spec.clone(**calc_spec_kwargs)
            kwargs = del_calc_spec_kwargs_from(kwargs)
            if new_calc_spec is not None:
                kwargs["calc_spec"] = new_calc_spec

        return self._replace(**kwargs)


FIELD_RE = re.compile(r"\[([^]]*)\]")


@attr.s
class ResultSchema:
    fields: List[BIField] = attr.ib(default=attr.Factory(list))
    _guid_cache: Dict[FieldId, BIField] = attr.ib(init=False, eq=False)
    _title_cache: Dict[str, BIField] = attr.ib(init=False, eq=False)
    valid: bool = attr.ib(default=True)

    def __attrs_post_init__(self):  # type: ignore  # TODO: fix
        self._guid_cache = {}
        self._title_cache = {}
        self.reload_caches()

    @property
    def id(self) -> str:
        return TopLevelComponentId.result_schema.value

    def clear_caches(self) -> None:
        self._guid_cache.clear()
        self._title_cache.clear()

    def reload_caches(self) -> None:
        self.clear_caches()
        for field in self.fields:
            self._guid_cache[field.guid] = field
            self._title_cache[field.title] = field

    def by_guid(self, guid: FieldId) -> BIField:
        """Find field by guid"""
        if not self._guid_cache:
            self.reload_caches()
        try:
            return self._guid_cache[guid]
        except KeyError:
            raise FieldNotFound(f"Unknown field {guid}")

    def by_title(self, title: str) -> BIField:
        """Find field by title"""
        if not self._title_cache:
            self.reload_caches()
        try:
            return self._title_cache[title]
        except KeyError:
            raise FieldNotFound(f"Unknown field {title}")

    @property
    def titles_to_guids(self) -> Dict[str, FieldId]:
        return {f.title: f.guid for f in self.fields}

    @property
    def guids_to_titles(self) -> Dict[FieldId, str]:
        return {f.guid: f.title for f in self.fields}

    def __iter__(self):  # type: ignore  # TODO: fix
        return self.fields.__iter__()

    def __bool__(self):  # type: ignore  # TODO: fix
        return self.fields.__iter__()

    def __len__(self):  # type: ignore  # TODO: fix
        return self.fields.__len__()

    def __getitem__(self, ind):  # type: ignore  # TODO: fix
        return self.fields.__getitem__(ind)

    def add(self, field: BIField, idx: Optional[int]) -> None:
        if idx is not None:
            self.fields.insert(idx, field)
        else:
            self.fields.append(field)
        self.clear_caches()

    def remove(self, field_id: FieldId) -> None:
        self.fields.remove(self.by_guid(field_id))
        self.clear_caches()

    def update_field(self, idx: int, field: BIField) -> None:
        self.fields[idx] = field
        self.clear_caches()

    def index(self, field_id: FieldId) -> int:
        return next((idx for idx, f in enumerate(self.fields) if f.guid == field_id))

    def remove_multiple(self, field_ids: Collection[str]) -> None:
        fields_to_remove = [self.by_guid(field_id) for field_id in field_ids]
        for field in fields_to_remove:
            self.fields.remove(field)
        self.clear_caches()
