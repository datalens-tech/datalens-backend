import abc
import enum
from typing import (
    ClassVar,
    Generic,
    Optional,
    TypeVar,
)

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.enums import ExtAPIType


_VISITOR_RETURN_DATA_TV = TypeVar("_VISITOR_RETURN_DATA_TV")


class ExtFieldVisitor(Generic[_VISITOR_RETURN_DATA_TV], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def visit_formula_field(self, dsrc: "FormulaCS") -> _VISITOR_RETURN_DATA_TV:
        raise NotImplementedError()

    @abc.abstractmethod
    def visit_direct_field(self, dsrc: "DirectCS") -> _VISITOR_RETURN_DATA_TV:
        raise NotImplementedError()


class FieldKind(enum.Enum):
    direct = enum.auto()
    formula = enum.auto()
    id_formula = enum.auto()
    parameter = enum.auto()


@enum.unique
class FieldType(enum.Enum):
    string = enum.auto()
    integer = enum.auto()
    float = enum.auto()
    date = enum.auto()
    datetime = enum.auto()
    boolean = enum.auto()
    geopoint = enum.auto()
    geopolygon = enum.auto()
    uuid = enum.auto()
    markup = enum.auto()
    datetimetz = enum.auto()
    unsupported = enum.auto()
    array_str = enum.auto()
    array_int = enum.auto()
    array_float = enum.auto()
    tree_str = enum.auto()
    genericdatetime = enum.auto()


class Aggregation(enum.Enum):
    none = "none"
    sum = "sum"
    avg = "avg"
    min = "min"
    max = "max"
    count = "count"
    countunique = "countunique"


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True, kw_only=True)
class CalcSpec:
    kind: ClassVar[FieldKind]


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class DatasetField:
    id: str = attr.ib()
    title: str = attr.ib()
    description: Optional[str] = attr.ib()
    hidden: bool = attr.ib(default=False)
    cast: FieldType = attr.ib()
    aggregation: Optional[Aggregation] = attr.ib(default=None)
    calc_spec: CalcSpec = attr.ib()

    @property
    def strict_aggregation(self) -> Aggregation:
        agg = self.aggregation
        assert agg is not None
        return agg


@ModelDescriptor(api_types_exclude=[ExtAPIType.DC])
@attr.s(kw_only=True)
class FormulaCS(CalcSpec):
    kind = FieldKind.formula

    formula: str = attr.ib()


@ModelDescriptor()
@attr.s(kw_only=True)
class IDFormulaCS(CalcSpec):
    kind = FieldKind.id_formula

    formula: str = attr.ib()


@ModelDescriptor()
@attr.s
class DirectCS(CalcSpec):
    kind = FieldKind.direct

    field_name: str = attr.ib()
    avatar_id: Optional[str] = attr.ib(default=None)  # None is accepted only in there is single avatar in dataset

    @property
    def strict_avatar_id(self) -> str:
        avatar_id = self.avatar_id
        assert avatar_id is not None
        return avatar_id


@ModelDescriptor()
@attr.s()
class ParameterCS(CalcSpec):
    kind = FieldKind.parameter

    # In current version of external API, only scalar types of parameters will be supported.
    # Type of parameter is declared on field level.
    # So to simplify API, we will not wrap default parameter with object with type discriminator.
    # If complex types of parameter default will be needed - it is a reason to completely rework literals design.
    default_value: str = attr.ib()
