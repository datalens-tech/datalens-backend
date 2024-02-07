from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    ClassVar,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.types import TypeEngine

from dl_formula.core import exc
from dl_formula.core.datatype import (
    DataType,
    DataTypeParams,
)
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    Function,
    SingleVariantTranslationBase,
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.flags import ContextFlag
from dl_formula.definitions.literals import (
    literal,
    un_literal,
)
from dl_formula.definitions.scope import Scope
from dl_formula.definitions.type_strategy import (
    Fixed,
    FromArgs,
    ParamsCustom,
    ParamsEmpty,
)
from dl_formula.shortcuts import n
from dl_formula.utils.datetime import make_datetimetz_value


if TYPE_CHECKING:
    from dl_formula.core.dialect import DialectCombo
    from dl_formula.translation.context import TranslationCtx
    from dl_formula.translation.env import TranslationEnvironment


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class TypeConvFunction(Function):
    pass


class FuncDate(TypeConvFunction):
    name = "date"
    arg_names = ["expression", "timezone"]
    return_type = Fixed(DataType.DATE)


class FuncDate1(FuncDate):
    arg_cnt = 1


class FuncDate1FromNull(FuncDate1):
    variants = [
        V(D.DUMMY, lambda _: sa.cast(sa.null(), sa.Date())),
    ]
    argument_types = [
        ArgTypeSequence([DataType.NULL]),
    ]


class FuncDate1FromDatetime(FuncDate1):
    variants = [
        VW(D.DUMMY, lambda expr: sa.cast(expr.expression, sa.Date())),
    ]
    argument_types = [
        ArgTypeSequence([{DataType.DATE, DataType.DATETIME, DataType.GENERICDATETIME}]),
    ]


class FuncDate1FromDatetimeTZ(FuncDate1):
    # CLICKHOUSE & POSTGRESQL
    argument_types = [
        ArgTypeSequence([DataType.DATETIMETZ]),
    ]


class FuncDate1FromString(FuncDate1):
    variants = [
        # TODO: sqlite3: sa.func.date
        V(D.DUMMY, lambda expr: sa.cast(expr, sa.Date())),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]


class FuncDate1FromNumber(FuncDate1):
    # Custom implementation for each dialect
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
        ArgTypeSequence([DataType.INTEGER]),
    ]


class FuncDate2(FuncDate):
    arg_cnt = 2


class FuncDatetimeImpl(TypeConvFunction):
    # WARNING: the `{arg:1}` in this description requires there to be at least
    # one `arg_cnt = 2` registered subclass translation, such as
    # `FuncDatetime2FromCHStuff`.
    arg_names = ["expression", "timezone"]


class FuncTypeGenericDatetimeImpl(FuncDatetimeImpl):
    return_type = Fixed(DataType.GENERICDATETIME)


class FuncTypeGenericDatetime1Impl(FuncTypeGenericDatetimeImpl):
    arg_cnt = 1


class FuncTypeGenericDatetime1FromNullImpl(FuncTypeGenericDatetime1Impl):
    variants = [
        V(D.DUMMY, lambda _: sa.cast(sa.null(), sa.DateTime())),
    ]
    argument_types = [
        ArgTypeSequence([DataType.NULL]),
    ]


class FuncTypeGenericDatetime1FromDatetimeImpl(FuncTypeGenericDatetime1Impl):
    variants = [V(D.DUMMY | D.SQLITE, lambda expr: expr)]
    argument_types = [
        ArgTypeSequence([{DataType.DATETIME, DataType.GENERICDATETIME}]),
    ]


class FuncTypeGenericDatetime1FromDateImpl(FuncTypeGenericDatetime1Impl):
    variants = [
        V(D.DUMMY, lambda expr: sa.cast(expr, sa.DateTime())),
    ]
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
    ]


class FuncTypeGenericDatetime1FromNumberImpl(FuncTypeGenericDatetime1Impl):
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
        ArgTypeSequence([DataType.INTEGER]),
    ]


class FuncTypeGenericDatetime1FromStringImpl(FuncTypeGenericDatetime1Impl):
    variants = [
        V(D.DUMMY, lambda expr: sa.cast(expr, sa.DateTime())),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]


class FuncTypeGenericDatetime2Impl(FuncTypeGenericDatetimeImpl):
    arg_cnt = 2


class FuncDatetime1FromNull(FuncTypeGenericDatetime1FromNullImpl):
    name = "datetime"


class FuncDatetime1FromDatetime(FuncTypeGenericDatetime1FromDatetimeImpl):
    name = "datetime"


class FuncDatetime1FromDate(FuncTypeGenericDatetime1FromDateImpl):
    name = "datetime"


class FuncDatetime1FromNumber(FuncTypeGenericDatetime1FromNumberImpl):
    name = "datetime"


class FuncDatetime1FromString(FuncTypeGenericDatetime1FromStringImpl):
    name = "datetime"


class FuncGenericDatetime1FromNull(FuncTypeGenericDatetime1FromNullImpl):
    name = "genericdatetime"
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class FuncGenericDatetime1FromDatetime(FuncTypeGenericDatetime1FromDatetimeImpl):
    name = "genericdatetime"
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class FuncGenericDatetime1FromDate(FuncTypeGenericDatetime1FromDateImpl):
    name = "genericdatetime"
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class FuncGenericDatetime1FromNumber(FuncTypeGenericDatetime1FromNumberImpl):
    name = "genericdatetime"
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class FuncGenericDatetime1FromString(FuncTypeGenericDatetime1FromStringImpl):
    name = "genericdatetime"
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class FuncDatetimeTZ(TypeConvFunction):
    name = "datetimetz"

    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED

    arg_cnt = 2
    arg_names = ["expression", "timezone"]

    return_type = Fixed(DataType.DATETIMETZ)
    return_type_params = ParamsCustom(
        lambda args: DataTypeParams(timezone=un_literal(args[1].expression))  # type: ignore  # TODO: fix
    )


class FuncDatetimeTZConst(SingleVariantTranslationBase, FuncDatetimeTZ):
    dialects = D.DUMMY | D.SQLITE
    argument_types = [
        ArgTypeSequence(
            [
                {
                    DataType.CONST_DATETIME,
                    DataType.CONST_GENERICDATETIME,
                    DataType.CONST_DATETIMETZ,
                    # Dubious: `DataType.CONST_DATE`.
                    DataType.CONST_INTEGER,
                    DataType.CONST_FLOAT,
                    DataType.CONST_STRING,
                },
                DataType.CONST_STRING,
            ]
        ),
    ]

    @classmethod
    def _translate_main(cls, value_ctx, tz_ctx, *, _env: TranslationEnvironment):  # type: ignore  # TODO: fix
        value = un_literal(value_ctx.expression, value_ctx=value_ctx)
        # TODO?: re-check the `type(value)` by `value_ctx.data_type`?

        # TODO: error-handling
        tzname = un_literal(tz_ctx.expression)
        # TODO: error-handling
        try:
            dt = make_datetimetz_value(value, tzname)
        except ValueError as err:
            raise exc.ParseValueError(str(err)) from err
        return literal(dt, d=_env.dialect)


class FuncDatetimeTZToNaive(SingleVariantTranslationBase, TypeConvFunction):
    name = "datetimetz_to_naive"  # TODO: consider having `datetime(datetimetz_value)` for this.
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED
    arg_cnt = 1
    arg_names = ["datetimetz"]
    argument_types = [
        ArgTypeSequence([DataType.DATETIMETZ, DataType.CONST_STRING]),
    ]

    return_type = Fixed(DataType.DATETIME)
    return_type_params = ParamsEmpty()  # this is default, but making it explicit


class FuncFloat(TypeConvFunction):
    name = "float"
    arg_names = ["expression"]
    arg_cnt = 1
    return_type = Fixed(DataType.FLOAT)


class FuncFloatNumber(FuncFloat):
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
    ]


class FuncFloatString(FuncFloat):
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]


class FuncFloatFromBool(FuncFloat):
    argument_types = [
        ArgTypeSequence([DataType.BOOLEAN]),
    ]


class FuncFloatFromDate(FuncFloat):
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
    ]


class FuncFloatFromDatetime(FuncFloat):
    argument_types = [
        ArgTypeSequence([DataType.DATETIME]),
    ]


class FuncFloatFromGenericDatetime(FuncFloat):
    argument_types = [
        ArgTypeSequence([DataType.GENERICDATETIME]),
    ]


class FuncInt(TypeConvFunction):
    name = "int"
    arg_names = ["expression"]
    arg_cnt = 1
    return_type = Fixed(DataType.INTEGER)


class FuncIntFromNull(FuncInt):
    argument_types = [
        ArgTypeSequence([DataType.NULL]),
    ]


class FuncIntFromInt(FuncInt):
    variants = [V(D.DUMMY | D.SQLITE, lambda value: value)]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
    ]


class FuncIntFromFloat(FuncInt):
    variants = [
        # FIXME: use something like `NUMBER(*, 0)` for all Oracle `sa.Integer` cases.
        V(D.SQLITE, lambda value: sa.cast(value, sa.Integer)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]


class FuncIntFromBool(FuncInt):
    argument_types = [
        ArgTypeSequence([DataType.BOOLEAN]),
    ]


class FuncIntFromStr(FuncInt):
    variants = [
        V(D.SQLITE, lambda value: sa.cast(value, sa.Integer)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]


class FuncIntFromDate(FuncInt):
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
    ]


class FuncIntFromDatetime(FuncInt):
    argument_types = [
        ArgTypeSequence([DataType.DATETIME]),
    ]


class FuncIntFromGenericDatetime(FuncInt):
    argument_types = [
        ArgTypeSequence([DataType.GENERICDATETIME]),
    ]


class FuncIntFromDatetimeTZ(FuncInt):
    argument_types = [
        ArgTypeSequence([DataType.DATETIMETZ]),
    ]


class FuncBool(TypeConvFunction):
    name = "bool"
    arg_names = ["expression"]
    arg_cnt = 1
    return_type = Fixed(DataType.BOOLEAN)


class FuncBoolFromNull(FuncBool):
    variants = [
        V(D.DUMMY, lambda _: sa.cast(sa.null(), sa.Boolean())),
    ]
    argument_types = [ArgTypeSequence([DataType.NULL])]


class FuncBoolFromNumber(FuncBool):
    variants = [
        V(D.DUMMY, lambda value: value != 0),
    ]
    argument_types = [ArgTypeSequence([DataType.FLOAT])]
    return_flags = ContextFlag.IS_CONDITION


class FuncBoolFromBool(FuncBool):
    variants = [V(D.DUMMY | D.SQLITE, lambda value: value)]
    argument_types = [
        ArgTypeSequence([DataType.BOOLEAN]),
    ]


class FuncBoolFromStrGeo(FuncBool):
    variants = [
        V(D.DUMMY, lambda value: value != ""),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
        ArgTypeSequence([DataType.GEOPOINT]),
        ArgTypeSequence([DataType.GEOPOLYGON]),
    ]
    return_flags = ContextFlag.IS_CONDITION


class FuncBoolFromDateDatetime(FuncBool):
    variants = [
        VW(D.DUMMY | D.SQLITE, lambda value: n.func.IF(n.func.ISNULL(value), None, True)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
        ArgTypeSequence([DataType.DATETIME]),
        ArgTypeSequence([DataType.GENERICDATETIME]),
    ]


class FuncStr(TypeConvFunction):
    name = "str"
    arg_names = ["expression"]
    arg_cnt = 1
    return_type = Fixed(DataType.STRING)


class FuncStrFromNull(FuncStr):
    argument_types = [
        ArgTypeSequence([DataType.NULL]),
    ]


class FuncStrFromUnsupported(FuncStr):
    variants = [
        V(D.SQLITE, lambda value: sa.cast(value, sa.TEXT)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.UNSUPPORTED]),
    ]


class FuncStrFromInteger(FuncStr):
    variants = [
        V(D.SQLITE, lambda value: sa.cast(value, sa.TEXT)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
    ]


class FuncStrFromFloat(FuncStr):
    variants = [
        V(D.SQLITE, lambda value: sa.cast(value, sa.TEXT)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]


class FuncStrFromBool(FuncStr):
    argument_types = [
        ArgTypeSequence([DataType.BOOLEAN]),
    ]


class FuncStrFromStrGeo(FuncStr):
    variants = [V(D.DUMMY | D.SQLITE, lambda value: value)]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
        ArgTypeSequence([DataType.GEOPOINT]),
        ArgTypeSequence([DataType.GEOPOLYGON]),
    ]


# Should *not* be done: FuncStrFromMarkup


class FuncStrFromDate(FuncStr):
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
    ]


class FuncStrFromDatetime(FuncStr):
    argument_types = [
        ArgTypeSequence([{DataType.DATETIME, DataType.GENERICDATETIME}]),
    ]


class FuncStrFromDatetimeTZ(FuncStr):
    argument_types = [
        ArgTypeSequence([DataType.DATETIMETZ]),
    ]


class FuncStrFromString(FuncStr):
    variants = [V(D.DUMMY | D.SQLITE, lambda value: value)]
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
    ]


class FuncStrFromUUID(FuncStr):
    argument_types = [
        ArgTypeSequence([DataType.UUID]),
    ]


class FuncStrFromArray(FuncStr):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_FLOAT]),
        ArgTypeSequence([DataType.ARRAY_INT]),
        ArgTypeSequence([DataType.ARRAY_STR]),
    ]


class FuncDatetimeParseImpl(TypeConvFunction):
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]


class FuncTypeGenericDatetimeParseImpl(FuncDatetimeParseImpl):
    return_type = Fixed(DataType.GENERICDATETIME)


class FuncDatetimeParse(FuncTypeGenericDatetimeParseImpl):
    name = "datetime_parse"


class FuncGenericDatetimeParse(FuncTypeGenericDatetimeParseImpl):
    name = "genericdatetime_parse"
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class FuncDateParse(TypeConvFunction):
    name = "date_parse"
    arg_cnt = 1
    # CLICKHOUSE
    argument_types = [ArgTypeSequence([DataType.STRING])]
    return_type = Fixed(DataType.DATE)


class FuncGeopoint(TypeConvFunction):
    name = "geopoint"
    arg_names = ["value_1", "value_2"]
    return_type = Fixed(DataType.GEOPOINT)


class FuncGeopointFromStr(FuncGeopoint):
    arg_cnt = 1
    variants = [V(D.DUMMY | D.SQLITE, lambda val: val)]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
        ArgTypeSequence([DataType.GEOPOINT]),
    ]


class FuncGeopointFromCoords(FuncGeopoint):
    arg_cnt = 2
    variants = [
        VW(D.DUMMY | D.SQLITE, lambda lat, lon: n.func.CONCAT("[", lat, ",", lon, "]")),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]


class FuncGeopolygonBase(TypeConvFunction):
    name = "geopolygon"
    return_type = Fixed(DataType.GEOPOLYGON)
    arg_cnt = 1


class FuncGeopolygon(FuncGeopolygonBase):
    variants = [V(D.DUMMY | D.SQLITE, lambda val: val)]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
        ArgTypeSequence([DataType.GEOPOLYGON]),
    ]


class FuncTreeBase(TypeConvFunction):
    arg_cnt = 1
    name = "tree"
    arg_names = ["array"]
    variants = [
        V(D.DUMMY, lambda val: val),
    ]


class FuncTreeStr(FuncTreeBase):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_STR]),
    ]
    return_type = Fixed(DataType.TREE_STR)


class DbCastArgTypes(ArgTypeSequence):
    __slots__ = ()

    def __init__(self):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        super().__init__(
            arg_types=[
                # 1. expression
                {
                    DataType.INTEGER,
                    DataType.FLOAT,
                    DataType.STRING,
                    DataType.DATE,
                    DataType.ARRAY_INT,
                    DataType.ARRAY_FLOAT,
                    DataType.ARRAY_STR,
                    # Expand as other types become supported
                },
                # 2. native_type
                DataType.CONST_STRING,
                # Followed by custom type params, but they are all handled in the translation function,
                # not passed down to the arg matcher superclass.
            ]
        )

    def match_arg_types(self, arg_types: Sequence[DataType]) -> bool:
        # Check the first two arguments using superclass
        # The rest can have any types and will be validated by the translation
        return super().match_arg_types(arg_types[: len(self._exp_arg_types)])

    def get_possible_arg_types_at_pos(self, pos: int, total: int) -> Set[DataType]:
        if pos < len(self._exp_arg_types):
            return super().get_possible_arg_types_at_pos(pos=pos, total=total)
        return set(DataType)


DataTypeSpec = Union[DataType, Tuple[DataType, ...]]


class WhitelistTypeSpec(NamedTuple):
    name: str
    sa_type: Type[TypeEngine]
    nested_sa_type: Optional[Type[TypeEngine]] = None
    arg_types: Tuple[DataTypeSpec, ...] = ()


DECIMAL_CAST_ARG_T = (DataType.CONST_INTEGER, DataType.CONST_INTEGER)
CHAR_CAST_ARG_T = (DataType.CONST_INTEGER,)


class FuncDbCastBase(TypeConvFunction):
    name = "db_cast"
    arg_names = ["expression", "native_type", "param_1", "param_2", "param_3"]
    argument_types = [
        DbCastArgTypes(),
    ]
    return_type = FromArgs(0)

    WHITELISTS: ClassVar[dict[DialectCombo, dict[DataType, list[WhitelistTypeSpec]]]] = {}

    @classmethod
    def generate_cast_type(
        cls, dialect: DialectCombo, wr_name: TranslationCtx, value: TranslationCtx, type_args: Sequence[TranslationCtx]
    ) -> TypeEngine:
        whitelist_by_type = cls.WHITELISTS[dialect]
        name = un_literal(wr_name.expression)  # type: ignore  # TODO: fix
        assert isinstance(name, str)
        name = name.upper()
        data_type = value.data_type.non_const_type  # type: ignore  # TODO: fix

        try:
            whitelist = whitelist_by_type[data_type]
        except KeyError as err:
            raise exc.TranslationError(
                f"Data type {data_type} is not supported by function {cls.name.upper()}"
            ) from err

        # Find the correct spec by data type name
        for each_spec in whitelist:
            if each_spec.name.upper() == name:
                type_spec = each_spec
                break
        else:
            raise exc.TranslationError(f"Native data type {name} is not supported by function {cls.name.upper()}")

        if len(type_args) != len(type_spec.arg_types):
            raise exc.TranslationError(
                f"Native type cast to {name} requires {type_spec.arg_types}, got {len(type_args)}"
            )

        def _raise_type_mismath(_arg_num: int):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
            dtype = type_args[_arg_num].data_type
            assert dtype is not None
            raise exc.TranslationError(
                f"Argument {_arg_num+1} of native type cast to {name} " f"has invalid type {dtype.name}"
            )

        unwrapped_native_type_args = []
        for arg_num, (arg, req_arg_type) in enumerate(zip(type_args, type_spec.arg_types, strict=True)):
            if isinstance(req_arg_type, DataType):
                if not arg.data_type.casts_to(req_arg_type):  # type: ignore  # TODO: fix
                    _raise_type_mismath(arg_num)
            elif isinstance(req_arg_type, tuple):
                for sub_req_arg_type in req_arg_type:
                    if not arg.data_type.casts_to(sub_req_arg_type):  # type: ignore  # TODO: fix
                        _raise_type_mismath(arg_num)
            else:
                raise TypeError(type(req_arg_type))

            unwrapped_native_type_args.append(un_literal(arg.expression))  # type: ignore  # TODO: fix

        # All validation checks are done
        # Create the type and make the cast
        if type_spec.nested_sa_type is not None:
            cast_to_type = type_spec.sa_type(type_spec.nested_sa_type(*unwrapped_native_type_args))  # type: ignore  # 2024-01-24 # TODO: Too many arguments for "TypeEngine"  [call-arg]
        else:
            cast_to_type = type_spec.sa_type(*unwrapped_native_type_args)

        return cast_to_type

    @classmethod
    def apply_cast_wrapper(
        cls,
        dialect: DialectCombo,
        expression: ClauseElement,
        type_: TypeEngine,
    ) -> ClauseElement:
        return sa.cast(expression, type_)

    @classmethod
    def perform_cast(
        cls, dialect: DialectCombo, wr_name: TranslationCtx, value: TranslationCtx, type_args: Sequence[TranslationCtx]
    ) -> ClauseElement:
        type_ = cls.generate_cast_type(dialect=dialect, wr_name=wr_name, value=value, type_args=type_args)
        assert value.expression is not None
        return cls.apply_cast_wrapper(dialect=dialect, expression=value.expression, type_=type_)

    def __init__(self):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
        super().__init__()

        self._inst_variants = [
            VW(
                dialects=dialect,
                translation=(
                    lambda value, wr_name, *args, _dialect=dialect: self.perform_cast(
                        dialect=_dialect,
                        wr_name=wr_name,
                        value=value,
                        type_args=args,
                    )
                ),
            )
            for dialect in self.WHITELISTS.keys()
        ]


class FuncDbCast2(FuncDbCastBase):
    arg_cnt = 2


class FuncDbCast3(FuncDbCastBase):
    arg_cnt = 3


class FuncDbCast4(FuncDbCastBase):
    arg_cnt = 4


DEFINITIONS_TYPE = [
    # bool
    FuncBoolFromNull,
    FuncBoolFromNumber,
    FuncBoolFromBool,
    FuncBoolFromStrGeo,
    FuncBoolFromDateDatetime,
    # date
    FuncDate1FromNull,
    FuncDate1FromDatetime,
    FuncDate1FromDatetimeTZ,
    FuncDate1FromString,
    FuncDate1FromNumber,
    # date_parse
    FuncDateParse,  # only CLICKHOUSE
    # datetime
    FuncDatetime1FromNull,
    FuncDatetime1FromDatetime,
    FuncDatetime1FromDate,
    FuncDatetime1FromNumber,
    FuncDatetime1FromString,
    # datetime_parse
    FuncDatetimeParse,  # only CLICKHOUSE
    # datetimetz
    FuncDatetimeTZConst,
    # datetimetz_to_naive
    # -> only for some dialects
    # db_cast
    FuncDbCast2,
    FuncDbCast3,
    FuncDbCast4,
    # float
    FuncFloatNumber,
    FuncFloatString,
    FuncFloatFromBool,
    FuncFloatFromDate,
    FuncFloatFromDatetime,
    FuncFloatFromGenericDatetime,
    # genericdatetime
    FuncGenericDatetime1FromNull,
    FuncGenericDatetime1FromDatetime,
    FuncGenericDatetime1FromDate,
    FuncGenericDatetime1FromNumber,
    FuncGenericDatetime1FromString,
    # genericdatetime_parse
    FuncGenericDatetimeParse,  # only CLICKHOUSE
    # geopoint
    FuncGeopointFromStr,
    FuncGeopointFromCoords,
    # geopolygon
    FuncGeopolygon,
    # int
    FuncIntFromNull,
    FuncIntFromInt,
    FuncIntFromFloat,
    FuncIntFromBool,
    FuncIntFromStr,
    FuncIntFromDate,
    FuncIntFromDatetime,
    FuncIntFromGenericDatetime,
    FuncIntFromDatetimeTZ,
    # str
    FuncStrFromNull,
    FuncStrFromUnsupported,
    FuncStrFromInteger,
    FuncStrFromFloat,
    FuncStrFromBool,
    FuncStrFromStrGeo,
    FuncStrFromDate,
    FuncStrFromDatetime,
    FuncStrFromDatetimeTZ,
    FuncStrFromString,
    FuncStrFromUUID,
    FuncStrFromArray,
    # tree
    FuncTreeStr,
]
