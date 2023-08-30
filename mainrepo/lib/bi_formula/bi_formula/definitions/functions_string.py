from __future__ import annotations

import sqlalchemy as sa

from bi_formula.core.dialect import StandardDialect as D
from bi_formula.core.datatype import DataType
from bi_formula.shortcuts import n
from bi_formula.definitions.args import ArgTypeForAll, ArgTypeSequence
from bi_formula.definitions.flags import ContextFlag
from bi_formula.definitions.type_strategy import Fixed
from bi_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
    Function,
)
from bi_formula.definitions.common import make_binary_chain
from bi_formula.definitions.literals import literal


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class StringFunction(Function):
    pass


class FuncAscii(StringFunction):
    name = 'ascii'
    arg_names = ['string']
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.ASCII),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]
    return_type = Fixed(DataType.INTEGER)


class FuncChar(StringFunction):
    name = 'char'
    arg_names = ['string']
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.CHAR),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
    ]
    return_type = Fixed(DataType.STRING)


class ConcatBase(StringFunction):
    name = 'concat'
    return_type = Fixed(DataType.STRING)


class ConcatMulti(ConcatBase):
    arg_cnt = None


_CONCAT_TYPES = {
    DataType.STRING,
    DataType.INTEGER,
    DataType.FLOAT,
    DataType.BOOLEAN,
    DataType.DATE,
    DataType.DATETIME,
    DataType.DATETIMETZ,
    DataType.GENERICDATETIME,
    DataType.GEOPOINT,
    DataType.GEOPOLYGON,
    DataType.UUID,
    # Should not be implemented: MARKUP:
    # markup should not be castable to string,
    # and, for consistency, `CONCAT` should always return a string.
}


class ConcatMultiStrConst(ConcatMulti):
    variants = [
        V(
            D.DUMMY | D.SQLITE | D.GSHEETS | D.BITRIX,
            lambda *args, _env: literal(''.join(arg.value for arg in args), d=_env.dialect)
        ),
    ]
    argument_types = [
        ArgTypeForAll(DataType.CONST_STRING),
    ]
    return_type = Fixed(DataType.CONST_STRING)


class Concat1(ConcatBase):
    arg_cnt = 1
    variants = [
        VW(D.DUMMY | D.SQLITE, n.func.STR),
    ]
    argument_types = [
        ArgTypeSequence([_CONCAT_TYPES]),
    ]


class ConcatMultiStr(ConcatMulti):
    """Simplified version that can only concatenate strings"""
    # TODO: join adjacent const strings, remove single-argument concat.
    variants = [
        V(D.DUMMY, lambda *args: sa.func.CONCAT(*args)),
    ]
    argument_types = [
        ArgTypeForAll(DataType.STRING),
    ]


class ConcatMultiAny(ConcatMulti):
    """Concatenation of any-type arguments"""
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda *args: make_binary_chain(
                # convert all to string and then concatenate
                (lambda x, y: n.func.CONCAT(n.func.STR(x), n.func.STR(y))),
                *args
            ),
        ),
    ]
    argument_types = [
        ArgTypeForAll(_CONCAT_TYPES),
    ]


class FuncContains(StringFunction):
    name = 'contains'
    arg_names = ['string', 'substring']
    arg_cnt = 2
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION


def quote_like(value, escape='\\', chars=('%', '_')):
    for char in (escape,) + tuple(chars):
        value = value.replace(char, escape + char)
    return value


def quote_like_metrica(value):
    value = value.replace('\\', r'\\')
    value = value.replace('*', r'\*')
    return value


NON_STR_CONTAINMENT_TYPES = {
    DataType.BOOLEAN,
    DataType.INTEGER,
    DataType.FLOAT,
    DataType.DATE,
    DataType.DATETIME,
    DataType.DATETIMETZ,
    DataType.GENERICDATETIME,
    DataType.UUID,
    DataType.GEOPOINT,
    DataType.GEOPOLYGON,
}


class FuncContainsConst(FuncContains):
    variants = [
        V(D.DUMMY,
          lambda x, y: x.like(
              '%{}%'.format(quote_like(y.value)),
              escape='\\')),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.CONST_STRING]),
    ]


class FuncContainsNonConst(FuncContains):
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]


class FuncContainsNonString(FuncContains):
    variants = [
        VW(D.DUMMY, lambda x, y: n.func.CONTAINS(n.func.STR(x), y)),
    ]
    argument_types = [
        ArgTypeSequence([NON_STR_CONTAINMENT_TYPES, DataType.STRING]),
    ]


class FuncIContains(StringFunction):
    name = 'icontains'
    arg_names = ['string', 'substring']
    arg_cnt = 2
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION


class FuncIContainsConst(FuncIContains):
    variants = [
        V(D.DUMMY,
          lambda x, y: x.ilike(
              # WARNING: even with a proper collation, the python's
              # `val.lower()` might not exactly match postgresql's `lower(val)`
              '%{}%'.format(quote_like(y.value.lower())),
              escape='\\')),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.CONST_STRING]),
    ]


class FuncIContainsNonConst(FuncIContains):
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda s, substr: n.func.CONTAINS(n.func.LOWER(s), n.func.LOWER(substr)),
        ),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]
    return_flags = ContextFlag.IS_CONDITION


class FuncIContainsNonString(FuncIContains):
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda x, y: n.func.ICONTAINS(n.func.STR(x), y)
        ),
    ]
    argument_types = [
        ArgTypeSequence([NON_STR_CONTAINMENT_TYPES, DataType.STRING]),
    ]


class FuncEndswith(StringFunction):
    name = 'endswith'
    arg_names = ['string', 'substring']
    arg_cnt = 2
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION


class FuncEndswithConst(FuncEndswith):
    variants = [
        V(D.DUMMY,
          lambda x, y: x.like(
              '%{}'.format(quote_like(y.value)),
              escape='\\')),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.CONST_STRING]),
    ]


class FuncEndswithNonConst(FuncEndswith):
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]


class FuncEndswithNonString(FuncEndswith):
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda x, y: n.func.ENDSWITH(n.func.STR(x), y)
        ),
    ]
    argument_types = [
        ArgTypeSequence([NON_STR_CONTAINMENT_TYPES, DataType.STRING]),
    ]


class FuncIEndswith(StringFunction):
    name = 'iendswith'
    arg_names = ['string', 'substring']
    arg_cnt = 2
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION


class FuncIEndswithConst(FuncIEndswith):
    variants = [
        V(D.DUMMY,
          lambda x, y: x.ilike(
              '%{}'.format(quote_like(y.value.lower())),
              escape='\\')),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.CONST_STRING]),
    ]


class FuncIEndswithNonConst(FuncIEndswith):
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda s, substr: n.func.ENDSWITH(n.func.LOWER(s), n.func.LOWER(substr)),
        ),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]


class FuncIEndswithNonString(FuncIEndswith):
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda x, y: n.func.IENDSWITH(n.func.STR(x), y)
        ),
    ]
    argument_types = [
        ArgTypeSequence([NON_STR_CONTAINMENT_TYPES, DataType.STRING]),
    ]


class FuncStartswith(StringFunction):
    name = 'startswith'
    arg_names = ['string', 'substring']
    arg_cnt = 2
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION


class FuncStartswithConst(FuncStartswith):
    variants = [
        V(D.DUMMY,
          lambda x, y: x.like(
              '{}%'.format(quote_like(y.value)),
              escape='\\')),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.CONST_STRING]),
    ]


class FuncStartswithNonConst(FuncStartswith):
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]


class FuncStartswithNonString(FuncStartswith):
    variants = [
        VW(D.DUMMY, lambda x, y: n.func.STARTSWITH(n.func.STR(x), y)),
    ]
    argument_types = [
        ArgTypeSequence([NON_STR_CONTAINMENT_TYPES, DataType.STRING]),
    ]


class FuncIStartswith(StringFunction):
    name = 'istartswith'
    arg_names = ['string', 'substring']
    arg_cnt = 2
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION


class FuncIStartswithConst(FuncIStartswith):
    variants = [
        V(D.DUMMY,
          lambda x, y: x.ilike(
              '{}%'.format(quote_like(y.value.lower())),
              escape='\\')),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.CONST_STRING]),
    ]


class FuncIStartswithNonConst(FuncIStartswith):
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda s, substr: n.func.STARTSWITH(n.func.LOWER(s), n.func.LOWER(substr)),
        ),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]


class FuncIStartswithNonString(FuncIStartswith):
    variants = [
        VW(
            D.DUMMY | D.SQLITE,
            lambda x, y: n.func.ISTARTSWITH(n.func.STR(x), y)
        ),
    ]
    argument_types = [
        ArgTypeSequence([NON_STR_CONTAINMENT_TYPES, DataType.STRING]),
    ]


class FuncFind(StringFunction):
    name = 'find'
    arg_names = ['string', 'substring', 'start_index']
    return_type = Fixed(DataType.INTEGER)


class FuncFind2(FuncFind):
    arg_cnt = 2
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]


class FuncFind3(FuncFind):
    arg_cnt = 3
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING, DataType.INTEGER]),
    ]


class FuncLeft(StringFunction):
    name = 'left'
    arg_cnt = 2
    arg_names = ['string', 'number']
    variants = [
        V(D.DUMMY, sa.func.LEFT),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.INTEGER]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncRight(StringFunction):
    name = 'right'
    arg_cnt = 2
    arg_names = ['string', 'number']
    variants = [
        V(D.DUMMY, sa.func.RIGHT),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.INTEGER]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncLen(StringFunction):
    name = 'len'
    arg_names = ['value']
    arg_cnt = 1
    return_type = Fixed(DataType.INTEGER)


class FuncLenString(FuncLen):
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]


class FuncLower(StringFunction):
    name = 'lower'
    arg_names = ['string']
    arg_cnt = 1


class FuncLowerConst(FuncLower):
    variants = [
        V(
            D.DUMMY | D.SQLITE,
            lambda s, _env: literal(s.value.lower(), d=_env.dialect)
        ),
    ]
    argument_types = [
        ArgTypeSequence([DataType.CONST_STRING]),
    ]
    return_type = Fixed(DataType.CONST_STRING)


class FuncLowerNonConst(FuncLower):
    variants = [
        V(D.DUMMY, sa.func.LOWER),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncUpper(StringFunction):
    name = 'upper'
    arg_names = ['string']
    arg_cnt = 1


class FuncUpperConst(FuncUpper):
    variants = [
        V(
            D.DUMMY | D.SQLITE,
            lambda s, _env: literal(s.value.upper(), d=_env.dialect)
        ),
    ]
    argument_types = [
        ArgTypeSequence([DataType.CONST_STRING]),
    ]
    return_type = Fixed(DataType.CONST_STRING)


class FuncUpperNonConst(FuncUpper):
    variants = [
        V(D.DUMMY, sa.func.UPPER),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncLtrim(StringFunction):
    name = 'ltrim'
    arg_names = ['string']
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.LTRIM),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncRtrim(StringFunction):
    name = 'rtrim'
    arg_names = ['string']
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.RTRIM),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncTrim(StringFunction):
    name = 'trim'
    arg_names = ['string']
    arg_cnt = 1
    variants = [
        V(D.DUMMY, sa.func.TRIM),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncSubstr(StringFunction):
    name = 'substr'
    arg_names = ['string', 'from_index', 'length']


class FuncSubstr2(FuncSubstr):
    arg_cnt = 2
    variants = [
        V(D.DUMMY, sa.func.SUBSTRING),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.INTEGER]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncSubstr3(FuncSubstr):
    arg_cnt = 3
    variants = [
        V(D.DUMMY, sa.func.SUBSTRING),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.INTEGER, DataType.INTEGER]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncRegexpExtract(StringFunction):
    name = 'regexp_extract'
    arg_cnt = 2
    arg_names = ['string', 'pattern']
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.CONST_STRING]),
    ]
    return_type = Fixed(DataType.STRING)


# In MS SQL Server regular expressions can only be used in user-defined functions

class FuncRegexpExtractNth(StringFunction):
    name = 'regexp_extract_nth'
    arg_cnt = 3
    arg_names = ['string', 'pattern', 'match_index']
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.CONST_STRING, DataType.INTEGER]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncRegexpMatch(StringFunction):
    name = 'regexp_match'
    arg_cnt = 2
    arg_names = ['string', 'pattern']
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION


class FuncRegexpReplace(StringFunction):
    name = 'regexp_replace'
    arg_cnt = 3
    arg_names = ['string', 'pattern', 'replace_with']
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING, DataType.STRING]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncReplace(StringFunction):
    name = 'replace'
    arg_cnt = 3
    arg_names = ['string', 'substring', 'replace_with']
    variants = [
        V(D.DUMMY, sa.func.REPLACE),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING, DataType.STRING]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncSpace(StringFunction):
    name = 'space'
    arg_cnt = 1
    return_type = Fixed(DataType.STRING)


class FuncSpaceConst(FuncSpace):
    variants = [
        V(
            D.DUMMY | D.SQLITE,
            lambda size, _env: literal(' ' * size.value, d=_env.dialect)
        ),
    ]
    argument_types = [
        ArgTypeSequence([DataType.CONST_INTEGER]),
    ]
    return_type = Fixed(DataType.CONST_STRING)


class FuncSpaceNonConst(FuncSpace):
    variants = [
        V(D.DUMMY, sa.func.SPACE),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
    ]


class FuncSplit(StringFunction):
    name = 'split'
    arg_names = ['orig_string', 'delimiter', 'part_index']


class FuncSplit1(FuncSplit):
    arg_cnt = 1
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]
    return_type = Fixed(DataType.ARRAY_STR)


class FuncSplit2(FuncSplit):
    arg_cnt = 2
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.CONST_STRING]),
    ]
    return_type = Fixed(DataType.ARRAY_STR)


class FuncSplit3(FuncSplit):
    arg_cnt = 3
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.CONST_STRING, DataType.INTEGER]),
    ]
    return_type = Fixed(DataType.STRING)


class FuncUtf8(StringFunction):
    name = 'utf8'
    arg_cnt = 2
    arg_names = ['string', 'old_encoding']
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]
    return_type = Fixed(DataType.STRING)


DEFINITIONS_STRING = [
    # ascii
    FuncAscii,

    # char
    FuncChar,

    # concat
    Concat1,
    ConcatMultiStrConst,
    ConcatMultiStr,
    ConcatMultiAny,

    # contains
    FuncContainsConst,
    FuncContainsNonConst,
    FuncContainsNonString,

    # endswith
    FuncEndswithConst,
    FuncEndswithNonConst,
    FuncEndswithNonString,

    # find
    FuncFind2,
    FuncFind3,

    # icontains
    FuncIContainsConst,
    FuncIContainsNonConst,
    FuncIContainsNonString,

    # iendswith
    FuncIEndswithConst,
    FuncIEndswithNonConst,
    FuncIEndswithNonString,

    # istartswith
    FuncIStartswithConst,
    FuncIStartswithNonConst,
    FuncIStartswithNonString,

    # left
    FuncLeft,

    # len
    FuncLenString,

    # lower
    FuncLowerConst,
    FuncLowerNonConst,

    # ltrim
    FuncLtrim,

    # regexp_extract
    FuncRegexpExtract,

    # regexp_extract_nth
    FuncRegexpExtractNth,

    # regexp_match
    FuncRegexpMatch,

    # regexp_replace
    FuncRegexpReplace,

    # replace
    FuncReplace,

    # right
    FuncRight,

    # rtrim
    FuncRtrim,

    # space
    FuncSpaceConst,
    FuncSpaceNonConst,

    # split
    FuncSplit1,
    FuncSplit2,
    FuncSplit3,

    # startswith
    FuncStartswithConst,
    FuncStartswithNonConst,
    FuncStartswithNonString,

    # substr
    FuncSubstr2,
    FuncSubstr3,

    # trim
    FuncTrim,

    # upper
    FuncUpperConst,
    FuncUpperNonConst,

    # utf8
    FuncUtf8,
]
