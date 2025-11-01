import sqlalchemy as sa

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    Function,
    TranslationVariant,
)
from dl_formula.definitions.scope import Scope
from dl_formula.definitions.type_strategy import Fixed


V = TranslationVariant.make


class HashFunction(Function):
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class MD5(HashFunction):
    name = "md5"
    arg_names = ["value"]
    arg_cnt = 1
    return_type = Fixed(DataType.STRING)
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]
    variants = [
        V(D.DUMMY, lambda value: sa.func.hex(sa.func.MD5(value))),
    ]


class SHA1(HashFunction):
    name = "sha1"
    arg_names = ["value"]
    arg_cnt = 1
    return_type = Fixed(DataType.STRING)
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]
    variants = [
        V(D.DUMMY, lambda value: sa.func.hex(sa.func.SHA1(value))),
    ]


class SHA256(HashFunction):
    name = "sha256"
    arg_names = ["value"]
    arg_cnt = 1
    return_type = Fixed(DataType.STRING)
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]
    variants = [
        V(D.DUMMY, lambda value: sa.func.hex(sa.func.SHA256(value))),
    ]


class MurmurHash2_64(HashFunction):
    name = "murmurhash2_64"
    arg_names = ["value"]
    arg_cnt = 1
    return_type = Fixed(DataType.INTEGER)
    variants = [
        V(D.DUMMY, sa.func.murmurHash2_64),
    ]


class SipHash64(HashFunction):
    name = "siphash64"
    arg_names = ["value"]
    arg_cnt = 1
    return_type = Fixed(DataType.INTEGER)
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
    ]
    variants = [
        V(D.DUMMY, sa.func.sipHash64),
    ]


class IntHash64(HashFunction):
    name = "inthash64"
    arg_names = ["value"]
    arg_cnt = 1
    return_type = Fixed(DataType.INTEGER)
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
    ]
    variants = [
        V(D.DUMMY, sa.func.intHash64),
    ]


class CityHash64(HashFunction):
    name = "cityhash64"
    arg_names = ["value"]
    arg_cnt = 1
    return_type = Fixed(DataType.INTEGER)
    variants = [
        V(D.DUMMY, sa.func.cityHash64),
    ]


DEFINITIONS_HASH = [
    MD5,
    SHA1,
    SHA256,
    MurmurHash2_64,
    SipHash64,
    IntHash64,
    CityHash64,
]
