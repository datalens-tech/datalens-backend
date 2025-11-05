import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_hash as base
from dl_formula.shortcuts import n

from dl_connector_ydb.formula.constants import YqlDialect as D


V = TranslationVariant.make


DEFINITIONS_HASH = [
    base.MD5(
        variants=[
            V(
                D.YQL,
                lambda value: n.func.upper(sa.func.Digest.Md5Hex(value)),
            ),
        ]
    ),
    base.SHA1(
        variants=[
            V(
                D.YQL,
                lambda value: n.func.upper(sa.func.String.HexEncode(sa.func.Digest.Sha1(value))),
            ),
        ]
    ),
    base.SHA256(
        variants=[
            V(
                D.YQL,
                lambda value: n.func.upper(sa.func.String.HexEncode(sa.func.Digest.Sha256(value))),
            ),
        ]
    ),
    base.MurmurHash2_64(
        variants=[
            V(
                D.YQL,
                lambda value: sa.func.Digest.MurMurHash(value),
            ),
        ]
    ),
    base.SipHash64(
        variants=[
            V(
                D.YQL,
                lambda value: sa.func.Digest.SipHash(0, 0, value),
            ),
        ]
    ),
    base.IntHash64(
        variants=[
            V(
                D.YQL,
                lambda value: sa.func.Digest.IntHash64(value),
            ),
        ]
    ),
    base.CityHash64(
        variants=[
            V(
                D.YQL,
                lambda value: sa.func.Digest.CityHash(value),
            ),
        ]
    ),
]
