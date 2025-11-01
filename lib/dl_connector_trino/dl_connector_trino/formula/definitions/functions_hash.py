import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_hash as base

from dl_connector_trino.formula.constants import TrinoDialect as D


V = TranslationVariant.make


DEFINITIONS_HASH = [
    base.MD5(
        variants=[
            V(
                D.TRINO,
                lambda value: sa.func.to_hex(sa.func.md5(sa.func.to_utf8(value))),
            ),
        ]
    ),
    base.SHA1(
        variants=[
            V(
                D.TRINO,
                lambda value: sa.func.to_hex(sa.func.sha1(sa.func.to_utf8(value))),
            ),
        ]
    ),
    base.SHA256(
        variants=[
            V(
                D.TRINO,
                lambda value: sa.func.to_hex(sa.func.sha256(sa.func.to_utf8(value))),
            ),
        ]
    ),
]
