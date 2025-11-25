import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_hash as base

from dl_connector_oracle.formula.constants import OracleDialect as D


V = TranslationVariant.make


DEFINITIONS_HASH = [
    base.MD5(
        variants=[
            V(
                D.ORACLE,
                lambda value: sa.func.RAWTOHEX(sa.func.STANDARD_HASH(value, sa.literal("MD5"))),
            ),
        ]
    ),
    base.SHA1(
        variants=[
            V(
                D.ORACLE,
                lambda value: sa.func.RAWTOHEX(sa.func.STANDARD_HASH(value, sa.literal("SHA1"))),
            ),
        ]
    ),
    base.SHA256(
        variants=[
            V(
                D.ORACLE,
                lambda value: sa.func.RAWTOHEX(sa.func.STANDARD_HASH(value, sa.literal("SHA256"))),
            ),
        ]
    ),
]
