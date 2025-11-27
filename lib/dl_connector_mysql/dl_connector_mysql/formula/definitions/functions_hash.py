import sqlalchemy as sa

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_hash as base
from dl_formula.shortcuts import n

from dl_connector_mysql.formula.constants import MySQLDialect as D


V = TranslationVariant.make


DEFINITIONS_HASH = [
    base.MD5(
        variants=[
            V(
                D.MYSQL,
                lambda value: n.func.upper(sa.func.md5(value)),
            ),
        ]
    ),
    base.SHA1(
        variants=[
            V(
                D.MYSQL,
                lambda value: n.func.upper(sa.func.sha1(value)),
            ),
        ]
    ),
    base.SHA256(
        variants=[
            V(
                D.MYSQL,
                lambda value: n.func.upper(sa.func.sha2(value, 256)),
            ),
        ]
    ),
]
