from typing import Callable

import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_hash as base

from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D


V = TranslationVariant.make


def _pgcrypto_hash_factory(func_name: str) -> Callable[[ClauseElement], ClauseElement]:
    def _pg_hash_impl(value: ClauseElement) -> ClauseElement:
        return sa.func.upper(sa.func.encode(sa.func.digest(value, func_name), "hex"))

    return _pg_hash_impl


DEFINITIONS_HASH = [
    base.MD5(
        variants=[
            V(
                D.POSTGRESQL,
                lambda value: sa.func.upper(sa.func.md5(value)),
            ),
        ]
    ),
    base.SHA1(
        variants=[
            V(
                D.POSTGRESQL,
                _pgcrypto_hash_factory("sha1"),
            ),
        ]
    ),
    base.SHA256(
        variants=[
            V(
                D.POSTGRESQL,
                _pgcrypto_hash_factory("sha256"),
            ),
        ]
    ),
]
