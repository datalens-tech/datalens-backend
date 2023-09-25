from __future__ import annotations

from collections import defaultdict

from sqlalchemy.engine.default import DefaultDialect

from dl_formula.core.dialect import DialectCombo
from dl_formula.core.dialect import StandardDialect as D


def get_sa_dialect(dialect: DialectCombo) -> DefaultDialect:
    return TO_SA_DIALECTS[dialect]


def register_sa_dialect(dialect: DialectCombo, sa_dialect: DefaultDialect) -> None:
    TO_SA_DIALECTS[dialect] = sa_dialect


TO_SA_DIALECTS: dict[DialectCombo, DefaultDialect] = defaultdict(DefaultDialect)
register_sa_dialect(D.DUMMY, DefaultDialect())
