import abc
from typing import Callable

import attr
import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from bi_formula.core.dialect import DialectCombo
from bi_formula.translation.sa_dialects import get_sa_dialect


@attr.s
class ColumnRenderer(abc.ABC):
    _dialect: DialectCombo = attr.ib(kw_only=True)
    _field_names: dict[str, tuple[str, ...]] = attr.ib(factory=dict)
    _quoter: Callable[[str], ClauseElement] = attr.ib(init=False)

    @_quoter.default
    def _make_quoter(self) -> Callable[[str], ClauseElement]:
        return get_sa_dialect(self._dialect).identifier_preparer.quote

    @abc.abstractmethod
    def make_column(self, name: str) -> sa.sql.ClauseElement:
        raise NotImplementedError


@attr.s
class DefaultColumnRenderer(ColumnRenderer):
    def make_column(self, name: str) -> sa.sql.ClauseElement:
        full_name_parts = self._field_names.get(name) or (name,)
        return sa.literal_column(
            '.'.join([self._quoter(part) for part in full_name_parts])
        )


@attr.s
class UnprefixedColumnRenderer(ColumnRenderer):
    def make_column(self, name: str) -> sa.sql.ClauseElement:
        full_name_parts = self._field_names.get(name) or (name,)
        return sa.literal_column(self._quoter(full_name_parts[-1]))
