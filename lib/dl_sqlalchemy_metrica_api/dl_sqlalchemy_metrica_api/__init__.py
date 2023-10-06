from __future__ import annotations

from dl_sqlalchemy_metrica_api import base


base.dialect = dialect = base.MetrikaApiDialect


__all__ = ("dialect",)
