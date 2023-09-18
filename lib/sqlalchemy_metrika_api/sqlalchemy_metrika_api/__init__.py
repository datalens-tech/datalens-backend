from __future__ import annotations

from sqlalchemy_metrika_api import base

base.dialect = dialect = base.MetrikaApiDialect


__all__ = ("dialect",)
