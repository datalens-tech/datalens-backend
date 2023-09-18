import attr

from dl_formula.core.dialect import DialectCombo


@attr.s(frozen=True)
class GenerationEnvironment:
    scopes: int = attr.ib(kw_only=True)
    supported_dialects: frozenset[DialectCombo] = attr.ib(kw_only=True)
