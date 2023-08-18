from typing import Optional, Sequence

import attr

from bi_formula.core.dialect import DialectCombo
from bi_formula.definitions.args import ArgTypeMatcher
from bi_formula.definitions.type_strategy import TypeStrategy


@attr.s(frozen=True)
class FunctionImplementationSpec:
    name: str = attr.ib(kw_only=True)
    arg_cnt: Optional[int] = attr.ib(kw_only=True)
    arg_names: Sequence[str] = attr.ib(kw_only=True)
    argument_types: Sequence[ArgTypeMatcher] = attr.ib(kw_only=True)
    return_type: TypeStrategy = attr.ib(kw_only=True)
    scopes: int = attr.ib(kw_only=True)
    dialects: DialectCombo = attr.ib(kw_only=True)
    return_flags: int = attr.ib(kw_only=True)
