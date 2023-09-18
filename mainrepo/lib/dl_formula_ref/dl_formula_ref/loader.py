from typing import (
    Collection,
    Optional,
)

import attr

from dl_formula.loader import load_bi_formula
from dl_formula_ref.formula_ref_plugins import register_all_plugins


@attr.s(frozen=True)
class FormulaRefLibraryConfig:
    plugin_ep_names: Optional[Collection[str]] = attr.ib(kw_only=True, default=None)


def load_formula_ref(formula_ref_lib_config: FormulaRefLibraryConfig = FormulaRefLibraryConfig()) -> None:
    load_bi_formula()
    register_all_plugins(plugin_ep_names=formula_ref_lib_config.plugin_ep_names)
