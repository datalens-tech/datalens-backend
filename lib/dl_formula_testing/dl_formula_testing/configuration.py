from __future__ import annotations

from typing import (
    Collection,
    Optional,
)

import attr

from dl_formula.loader import FormulaLibraryConfig


@attr.s(kw_only=True)
class FormulaTestEnvironmentConfiguration:
    formula_connector_ep_names: Optional[Collection[str]] = attr.ib(default=None)

    def get_formula_library_config(self) -> FormulaLibraryConfig:
        return FormulaLibraryConfig(
            formula_connector_ep_names=self.formula_connector_ep_names,
        )
