from typing import Collection, Optional

import attr

from bi_formula.definitions.ll_registry_loader import (
    populate_translation_registry, populate_ll_op_registry,
)
from bi_formula.formula_connectors import load_all_connectors, register_all_connectors


@attr.s(frozen=True)
class FormulaLibraryConfig:
    # Whitelist of connector entrypoints to be loaded
    formula_connector_ep_names: Optional[Collection[str]] = attr.ib(kw_only=True, default=None)


def preload_bi_formula() -> None:
    """ Loads all entrypoint connectors without registering them """

    load_all_connectors()


def load_bi_formula(formula_lib_config: FormulaLibraryConfig = FormulaLibraryConfig()) -> None:
    """Initialize the library"""
    register_all_connectors(connector_ep_names=formula_lib_config.formula_connector_ep_names)
    populate_translation_registry()
    populate_ll_op_registry()
