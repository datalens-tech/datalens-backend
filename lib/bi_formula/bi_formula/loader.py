from bi_formula.definitions.ll_registry_loader import (
    populate_translation_registry, populate_ll_op_registry,
)
from bi_formula.formula_connectors import register_all_connectors


def load_bi_formula() -> None:
    """Initialize the library"""
    register_all_connectors()
    populate_translation_registry()
    populate_ll_op_registry()
