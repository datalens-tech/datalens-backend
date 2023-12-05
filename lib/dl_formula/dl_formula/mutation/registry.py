import typing as tp

from dl_api_lib.query.registry import is_forkable_dialect
from dl_formula.core.dialect import DialectCombo
from dl_formula.mutation.lookup import LOOKUP_MUTATOR_REGISTRY


def get_mutation_functions_names(dialect: DialectCombo) -> tp.List[str]:
    if is_forkable_dialect(dialect):
        return list(LOOKUP_MUTATOR_REGISTRY.keys())

    return []
