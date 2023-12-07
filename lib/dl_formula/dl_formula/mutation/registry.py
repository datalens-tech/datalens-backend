from dl_formula.mutation.lookup import LOOKUP_MUTATOR_REGISTRY


def get_mutation_lookup_functions_names() -> list[str]:
    return list(LOOKUP_MUTATOR_REGISTRY.keys())
