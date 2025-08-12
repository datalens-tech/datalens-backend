from functools import lru_cache

from dl_formula.connectors.base.context_processor import ContextPostprocessor
from dl_formula.core.dialect import DialectCombo


_CONTEXT_PROCESSORS: dict[DialectCombo, ContextPostprocessor] = {}


@lru_cache
def get_context_processor(dialect: DialectCombo) -> ContextPostprocessor:
    sorted_dialect_items = sorted(
        _CONTEXT_PROCESSORS.items(), key=lambda el: el[0].ambiguity
    )  # Most specific dialects go first, the most ambiguous ones go last
    for d, context_processor in sorted_dialect_items:
        if d & dialect == dialect:
            return context_processor

    return ContextPostprocessor()


def register_context_processor(dialect: DialectCombo, context_processor_cls: type[ContextPostprocessor]) -> None:
    if dialect in _CONTEXT_PROCESSORS:
        assert isinstance(_CONTEXT_PROCESSORS[dialect], context_processor_cls)
        return

    _CONTEXT_PROCESSORS[dialect] = context_processor_cls()
