
from dl_formula.connectors.base.column import (
    ColumnRenderer,
    DefaultColumnRenderer,
)
from dl_formula.core.dialect import DialectCombo
from dl_formula.core.dialect import StandardDialect as D


_COLUMN_RENDERER_TYPES: dict[DialectCombo, type[ColumnRenderer]] = {
    D.DUMMY: DefaultColumnRenderer,
}


def get_column_renderer(dialect: DialectCombo, field_names: dict[str, tuple[str, ...]]) -> ColumnRenderer:
    sorted_dialect_items = sorted(
        _COLUMN_RENDERER_TYPES.items(), key=lambda el: el[0].ambiguity
    )  # Most specific dialects go first, the most ambiguous ones go last
    for d, context_processor in sorted_dialect_items:
        if d & dialect == dialect:
            return context_processor(dialect=dialect, field_names=field_names)
    raise ValueError(f"Couldn't find a column renderer for dialect {dialect}")


def register_column_renderer_cls(dialect: DialectCombo, column_renderer_cls: type[ColumnRenderer]) -> None:
    if dialect in _COLUMN_RENDERER_TYPES:
        assert _COLUMN_RENDERER_TYPES[dialect] is column_renderer_cls
    else:
        _COLUMN_RENDERER_TYPES[dialect] = column_renderer_cls
