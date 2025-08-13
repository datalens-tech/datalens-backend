from typing import (
    ClassVar,
    Iterable,
    Optional,
    Sequence,
)

from sqlalchemy.engine.default import DefaultDialect

from dl_formula.connectors.base.column import (
    ColumnRenderer,
    DefaultColumnRenderer,
)
from dl_formula.connectors.base.context_processor import ContextPostprocessor
from dl_formula.connectors.base.literal import Literalizer
from dl_formula.connectors.base.type_constructor import (
    DefaultSATypeConstructor,
    SATypeConstructor,
)
from dl_formula.core.dialect import (
    DialectCombo,
    DialectName,
    DialectNamespace,
    StandardDialect,
)
from dl_formula.definitions.base import NodeTranslation


class FormulaConnector:
    dialect_ns_cls: ClassVar[type[DialectNamespace]] = StandardDialect
    dialects: ClassVar[DialectCombo]
    default_dialect: ClassVar[Optional[DialectCombo]] = None
    op_definitions: ClassVar[Iterable[NodeTranslation]] = []
    literalizer_cls: ClassVar[type[Literalizer]] = Literalizer
    column_renderer_cls: ClassVar[type[ColumnRenderer]] = DefaultColumnRenderer
    context_processor_cls: ClassVar[type[ContextPostprocessor]] = ContextPostprocessor
    type_constructor_cls: ClassVar[type[SATypeConstructor]] = DefaultSATypeConstructor
    sa_dialect: ClassVar[DefaultDialect] = DefaultDialect()

    @classmethod
    def get_dialect_names(cls) -> Sequence[DialectName]:
        dialect_names = sorted({bit.name for bit in cls.dialects.bits}, key=lambda item: item.name)
        return dialect_names

    @classmethod
    def registration_hook(cls) -> None:
        """Do some non-standard stuff here. Think twice before implementing."""
