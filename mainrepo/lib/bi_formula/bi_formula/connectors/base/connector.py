from typing import (
    ClassVar,
    Iterable,
    Optional,
    Sequence,
    Type,
)

from sqlalchemy.engine.default import DefaultDialect

from bi_formula.connectors.base.column import (
    ColumnRenderer,
    DefaultColumnRenderer,
)
from bi_formula.connectors.base.context_processor import ContextPostprocessor
from bi_formula.connectors.base.literal import Literalizer
from bi_formula.connectors.base.type_constructor import (
    DefaultSATypeConstructor,
    SATypeConstructor,
)
from bi_formula.core.dialect import (
    DialectCombo,
    DialectName,
    DialectNamespace,
    StandardDialect,
)
from bi_formula.definitions.base import NodeTranslation


class FormulaConnector:
    dialect_ns_cls: ClassVar[Type[DialectNamespace]] = StandardDialect
    dialects: ClassVar[DialectCombo]
    default_dialect: ClassVar[Optional[DialectCombo]] = None
    op_definitions: ClassVar[Iterable[NodeTranslation]] = []
    literalizer_cls: ClassVar[Type[Literalizer]] = Literalizer
    column_renderer_cls: ClassVar[Type[ColumnRenderer]] = DefaultColumnRenderer
    context_processor_cls: ClassVar[Type[ContextPostprocessor]] = ContextPostprocessor
    type_constructor_cls: ClassVar[Type[SATypeConstructor]] = DefaultSATypeConstructor
    sa_dialect: ClassVar[DefaultDialect] = DefaultDialect()

    @classmethod
    def get_dialect_names(cls) -> Sequence[DialectName]:
        dialect_names = sorted({bit.name for bit in cls.dialects.bits}, key=lambda item: item.name)
        return dialect_names

    @classmethod
    def registration_hook(cls) -> None:
        """Do some non-standard stuff here. Think twice before implementing."""
