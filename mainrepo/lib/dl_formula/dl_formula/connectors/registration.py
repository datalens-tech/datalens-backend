from typing import (
    Iterable,
    Type,
)

from dl_formula.connectors.base.connector import FormulaConnector
from dl_formula.connectors.base.type_constructor import register_type_constructor
from dl_formula.core.dialect import (
    register_basic_dialects,
    register_default_dialect,
    register_dialect_namespace,
)
from dl_formula.definitions.base import NodeTranslation
from dl_formula.definitions.literals import register_literalizer
from dl_formula.definitions.registry import OPERATION_REGISTRY
from dl_formula.translation.columns import register_column_renderer_cls
from dl_formula.translation.context_processing import register_context_processor
from dl_formula.translation.sa_dialects import register_sa_dialect

_REGISTERED: set[Type[FormulaConnector]] = set()


class FormulaConnectorRegistrator:
    @classmethod
    def _register_op_definitions(cls, op_definitions: Iterable[NodeTranslation]) -> None:
        for def_item in op_definitions:
            OPERATION_REGISTRY.register(def_item)

    @classmethod
    def register_connector(cls, connector: Type[FormulaConnector]) -> None:
        if connector in _REGISTERED:
            return

        cls._register_op_definitions(connector.op_definitions)
        register_literalizer(connector.dialects, connector.literalizer_cls)
        register_context_processor(connector.dialects, connector.context_processor_cls)
        register_column_renderer_cls(connector.dialects, connector.column_renderer_cls)
        register_basic_dialects(connector.dialects)
        register_dialect_namespace(connector.dialect_ns_cls)
        for dialect_name in connector.get_dialect_names():
            if connector.default_dialect is not None:
                register_default_dialect(dialect_name, connector.default_dialect)
            register_type_constructor(
                dialect_name=dialect_name,
                type_constructor_cls=connector.type_constructor_cls,
            )

        for dialect in connector.dialects.to_list(with_self=True):
            register_sa_dialect(dialect=dialect, sa_dialect=connector.sa_dialect)

        connector.registration_hook()  # for some weird custom actions

        _REGISTERED.add(connector)


CONN_REG_FORMULA = FormulaConnectorRegistrator
