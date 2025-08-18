from dl_formula.connectors.base.connector import FormulaConnector
from dl_query_processing.compilation.query_mutator import RemoveConstFromGroupByFormulaAtomicQueryMutator

from dl_connector_ydb.core.ydb.dialect import CustomYqlDialect
from dl_connector_ydb.formula.constants import YqlDialect as YqlDialectNS
from dl_connector_ydb.formula.definitions.all import DEFINITIONS


class YQLFormulaConnector(FormulaConnector):
    dialect_ns_cls = YqlDialectNS
    dialects = YqlDialectNS.YQL
    op_definitions = DEFINITIONS
    sa_dialect = CustomYqlDialect(_add_declare_for_yql_stmt_vars=True)
    # sa_dialect = SAYqlDialect(_add_declare_for_yql_stmt_vars=True)
    # TODO(catsona): Do we need it?
    # type_constructor_cls = YDBTypeConstructor

    @classmethod
    def registration_hook(cls) -> None:
        RemoveConstFromGroupByFormulaAtomicQueryMutator.register_dialect(cls.dialects)
