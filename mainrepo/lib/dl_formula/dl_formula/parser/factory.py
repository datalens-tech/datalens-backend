from __future__ import annotations

from enum import (
    Enum,
    unique,
)

from dl_formula.parser.antlr.main import AntlrPyFormulaParser
from dl_formula.parser.base import FormulaParser


@unique
class ParserType(Enum):
    antlr_py = "antlr_py"


def get_parser(parser_type: ParserType = ParserType.antlr_py) -> FormulaParser:
    if parser_type == ParserType.antlr_py:
        return AntlrPyFormulaParser()

    raise ValueError(f"Unknown parser type: {parser_type}")
