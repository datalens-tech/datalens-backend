from __future__ import annotations

from functools import _CacheInfo
from typing import Dict

import antlr4
from antlr4.Token import Token
from antlr4.error.Errors import ParseCancellationException

import dl_formula.core.exc as exc
import dl_formula.core.nodes as nodes
from dl_formula.core.position import PositionConverter
from dl_formula.parser.antlr.visitor import CustomDataLensVisitor
from dl_formula.parser.base import (
    FORMULA_CACHE_SIZE,
    FormulaParser,
    parser_cache_qualifier,
)
from dl_formula.utils.caching import multi_cached_with_errors


try:
    from dl_formula.parser.antlr.gen.DataLensLexer import DataLensLexer
    from dl_formula.parser.antlr.gen.DataLensParser import DataLensParser
except ImportError as e:
    raise exc.ParserNotFoundError() from e


@multi_cached_with_errors(
    FORMULA_CACHE_SIZE,
    cache_exceptions=(exc.ParseError,),
    cache_qualifier=parser_cache_qualifier,
)
def parse(formula: str) -> nodes.Formula:
    pos_conv = PositionConverter(text=formula)

    if not formula.strip():
        raise exc.ParseEmptyFormulaError("Empty formula", position=pos_conv.idx_to_position(0))

    input_stream = antlr4.InputStream(formula)
    lexer = DataLensLexer(input_stream)
    stream = antlr4.CommonTokenStream(lexer)
    parser = DataLensParser(stream)
    parser._errHandler = antlr4.BailErrorStrategy()
    try:
        tree = parser.parse()
    except ParseCancellationException as err:
        raw_token = err.args[0].offendingToken
        if raw_token.type == Token.EOF:
            raise exc.ParseUnexpectedEOFError(
                "Failed to parse: unexpected end of formula",
                position=pos_conv.idx_to_position(idx=raw_token.start),
            ) from err
        else:
            raise exc.ParseUnexpectedTokenError(
                "Failed to parse formula: unexpected token",
                position=pos_conv.idx_to_position(idx=raw_token.start),
                token=raw_token.text,
            ) from err

    formula_obj = CustomDataLensVisitor(text=formula).visitParse(tree)
    return formula_obj


class AntlrPyFormulaParser(FormulaParser):
    def _parse(self, formula: str) -> nodes.Formula:
        try:
            return parse(formula)
        except RecursionError as err:
            raise exc.ParseRecursionError(
                "Failed to parse formula: maximum recursion depth exceeded",
            ) from err

    def _get_global_stats(self) -> Dict[str, _CacheInfo]:
        return parse.cache_info()  # type: ignore  # 2024-01-30 # TODO: "Callable[..., Any]" has no attribute "cache_info"  [attr-defined]
