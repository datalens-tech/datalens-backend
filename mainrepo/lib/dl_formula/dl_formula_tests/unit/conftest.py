import pytest

from dl_formula.loader import load_bi_formula
from dl_formula.parser.base import FormulaParser
from dl_formula.parser.factory import (
    ParserType,
    get_parser,
)
from dl_formula_testing.forced_literal import forced_literal_use  # noqa

PARSERS = (ParserType.antlr_py,)


@pytest.fixture(
    scope="session",
    params=PARSERS,
    ids=[parser.name for parser in PARSERS],
)
def parser(request) -> FormulaParser:
    parser_type = request.param
    return get_parser(parser_type=parser_type)


@pytest.fixture(scope="session", autouse=True)
def loaded_bi_libraries() -> None:
    load_bi_formula()
