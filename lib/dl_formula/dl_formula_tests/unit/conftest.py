import pytest

from dl_formula.parser.base import FormulaParser
from dl_formula.parser.factory import (
    ParserType,
    get_parser,
)
from dl_formula_testing.configuration import FormulaTestEnvironmentConfiguration
from dl_formula_testing.forced_literal import forced_literal_use  # noqa
from dl_formula_testing.initialization import initialize_formula_test


PARSERS = (ParserType.antlr_py,)


def pytest_configure(config):  # noqa
    initialize_formula_test(
        pytest_config=config,
        formula_test_config=FormulaTestEnvironmentConfiguration(
            formula_connector_ep_names=("clickhouse",),
        ),
    )


@pytest.fixture(
    scope="session",
    params=PARSERS,
    ids=[parser.name for parser in PARSERS],
)
def parser(request) -> FormulaParser:
    parser_type = request.param
    return get_parser(parser_type=parser_type)
