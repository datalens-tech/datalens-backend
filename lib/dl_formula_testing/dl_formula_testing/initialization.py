from pytest import Config

from dl_db_testing.loader import load_db_testing_lib
from dl_formula.loader import load_formula_lib
from dl_formula_testing.configuration import FormulaTestEnvironmentConfiguration


def initialize_formula_test(pytest_config: Config, formula_test_config: FormulaTestEnvironmentConfiguration) -> None:
    load_db_testing_lib()
    load_formula_lib(formula_lib_config=formula_test_config.get_formula_library_config())
