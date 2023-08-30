import pytest

from bi_db_testing.loader import load_bi_db_testing
from bi_formula_ref.loader import load_formula_ref


@pytest.fixture(scope='session', autouse=True)
def loaded_libraries() -> None:
    load_formula_ref()
    load_bi_db_testing()
