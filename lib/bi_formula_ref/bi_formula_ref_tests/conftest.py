import pytest

from bi_db_testing.loader import load_bi_db_testing
from bi_formula.loader import load_bi_formula


@pytest.fixture(scope='session', autouse=True)
def loaded_libraries() -> None:
    load_bi_formula()
    load_bi_db_testing()
