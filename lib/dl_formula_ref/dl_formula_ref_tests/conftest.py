import pytest

from dl_db_testing.loader import load_db_testing_lib
from dl_formula_ref.loader import load_formula_ref


@pytest.fixture(scope="session", autouse=True)
def loaded_libraries() -> None:
    load_formula_ref()
    load_db_testing_lib()
