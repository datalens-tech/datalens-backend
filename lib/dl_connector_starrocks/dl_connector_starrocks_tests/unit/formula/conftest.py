from dl_formula.loader import load_formula_lib


def pytest_configure(config):  # type: ignore[no-untyped-def]
    load_formula_lib()
