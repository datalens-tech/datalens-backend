from dl_formula_testing.configuration import FormulaTestEnvironmentConfiguration
from dl_formula_testing.initialization import initialize_formula_test


def pytest_configure(config):  # noqa
    initialize_formula_test(
        pytest_config=config,
        formula_test_config=FormulaTestEnvironmentConfiguration(
            formula_connector_ep_names=("starrocks",),
        ),
    )
