from dl_core.loader import (
    CoreLibraryConfig,
    load_core_lib,
)


def pytest_configure(config):
    load_core_lib(core_lib_config=CoreLibraryConfig(core_connector_ep_names=["clickhouse", "postgresql"]))
