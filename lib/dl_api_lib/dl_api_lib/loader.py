from typing import (
    Collection,
)

import attr

from dl_api_lib.app_connectors import register_all_connectors
from dl_api_lib.app_settings import WhitelistsAppSettings
from dl_core.loader import (
    CoreLibraryConfig,
    load_core_lib,
    preload_bi_core,
)
from dl_formula.loader import (
    FormulaLibraryConfig,
    load_formula_lib,
    preload_bi_formula,
)
from dl_pivot.plugin_registration import (
    preload_pivot_engine_plugins,
    register_all_pivot_engine_plugins,
)


@attr.s(frozen=True)
class ApiLibraryConfig:
    formula_lib_config: FormulaLibraryConfig = attr.ib(kw_only=True, default=FormulaLibraryConfig())
    core_lib_config: CoreLibraryConfig = attr.ib(kw_only=True, default=CoreLibraryConfig())
    api_connector_ep_names: Collection[str] | None = attr.ib(kw_only=True, default=None)


def preload_api_lib() -> None:
    """Loads all entrypoint connectors without registering them, e.g. to trigger constants declaration"""

    preload_bi_core()
    preload_bi_formula()
    preload_pivot_engine_plugins()


def load_api_lib(api_lib_config: ApiLibraryConfig = ApiLibraryConfig()) -> None:  # noqa: B008
    load_formula_lib(formula_lib_config=api_lib_config.formula_lib_config)
    load_core_lib(core_lib_config=api_lib_config.core_lib_config)
    register_all_connectors(connector_ep_names=api_lib_config.api_connector_ep_names)
    register_all_pivot_engine_plugins()


def load_api_lib_with_settings() -> None:
    preload_api_lib()
    whitelists = WhitelistsAppSettings()
    api_lib_config = ApiLibraryConfig(
        api_connector_ep_names=whitelists.BI_API_CONNECTOR_WHITELIST,
        core_lib_config=CoreLibraryConfig(core_connector_ep_names=whitelists.CORE_CONNECTOR_WHITELIST),
    )

    load_formula_lib(formula_lib_config=api_lib_config.formula_lib_config)
    load_core_lib(core_lib_config=api_lib_config.core_lib_config)
    register_all_connectors(connector_ep_names=api_lib_config.api_connector_ep_names)
    register_all_pivot_engine_plugins()
