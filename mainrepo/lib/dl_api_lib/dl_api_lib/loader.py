from typing import (
    Collection,
    Optional,
)

import attr

from dl_api_lib.app_connectors import register_all_connectors
from dl_core.loader import (
    CoreLibraryConfig,
    load_bi_core,
    preload_bi_core,
)
from dl_formula.loader import (
    FormulaLibraryConfig,
    load_bi_formula,
    preload_bi_formula,
)


@attr.s(frozen=True)
class ApiLibraryConfig:
    formula_lib_config: FormulaLibraryConfig = attr.ib(kw_only=True, default=FormulaLibraryConfig())
    core_lib_config: CoreLibraryConfig = attr.ib(kw_only=True, default=CoreLibraryConfig())
    api_connector_ep_names: Optional[Collection[str]] = attr.ib(kw_only=True, default=None)


def preload_bi_api_lib() -> None:
    """Loads all entrypoint connectors without registering them, e.g. to trigger constants declaration"""

    preload_bi_core()
    preload_bi_formula()


def load_bi_api_lib(api_lib_config: ApiLibraryConfig = ApiLibraryConfig()) -> None:
    load_bi_formula(formula_lib_config=api_lib_config.formula_lib_config)
    load_bi_core(core_lib_config=api_lib_config.core_lib_config)
    register_all_connectors(connector_ep_names=api_lib_config.api_connector_ep_names)
