from typing import (
    Collection,
    Optional,
    Type,
)

import attr

import dl_formula_ref as package
from dl_formula_ref.plugins.base.plugin import FormulaRefPlugin
from dl_formula_ref.plugins.registration import FORMULA_REF_PLUGIN_REG
from dl_utils.entrypoints import EntrypointClassManager

_PLUGIN_EP_GROUP = f"{package.__name__}.plugins"


@attr.s
class FormulaRefPluginEntrypointManager(EntrypointClassManager[FormulaRefPlugin]):
    entrypoint_group_name = attr.ib(init=False, default=_PLUGIN_EP_GROUP)


def get_all_formula_ref_plugins() -> dict[str, Type[FormulaRefPlugin]]:
    ep_mgr = FormulaRefPluginEntrypointManager()
    return ep_mgr.get_all_ep_classes()


def _register_plugin(plugin_cls: Type[FormulaRefPlugin]) -> None:
    FORMULA_REF_PLUGIN_REG.register_plugin(plugin_cls)


def register_all_plugins(plugin_ep_names: Optional[Collection[str]] = None) -> None:
    for ep_name, plugin_cls in sorted(get_all_formula_ref_plugins().items()):
        if plugin_ep_names is not None and ep_name not in plugin_ep_names:
            continue
        _register_plugin(plugin_cls)
