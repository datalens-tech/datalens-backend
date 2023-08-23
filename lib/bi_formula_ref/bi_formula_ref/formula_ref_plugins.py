from typing import Collection, Optional, Type

import attr

from bi_utils.entrypoints import EntrypointClassManager

import bi_formula_ref as package
from bi_formula_ref.plugins.base.plugin import FormulaRefPlugin
from bi_formula_ref.plugins.registration import FORMULA_REF_PLUGIN_REG


_PLUGIN_EP_GROUP = f'{package.__name__}.plugins'


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

    # FIXME: Remove
    from bi_connector_clickhouse.formula_ref.plugin import ClickHouseFormulaRefPlugin
    _register_plugin(ClickHouseFormulaRefPlugin)
