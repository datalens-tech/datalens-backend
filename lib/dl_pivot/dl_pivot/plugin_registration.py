
import attr

from dl_constants.enums import DataPivotEngineType
import dl_pivot as package
from dl_pivot.base.plugin import PivotEnginePlugin
from dl_pivot.base.transformer_factory import PivotTransformerFactory
from dl_utils.entrypoints import EntrypointClassManager


_PIVOT_TRANSFORMER_FACTORY_CLS_REGISTRY: dict[DataPivotEngineType, type[PivotTransformerFactory]] = {}


def get_pivot_transformer_factory_cls(pivot_engine_type: DataPivotEngineType) -> type[PivotTransformerFactory]:
    return _PIVOT_TRANSFORMER_FACTORY_CLS_REGISTRY[pivot_engine_type]


def register_pivot_transformer_factory_cls(
    pivot_engine_type: DataPivotEngineType,
    factory_cls: type[PivotTransformerFactory],
) -> None:
    if (existing_factory_cls := _PIVOT_TRANSFORMER_FACTORY_CLS_REGISTRY.get(pivot_engine_type)) is not None:
        assert (
            factory_cls is existing_factory_cls
        ), f"{existing_factory_cls} is already registered for {pivot_engine_type}"
    else:
        _PIVOT_TRANSFORMER_FACTORY_CLS_REGISTRY[pivot_engine_type] = factory_cls


def register_pivot_engine_plugin(plugin_cls: type[PivotEnginePlugin]) -> None:
    register_pivot_transformer_factory_cls(
        pivot_engine_type=plugin_cls.pivot_engine_type,
        factory_cls=plugin_cls.transformer_factory_cls,
    )


_PIVOT_PLUGIN_EP_GROUP = f"{package.__name__}.pivot_engine_plugins"


@attr.s
class PivotEnginePluginEntrypointManager(EntrypointClassManager[PivotEnginePlugin]):
    entrypoint_group_name = attr.ib(init=False, default=_PIVOT_PLUGIN_EP_GROUP)


def _get_all_pivot_engine_plugins() -> dict[str, type[PivotEnginePlugin]]:
    ep_mgr = PivotEnginePluginEntrypointManager()
    return ep_mgr.get_all_ep_classes()


def register_all_pivot_engine_plugins() -> None:
    for plugin_cls in _get_all_pivot_engine_plugins().values():
        register_pivot_engine_plugin(plugin_cls)


def preload_pivot_engine_plugins() -> None:
    _get_all_pivot_engine_plugins()
