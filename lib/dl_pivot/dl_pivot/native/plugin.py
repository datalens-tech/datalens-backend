from dl_pivot.base.plugin import PivotEnginePlugin
from dl_pivot.native.constants import PIVOT_ENGINE_TYPE_NATIVE
from dl_pivot.native.transformer_factory import NativePivotTransformerFactory


class NativePivotEnginePlugin(PivotEnginePlugin):
    pivot_engine_type = PIVOT_ENGINE_TYPE_NATIVE
    transformer_factory_cls = NativePivotTransformerFactory
