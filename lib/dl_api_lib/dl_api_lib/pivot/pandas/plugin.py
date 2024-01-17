from dl_api_lib.pivot.base.plugin import PivotEnginePlugin
from dl_api_lib.pivot.pandas.constants import PIVOT_ENGINE_TYPE_PANDAS
from dl_api_lib.pivot.pandas.transformer_factory import PdPivotTransformerFactory


class PandasPivotEnginePlugin(PivotEnginePlugin):
    pivot_engine_type = PIVOT_ENGINE_TYPE_PANDAS
    transformer_factory_cls = PdPivotTransformerFactory
