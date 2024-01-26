from dl_pivot.base.plugin import PivotEnginePlugin
from dl_pivot_pandas.pandas.constants import PIVOT_ENGINE_TYPE_PANDAS
from dl_pivot_pandas.pandas.transformer_factory import PdPivotTransformerFactory


class PandasPivotEnginePlugin(PivotEnginePlugin):
    pivot_engine_type = PIVOT_ENGINE_TYPE_PANDAS
    transformer_factory_cls = PdPivotTransformerFactory
