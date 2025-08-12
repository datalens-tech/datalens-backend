from typing import (
    ClassVar,
)

from dl_constants.enums import DataPivotEngineType
from dl_pivot.base.transformer_factory import PivotTransformerFactory


class PivotEnginePlugin:
    pivot_engine_type: ClassVar[DataPivotEngineType]
    transformer_factory_cls: type[PivotTransformerFactory]
