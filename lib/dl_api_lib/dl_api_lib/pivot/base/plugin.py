from typing import (
    ClassVar,
    Type,
)

from dl_api_lib.pivot.base.transformer_factory import PivotTransformerFactory
from dl_constants.enums import DataPivotEngineType


class PivotEnginePlugin:
    pivot_engine_type: ClassVar[DataPivotEngineType]
    transformer_factory_cls: Type[PivotTransformerFactory]
