from typing import (
    ClassVar,
    Type,
)

from bi_db_testing.database.engine_wrapper import EngineWrapperBase


class DbTestingConnector:
    engine_wrapper_classes: ClassVar[tuple[Type[EngineWrapperBase], ...]] = ()
