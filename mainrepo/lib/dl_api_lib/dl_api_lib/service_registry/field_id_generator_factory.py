from __future__ import annotations

import attr

from dl_core.components.ids import (
    DEFAULT_FIELD_ID_GENERATOR_TYPE,
    FIELD_ID_GENERATOR_MAP,
    FieldIdGenerator,
    FieldIdGeneratorType,
)
from dl_core.us_dataset import Dataset


@attr.s
class FieldIdGeneratorFactory:
    field_id_generator_type: FieldIdGeneratorType = attr.ib(kw_only=True, default=DEFAULT_FIELD_ID_GENERATOR_TYPE)

    def __attrs_post_init__(self) -> None:
        if self.field_id_generator_type not in FIELD_ID_GENERATOR_MAP:
            self.field_id_generator_type = DEFAULT_FIELD_ID_GENERATOR_TYPE

    def get_field_id_generator(
        self,
        ds: Dataset,
    ) -> FieldIdGenerator:
        FieldIdGeneratorClass = FIELD_ID_GENERATOR_MAP[self.field_id_generator_type]
        return FieldIdGeneratorClass(dataset=ds)
