from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_api_lib.query.formalization.raw_specs import (
    IdFieldRef,
    IdOrTitleFieldRef,
    TitleFieldRef,
)
import dl_core.exc

if TYPE_CHECKING:
    from dl_api_lib.query.formalization.raw_specs import FieldRef
    from dl_core.components.ids import FieldId
    from dl_core.fields import BIField
    from dl_core.us_dataset import Dataset


@attr.s
class FieldResolver:
    _dataset: Dataset = attr.ib(kw_only=True)

    def _find_field_by_id_or_title(self, id_or_title: str) -> BIField:
        try:
            field = self._dataset.result_schema.by_guid(id_or_title)
        except dl_core.exc.FieldNotFound:
            field = self._dataset.result_schema.by_title(id_or_title)
            # if it still raises `FieldNotFound`, well, then it raises
        return field

    def field_id_from_spec(self, field_ref: Optional[FieldRef]) -> FieldId:
        if isinstance(field_ref, IdFieldRef):
            # Run the search against `result_schema` to make sure the field exists
            # And that the correct exception is thrown if it doesn't
            field_id = self._dataset.result_schema.by_guid(field_ref.id).guid
        elif isinstance(field_ref, TitleFieldRef):
            field_id = self._dataset.result_schema.by_title(field_ref.title).guid
        elif isinstance(field_ref, IdOrTitleFieldRef):
            field_id = self._find_field_by_id_or_title(field_ref.id_or_title).guid
        else:
            raise TypeError(f"Invalid field spec: {field_ref}")
        return field_id
