from __future__ import annotations

from typing import (
    Any,
)

from dl_api_connector.api_schema.top_level import (
    BaseTopLevelSchema,
    USEntryBaseSchema,
)
from dl_constants.enums import OperationsMode
from dl_core.us_manager.us_manager import USManagerBase


def prepare_schema_context(
    usm: USManagerBase | None = None,
    op_mode: OperationsMode | None = None,
    editable_object: Any | None = None,
) -> dict[Any, Any]:
    return {
        BaseTopLevelSchema.CTX_KEY_EDITABLE_OBJECT: editable_object,
        BaseTopLevelSchema.CTX_KEY_OPERATIONS_MODE: op_mode,
        USEntryBaseSchema.CTX_KEY_USM: usm,
    }
