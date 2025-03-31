from __future__ import annotations

from typing import (
    Optional,
)

from dl_core.db.elements import SchemaColumn


SAMPLE_ID_COLUMN_NAME = "__dl_sample_id"  # TODO: Move somewhere else - it has to do with sampling, not schemas


def are_raw_schemas_same(first: Optional[list[SchemaColumn]], second: Optional[list[SchemaColumn]]) -> bool:
    if first is None or second is None:
        return first == second
    if len(first) != len(second):
        return False
    for col_1, col_2 in zip(first, second, strict=True):
        data_1, data_2 = col_1._asdict(), col_2._asdict()
        # FIXME:
        # API doesn't have the full NativeType definition yet, so we can't compare them directly
        exclude_from_comparison = ("native_type", "source_id")
        for col_data in (data_1, data_2):
            for attr in exclude_from_comparison:
                col_data.pop(attr)

        ntypes_arent_none = all((col_1.native_type, col_2.native_type))
        if (
            # main attrs are different
            data_1 != data_2
            # or exactly one of the two native types is None
            or not ntypes_arent_none
            and col_1.native_type != col_2.native_type
            # or native types have different names (only for different db (conn) types)
            or ntypes_arent_none
            and col_1.native_type.name != col_2.native_type.name
        ):
            return False

    return True
