import enum
from typing import (
    Mapping,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

from bi_external_api.domain import external as ext
from bi_external_api.domain.internal.dl_common import (
    EntryScope,
    EntrySummary,
)


_DST_ENUM_T = TypeVar("_DST_ENUM_T", bound=enum.Enum)


def convert_enum_by_name(subject: enum.Enum, target_cls: Type[_DST_ENUM_T]) -> _DST_ENUM_T:
    return target_cls[subject.name]


def convert_enum_by_name_allow_none(
    subject: Optional[enum.Enum], target_cls: Type[_DST_ENUM_T]
) -> Optional[_DST_ENUM_T]:
    if subject is None:
        return None
    return convert_enum_by_name(subject, target_cls)


def convert_entry_scope_to_ext_entry_kind(scope: EntryScope) -> ext.EntryKind:
    return {
        EntryScope.connection: ext.EntryKind.connection,
        EntryScope.dataset: ext.EntryKind.dataset,
        EntryScope.widget: ext.EntryKind.chart,
        EntryScope.dash: ext.EntryKind.dashboard,
    }[scope]


def convert_int_entry_summary_to_name_map_entry(name: str, summary: EntrySummary) -> ext.NameMapEntry:
    return ext.NameMapEntry(
        local_name=name,
        entry_kind=convert_entry_scope_to_ext_entry_kind(summary.scope),
        unique_entry_id=summary.id,
        legacy_location=tuple(summary.workbook_id.split("/") + [summary.name]),
    )


def convert_int_name_remap_to_ext_name_map(name_remap: Mapping[str, EntrySummary]) -> Sequence[ext.NameMapEntry]:
    ext_name_map = [
        convert_int_entry_summary_to_name_map_entry(wb_local_name, summary)
        for wb_local_name, summary in name_remap.items()
    ]
    ext_name_map.sort(key=lambda name_map_entry: tuple(name_map_entry.legacy_location))
    return tuple(ext_name_map)
