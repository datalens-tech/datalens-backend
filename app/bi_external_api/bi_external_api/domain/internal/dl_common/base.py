import enum
from typing import (
    Any,
    ClassVar,
    Optional,
)

import attr

from bi_external_api.attrs_model_mapper import MapperBaseModel


class DatasetAPIBaseModel(MapperBaseModel):
    ignored_keys: ClassVar[set[str]] = set()
    ignore_not_declared_fields: ClassVar[bool] = False

    @classmethod
    def _remove_ignored_keys(cls, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        if cls.ignore_not_declared_fields:
            declared_fields = attr.fields_dict(cls)
            return {key: val for key, val in data.items() if key in declared_fields}

        return {key: val for key, val in data.items() if key not in cls.ignored_keys}

    @classmethod
    def pre_load(cls, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        normalized_data = cls.adopt_json_before_deserialization(data)

        return cls._remove_ignored_keys(data if normalized_data is None else normalized_data)

    @classmethod
    def post_dump(cls, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        normalized_data = cls.adopt_json_before_sending_to_api(data)
        if normalized_data is not None:
            return normalized_data

        return None

    @classmethod
    def adopt_json_before_sending_to_api(cls, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        return None

    @classmethod
    def adopt_json_before_deserialization(cls, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        return None


@enum.unique
class EntryScope(enum.Enum):
    connection = "connection"
    dataset = "dataset"
    widget = "widget"
    dash = "dash"
    folder = "folder"


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class EntrySummary:
    id: str
    name: str
    workbook_id: str
    scope: EntryScope

    @classmethod
    def from_key(
        cls,
        *,
        entry_id: str,
        key: str,
        scope: EntryScope,
        true_workbook: bool,
        workbook_id: str = None,
    ) -> "EntrySummary":
        assert (
            "/" in key
        ), f"Can not build entry summary for entry in US root: {scope.name}(id={entry_id!r}, key={key!r})"

        name = key.split("/")[-1]
        effective_workbook_id: str

        if true_workbook:
            assert workbook_id is not None
            effective_workbook_id = workbook_id
        else:
            effective_workbook_id = "/".join(key.split("/")[:-1])

        return cls(
            scope=scope,
            id=entry_id,
            name=name,
            workbook_id=effective_workbook_id,
        )


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class EntryInstance:
    SCOPE: ClassVar[EntryScope]
    summary: EntrySummary = attr.ib()

    def __attrs_post_init__(self) -> None:
        assert self.summary.scope == self.SCOPE


class IntModelTags(enum.Enum):
    connection_id = enum.auto()
    dataset_id = enum.auto()
    chart_id = enum.auto()
