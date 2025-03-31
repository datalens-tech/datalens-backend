from __future__ import annotations

import datetime
from decimal import Decimal
import json
from typing import (
    Any,
)

import attr

from dl_formula.core.datatype import DataType
from dl_formula_ref.examples.data_table import (
    DataColumn,
    DataTable,
)


@attr.s(frozen=True)
class StorageKey:
    item_key: str = attr.ib(kw_only=True)
    example_key: str = attr.ib(kw_only=True)

    @property
    def as_str(self) -> str:
        return f"{self.item_key}.{self.example_key}"


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj) -> Any:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        if isinstance(obj, datetime.datetime):
            return {"__type__": "datetime", "__value__": obj.isoformat()}
        if isinstance(obj, datetime.date):
            return {"__type__": "date", "__value__": obj.isoformat()}
        if isinstance(obj, Decimal):
            return {"__type__": "decimal", "__value__": str(obj)}
        return super().default(obj)


class CustomJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
        super().__init__(object_hook=self.object_hook, *args, **kwargs)  # noqa: B026

    def object_hook(self, obj_dict: dict[str, Any]) -> Any:
        if obj_dict.get("__type__") == "datetime":
            return datetime.datetime.fromisoformat(obj_dict["__value__"])
        if obj_dict.get("__type__") == "date":
            return datetime.date.fromisoformat(obj_dict["__value__"])
        if obj_dict.get("__type__") == "decimal":
            return Decimal(obj_dict["__value__"])
        return obj_dict


class ReadableDataStorageBase:
    def load(self) -> None:
        raise NotImplementedError

    def get_result_data(self, storage_key: StorageKey) -> DataTable:
        raise NotImplementedError


class WritableDataStorageBase:
    def save(self) -> None:
        raise NotImplementedError

    def set_result_data(self, storage_key: StorageKey, result_data: DataTable) -> None:
        raise NotImplementedError


@attr.s
class ReadableDataStorage(ReadableDataStorageBase):
    _filename: str = attr.ib(kw_only=True)
    _data: dict[str, DataTable] = attr.ib(factory=dict)

    def _load_data_table(self, table_data: dict) -> DataTable:
        return DataTable(
            columns=[
                DataColumn(name=col_data["name"], data_type=DataType[col_data["type"]])
                for col_data in table_data["columns"]
            ],
            rows=table_data["rows"],
        )

    def load(self) -> None:
        with open(self._filename) as storage_file:
            data_as_list = json.load(storage_file, cls=CustomJSONDecoder)
        self._data = {key: self._load_data_table(table_data) for key, table_data in data_as_list}

    def get_result_data(self, storage_key: StorageKey) -> DataTable:
        return self._data[storage_key.as_str]


@attr.s
class WritableDataStorage(WritableDataStorageBase):
    _filename: str = attr.ib(kw_only=True)
    _data: dict[str, DataTable] = attr.ib(init=False, factory=dict)

    def _dump_data_table(self, data_table: DataTable) -> dict:
        return {
            "columns": [{"name": col.name, "type": col.data_type.name} for col in data_table.columns],
            "rows": data_table.rows,
        }

    def save(self) -> None:
        data_as_list = sorted([[key, self._dump_data_table(data_table)] for key, data_table in self._data.items()])
        with open(self._filename, "w") as storage_file:
            json.dump(data_as_list, storage_file, cls=CustomJSONEncoder)

    def set_result_data(self, storage_key: StorageKey, result_data: DataTable) -> None:
        self._data[storage_key.as_str] = result_data
