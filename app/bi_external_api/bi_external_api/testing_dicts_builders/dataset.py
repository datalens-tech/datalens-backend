from typing import (
    Any,
    ClassVar,
    Iterable,
    Optional,
    TypeVar,
)

import attr

from .common import DatasetJSONBuilder
from .dataset_data import SAMPLE_SUPER_STORE_LIGHT_CSV


_DS_BUILDER_TV = TypeVar("_DS_BUILDER_TV", bound="BaseDatasetJSONBuilder")


@attr.s()
class BaseDatasetJSONBuilder(DatasetJSONBuilder):
    conn_name: Optional[str] = attr.ib(default=None)
    source: Optional[dict[str, Any]] = attr.ib(default=None, kw_only=True)
    extra_field: list[dict[str, Any]] = attr.ib(factory=list, kw_only=True)
    add_default_fields: bool = attr.ib(default=False, kw_only=True)
    default_field_ids_to_add: Optional[list[str]] = attr.ib(default=None, kw_only=True)
    dataset_name: Optional[str] = attr.ib(default=None, kw_only=True)

    default_source_id: ClassVar[str] = "main"

    @classmethod
    def cs_direct(cls, field_name: str, *, avatar_id: str) -> dict[str, Any]:
        return {"kind": "direct", "avatar_id": avatar_id, "field_name": field_name}

    @classmethod
    def cs_id_formula(cls, formula: str) -> dict[str, Any]:
        return {
            "kind": "id_formula",
            "formula": formula,
        }

    @classmethod
    def field_id_formula(cls, formula: str, *, id: str, cast: str, title: Optional[str] = None) -> dict[str, Any]:
        effective_title = title or f"The {id}"
        return {
            "title": effective_title,
            "id": id,
            "cast": cast,
            "description": None,
            "hidden": False,
            "aggregation": "none",
            "calc_spec": cls.cs_id_formula(formula),
        }

    @classmethod
    def field_direct(
        cls,
        field_name: str,
        *,
        cast: str,
        avatar_id: str,
        aggregation: Optional[str] = None,
    ) -> dict[str, Any]:
        return {
            "title": f"The {field_name.capitalize()}",
            "id": field_name,
            "cast": cast,
            "description": None,
            "hidden": False,
            "aggregation": aggregation or "none",
            "calc_spec": cls.cs_direct(field_name, avatar_id=avatar_id),
        }

    @classmethod
    def source_sql(cls, query: str, *, conn_name: str, id: Optional[str] = None) -> dict[str, Any]:
        effective_id = id if id is not None else cls.default_source_id
        effective_title = "SQL"

        return dict(
            id=effective_id,
            title=effective_title,
            connection_ref=conn_name,  # connection name
            spec=dict(
                kind="sql_query",
                sql=query,
            ),
        )

    def _default_fields(self, avatar_id: Optional[str] = None) -> list[dict[str, Any]]:
        raise NotImplementedError()

    def _get_default_source(self) -> dict[str, Any]:
        raise NotImplementedError()

    def do_add_default_fields(self: _DS_BUILDER_TV) -> _DS_BUILDER_TV:
        return attr.evolve(self, add_default_fields=True)

    def add_field(self: _DS_BUILDER_TV, field: dict[str, Any]) -> _DS_BUILDER_TV:
        return attr.evolve(self, extra_field=[*self.extra_field, field])

    def add_fields(self: _DS_BUILDER_TV, fields: Iterable[dict[str, Any]]) -> _DS_BUILDER_TV:
        return attr.evolve(self, extra_field=[*self.extra_field, *fields])

    def set_source(self: _DS_BUILDER_TV, source: dict[str, Any]) -> _DS_BUILDER_TV:
        return attr.evolve(self, source=source)

    def with_conn_name(self: _DS_BUILDER_TV, conn_name: str) -> _DS_BUILDER_TV:
        return attr.evolve(self, conn_name=conn_name)

    def build_internal(self) -> dict[str, Any]:
        effective_source_wo_conn_ref = self.source or self._get_default_source()

        effective_source = {
            **effective_source_wo_conn_ref,
            "connection_ref": self.conn_name,
        }

        # Filling fields
        effective_fields: list[dict[str, Any]] = []
        default_field_ids_to_add = self.default_field_ids_to_add

        if self.add_default_fields:
            all_default_fields = self._default_fields()

            if default_field_ids_to_add is None:
                effective_fields.extend(all_default_fields)
            else:
                effective_fields.extend(f for f in all_default_fields if f["id"] in default_field_ids_to_add)

        effective_fields.extend(self.extra_field)

        return dict(
            sources=[effective_source],
            avatars={
                "definitions": [
                    {
                        "id": effective_source["id"],
                        "source_id": effective_source["id"],
                        "title": effective_source["title"],
                    }
                ],
                "joins": [],
                "root": effective_source["id"],
            }
            if self.fill_defaults
            else None,
            fields=effective_fields,
        )


_DS_BUILDER_SSSL_TV = TypeVar("_DS_BUILDER_SSSL_TV", bound="SampleSuperStoreLightJSONBuilder")


@attr.s()
class SampleSuperStoreLightJSONBuilder(BaseDatasetJSONBuilder):
    _use_full_data: bool = attr.ib(default=False)

    SAMPLE_SQL_HEADER = """
SELECT * FROM format(CSVWithNamesAndTypes,
$$"category","customer_id","date","order_id","postal_code","profit","region","sales","segment","sub_category"
"String","String","Date","String","String","Float32","String","Float32","String","String"
""".lstrip(
        " \n"
    )

    SAMPLE_SQL_DATA_LIGHT = """
Office Supplies,JD-15895,2022-01-01,CA-2022-143805,23223,694.5,South,2104.6,Corporate,Appliances
""".lstrip(
        " \n"
    )

    SAMPLE_SQL_FOOTER = """
$$)
""".lstrip(
        " \n"
    )

    def with_full_data(self: _DS_BUILDER_SSSL_TV, full_data: bool) -> _DS_BUILDER_SSSL_TV:
        return attr.evolve(self, use_full_data=full_data)

    @classmethod
    def expected_direct_field(cls) -> list[dict[str, Any]]:
        return [
            cls.field_direct("category", cast="string", avatar_id="main"),
            cls.field_direct("customer_id", cast="string", avatar_id="main"),
            cls.field_direct("date", cast="date", avatar_id="main"),
            cls.field_direct("order_id", cast="string", avatar_id="main"),
            cls.field_direct("postal_code", cast="string", avatar_id="main"),
            cls.field_direct("profit", cast="float", avatar_id="main"),
            cls.field_direct("region", cast="string", avatar_id="main"),
            cls.field_direct("sales", cast="float", avatar_id="main"),
            cls.field_direct("segment", cast="string", avatar_id="main"),
            cls.field_direct("sub_category", cast="string", avatar_id="main"),
        ]

    def _default_fields(self, avatar_id: Optional[str] = None) -> list[dict[str, Any]]:
        return self.expected_direct_field()

    def _get_default_source(self) -> dict[str, Any]:
        return self.source_sql(
            query="".join(
                [
                    self.SAMPLE_SQL_HEADER,
                    SAMPLE_SUPER_STORE_LIGHT_CSV if self._use_full_data else self.SAMPLE_SQL_DATA_LIGHT,
                    self.SAMPLE_SQL_FOOTER,
                ]
            ),
            conn_name="whatever",
        )
