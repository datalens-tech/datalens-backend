from __future__ import annotations

import json
from typing import (
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Sized,
)

from aiohttp import web
import attr
from marshmallow import (
    Schema,
    fields,
)

from dl_constants.types import TJSONLike


@attr.s
class SC:
    name: str = attr.ib()
    type: str = attr.ib(
        validator=attr.validators.in_(
            [
                "string",
                "integer",
                "float",
                "boolean",
                "date",
                "datetime",
            ]
        )
    )
    nullable: bool = attr.ib(default=False)
    title: Optional[str] = attr.ib(default=None)


@attr.s
class Table:
    name: str = attr.ib()
    schema: List[SC] = attr.ib()
    partition_key: List[str] = attr.ib(factory=lambda: [])
    meta: Dict[str, str] = attr.ib(factory=lambda: {})
    data: Iterable[Sequence[TJSONLike]] = attr.ib(factory=lambda: [])
    _data_length: Optional[int] = attr.ib(default=None)

    @property
    def data_length(self) -> int:
        if self._data_length is not None:
            return self._data_length

        if isinstance(self.data, Sized):
            return len(self.data)

        raise ValueError("Data length is unknown")


@attr.s
class TablesRegistry:
    tbl_list: List[Table] = attr.ib(factory=lambda: [])

    def get_table(self, table_name) -> Optional[Table]:  # type: ignore  # TODO: fix
        return next((t for t in self.tbl_list if t.name == table_name), None)


#
# Schemas
#
class ColumnSchema(Schema):
    name = fields.String()
    type = fields.String()
    nullable = fields.Bool()
    title = fields.String()


class TablePropertiesSchema(Schema):
    name = fields.String()
    schema = fields.Nested(ColumnSchema, many=True)
    partition_key = fields.List(fields.String())
    meta = fields.Dict(keys=fields.String(), values=fields.String())


def create_app(table_registry: TablesRegistry):  # type: ignore  # TODO: fix
    app = web.Application()
    app["TABLE_REGISTRY"] = table_registry

    def get_table_registry() -> TablesRegistry:
        return app["TABLE_REGISTRY"]

    def get_table_for_request(request: web.Request):  # type: ignore  # TODO: fix
        tbl = get_table_registry().get_table(request.match_info["table_name"])
        if tbl is None:
            raise web.HTTPNotFound()
        return tbl

    async def rq_table_data(request: web.Request):  # type: ignore  # TODO: fix
        table = get_table_for_request(request)
        resp = web.StreamResponse(status=200)
        resp.headers["X-DL-Row-Count"] = str(table.data_length)
        resp.headers["X-DL-Table-Definition"] = TablePropertiesSchema().dumps(table)
        resp.enable_chunked_encoding()
        await resp.prepare(request)

        for idx, data_line in enumerate(table.data):
            bytes_line = json.dumps(data_line, ensure_ascii=True).encode()
            if idx != 0:
                bytes_line = b"\n" + bytes_line

            await resp.write(bytes_line)

        await resp.write_eof()
        return resp

    async def rq_list_tables(_):  # type: ignore  # TODO: fix
        tr = get_table_registry()

        return web.json_response({"tables": [t.name for t in tr.tbl_list]})

    async def rq_table_properties(request: web.Request):  # type: ignore  # TODO: fix
        table = get_table_for_request(request)
        return web.json_response(TablePropertiesSchema().dump(table))

    app.router.add_post("/list_tables", rq_list_tables)
    app.router.add_post("/table_data/{table_name}", rq_table_data)
    app.router.add_post("/table_properties/{table_name}", rq_table_properties)
    return app
