from __future__ import annotations

import traceback
from typing import Any

from marshmallow import (
    Schema,
    fields,
    post_load,
)
from marshmallow_oneofschema import OneOfSchema

from dl_constants.exc import DLBaseError
from dl_core.connection_executors.models.exc import QueryExecutorError


class DLExcSchema(Schema):
    dl_exec_dict = fields.Function(serialize=lambda exc: exc.to_jsonable_dict(), deserialize=lambda o: o)

    @post_load(pass_many=False)
    def post_load(self, data: dict, **_: Any) -> DLBaseError:
        return DLBaseError.from_jsonable_dict(data["dl_exec_dict"])


class OtherExcSchema(Schema):
    exc_type_name = fields.Function(serialize=lambda err: type(err).__qualname__, deserialize=lambda obj: obj)
    exc_msg = fields.Function(serialize=lambda err: str(err), deserialize=lambda obj: obj)
    exc_tb = fields.Function(
        serialize=lambda err: (traceback.format_tb(err.__traceback__) if getattr(err, "__traceback__", None) else None),
        deserialize=lambda obj: obj,
    )

    @post_load(pass_many=False)
    def post_load(self, data: dict, **_: dict) -> QueryExecutorError:
        tb = "".join(data.get("exc_tb") or ())
        return QueryExecutorError(f"{data['exc_type_name']}: {data['exc_msg']}\n{tb}")


class GenericExcSchema(OneOfSchema):
    type_schemas = {  # noqa: RUF012
        "dl_exc": DLExcSchema,
        "other_exc": OtherExcSchema,
    }

    def get_obj_type(self, obj: Any) -> str:
        if isinstance(obj, DLBaseError):
            return "dl_exc"
        if isinstance(obj, Exception):
            return "other_exc"
        raise TypeError("Can not serialize non-exception")
