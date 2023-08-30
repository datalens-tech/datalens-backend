import base64

import marshmallow
import pytest

import bi_core.marshmallow as bi_core_marshmallow


class Schema(marshmallow.Schema):
    field = bi_core_marshmallow.Base64StringField()


def test_load():
    schema = Schema()

    value = b"data:smt/smt;base64," + base64.b64encode(b"test")

    data = schema.load(data={"field": value})
    assert data["field"] == "test"

    data = schema.load(data={"field": value.decode(encoding="utf-8")})
    assert data["field"] == "test"

    data = schema.load(data={"field": ""})
    assert data["field"] is None

    with pytest.raises(marshmallow.exceptions.ValidationError):
        schema.load(data=base64.b64encode(b"test"))

    with pytest.raises(marshmallow.exceptions.ValidationError):
        schema.load(data={"field": 123})
