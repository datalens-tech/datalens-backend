import marshmallow
import pytest

import dl_core.marshmallow as bi_core_marshmallow


class Schema(marshmallow.Schema):
    field = bi_core_marshmallow.OnOffField()


def test_dump():
    schema = Schema()

    data = schema.dump(obj={"field": True})
    assert data["field"] == "on"

    data = schema.dump(obj={"field": False})
    assert data["field"] == "off"


def test_load():
    schema = Schema()

    data = schema.load(data={"field": "on"})
    assert data["field"] is True

    data = schema.load(data={"field": "off"})
    assert data["field"] is False

    with pytest.raises(marshmallow.exceptions.ValidationError):
        schema.load(data={"field": True})

    with pytest.raises(marshmallow.exceptions.ValidationError):
        schema.load(data={"field": "On"})
