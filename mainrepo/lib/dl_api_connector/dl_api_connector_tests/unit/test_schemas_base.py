from __future__ import annotations

from typing import (
    Any,
    Dict,
    Set,
)

from marshmallow import (
    ValidationError,
    fields,
)
import pytest

from dl_api_connector.api_schema.extras import (
    CreateMode,
    EditMode,
    FieldExtra,
)
from dl_api_connector.api_schema.top_level import BaseTopLevelSchema
from dl_testing.utils import get_log_record


def test_allowed_unknown_fields(caplog):
    caplog.set_level("INFO")

    def get_unk_fields_record():
        return get_log_record(
            caplog,
            predicate=lambda r: r.msg.startswith("Got unknown fields for schema"),
            single=True,
        )

    class SampleSchema(BaseTopLevelSchema[Dict[str, Any]]):
        TARGET_CLS = dict

        a = fields.String(required=True)
        b = fields.String(required=True, bi_extra=FieldExtra(editable=True))

        def create_object(self, data: Dict[str, Any]) -> dict:
            return dict(data)

        def update_object(self, obj: dict, data: Dict[str, Any]) -> dict:
            obj.update(data)
            return obj

        def get_allowed_unknown_fields(self) -> Set[str]:
            super_fields = super().get_allowed_unknown_fields()
            super_fields.update(
                [
                    "allowed_unknown_field",
                    "a",
                ]
            )
            return super_fields

    # Allowed unknown field create
    caplog.clear()

    loaded_data = SampleSchema(
        context={SampleSchema.CTX_KEY_OPERATIONS_MODE: CreateMode.create},
    ).load(dict(a="a", b="b", allowed_unknown_field="val"))
    assert loaded_data == dict(a="a", b="b")

    unk_record = get_unk_fields_record()
    assert unk_record.schema_unk_fields == ["allowed_unknown_field"]
    assert unk_record.schema_unk_fields_allowed == ["allowed_unknown_field"]
    assert unk_record.schema_unk_fields_disallowed == []

    # Disallowed unknown fields create
    caplog.clear()

    with pytest.raises(ValidationError) as validation_err:
        SampleSchema(
            context={SampleSchema.CTX_KEY_OPERATIONS_MODE: CreateMode.create},
        ).load(dict(a="", b="", len=1))

    assert validation_err.value.messages == dict(
        len=["Unknown field."],
    )

    unk_record = get_unk_fields_record()
    assert unk_record.schema_unk_fields == ["len"]
    assert unk_record.schema_unk_fields_allowed == []
    assert unk_record.schema_unk_fields_disallowed == ["len"]

    # Allowed unknown field update
    caplog.clear()

    edit_target = dict(a="old_val_a", b="old_val_b")
    SampleSchema(
        context={
            SampleSchema.CTX_KEY_OPERATIONS_MODE: EditMode.edit,
            SampleSchema.CTX_KEY_EDITABLE_OBJECT: edit_target,
        },
    ).load(
        dict(a="new_val_a", b="new_val_b", allowed_unknown_field="val")  # this field should be ignored
    )
    assert edit_target == dict(a="old_val_a", b="new_val_b")

    unk_record = get_unk_fields_record()
    assert unk_record.schema_unk_fields == ["a", "allowed_unknown_field"]
    assert unk_record.schema_unk_fields_allowed == ["a", "allowed_unknown_field"]
    assert unk_record.schema_unk_fields_disallowed == []

    # Disallowed unknown fields update
    caplog.clear()

    with pytest.raises(ValidationError) as validation_err:
        edit_target = dict(a="old_val_a", b="old_val_b")
        SampleSchema(
            context={
                SampleSchema.CTX_KEY_OPERATIONS_MODE: EditMode.edit,
                SampleSchema.CTX_KEY_EDITABLE_OBJECT: edit_target,
            },
        ).load(dict(b="new_val_b", some_unknown_field="val", a="new_val_a"))

    assert validation_err.value.messages == dict(
        some_unknown_field=["Unknown field."],
    )

    unk_record = get_unk_fields_record()
    assert unk_record.schema_unk_fields == ["a", "some_unknown_field"]
    assert unk_record.schema_unk_fields_allowed == ["a"]
    assert unk_record.schema_unk_fields_disallowed == ["some_unknown_field"]
