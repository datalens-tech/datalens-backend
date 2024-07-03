from typing import Any

from marshmallow import (
    Schema,
    ValidationError,
)
from marshmallow import (
    post_dump,
    post_load,
    pre_load,
)
from marshmallow import fields as ma_fields

from dl_constants.enums import (
    RLSPatternType,
    RLSSubjectType,
)
from dl_rls.models import (
    RLSEntry,
    RLSSubject,
)
from dl_rls.rls import RLS


class RLSSchema(Schema):
    class RLSEntrySchema(Schema):
        class RLSSubjectSchema(Schema):
            subject_type = ma_fields.Enum(RLSSubjectType)
            subject_id = ma_fields.String()
            subject_name = ma_fields.String()  # login, group name, etc

            @post_load
            def post_load(self, data: dict[str, Any], **kwargs: Any) -> Any:
                return RLSSubject(**data)

        field_guid = ma_fields.String()
        # TODO: validate that either of these is correct for each instance:
        #   * pattern_type=RLSPatternType.value and allowed_value is not None
        #   * pattern_type=RLSPatternType.all and allowed_value is None
        allowed_value = ma_fields.String(allow_none=True)
        pattern_type = ma_fields.Enum(RLSPatternType, dump_default=RLSPatternType.value)
        subject = ma_fields.Nested(RLSSubjectSchema)

        @post_load
        def post_load(self, data: dict[str, Any], **kwargs: Any) -> Any:
            return RLSEntry(**data)

    items = ma_fields.List(ma_fields.Nested(RLSEntrySchema))

    @pre_load
    def pre_load(self, data: list, **kwargs: Any) -> dict[str, list]:
        return dict(items=data)

    @post_load
    def post_load(self, data: dict[str, list[RLSEntry]], **kwargs: Any) -> RLS:
        if list(data.keys()) != ["items"]:
            raise ValidationError("Only items are expected at this point")
        return RLS(items=data["items"])

    @post_dump
    def post_dump(self, data: dict[str, Any], **kwargs: Any) -> Any:
        return data["items"]
