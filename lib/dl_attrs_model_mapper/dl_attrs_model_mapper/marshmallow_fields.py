from collections.abc import (
    Mapping,
    Sequence,
)
from typing import Any

from marshmallow import (
    ValidationError,
    fields,
)

from dl_attrs_model_mapper.structs.mappings import (
    FrozenMappingStrToStrOrStrSeq,
    FrozenStrMapping,
)
from dl_attrs_model_mapper.structs.singleormultistring import SingleOrMultiString


# TODO FIX: BI-3005 Add verbosity to error messages or totally remove
class FrozenMappingStrToStrOrStrSeqField(fields.Field):
    def _serialize(
        self,
        value: Any,
        attr: str | None,
        obj: Any,
        **kwargs: Any,
    ) -> dict[str, str | list[str]] | None:
        if value is None:
            return None

        assert isinstance(value, FrozenMappingStrToStrOrStrSeq)

        return {d_key: d_value if isinstance(d_value, str) else list(d_value) for d_key, d_value in value.items()}

    def _deserialize(
        self,
        value: Any,
        attr: str | None,
        data: Mapping[str, Any] | None,
        **kwargs: Any,
    ) -> FrozenMappingStrToStrOrStrSeq:
        try:
            assert isinstance(value, dict)
            return FrozenMappingStrToStrOrStrSeq(mapping=value)
        except Exception as error:
            raise ValidationError("Invalid mapping string to (string|list[string])") from error


# TODO FIX: BI-3005 Add verbosity to error messages or totally remove
class SingleOrMultiStringField(fields.Field):
    def _serialize(
        self,
        value: Any,
        attr: str | None,
        obj: Any,
        **kwargs: Any,
    ) -> str | Sequence[str] | None:
        if value is None:
            return None

        assert isinstance(value, SingleOrMultiString)

        if value.is_single:
            return value.as_single()
        return value.as_sequence()

    def _deserialize(
        self,
        value: Any,
        attr: str | None,
        data: Mapping[str, Any] | None,
        **kwargs: Any,
    ) -> SingleOrMultiString | None:
        try:
            if isinstance(value, str):
                return SingleOrMultiString.from_string(value)
            if isinstance(value, list):
                assert all(isinstance(s, str) for s in value)
                return SingleOrMultiString.from_sequence(value)
        except Exception as error:
            raise ValidationError("Must be a string or list of strings") from error
        return None


class FrozenStrMappingField(fields.Dict):
    def _serialize(
        self,
        value: Any,
        attr: str | None,
        obj: Any,
        **kwargs: Any,
    ) -> dict[Any, Any] | None:
        if isinstance(value, FrozenStrMapping):
            value = dict(value)
        return super()._serialize(value, attr, obj, **kwargs)

    def _deserialize(
        self,
        value: Any,
        attr: str | None,
        data: Mapping[str, Any] | None,
        **kwargs: Any,
    ) -> FrozenStrMapping:
        plain_dict = super()._deserialize(value, attr, data, **kwargs)
        return FrozenStrMapping(plain_dict)
