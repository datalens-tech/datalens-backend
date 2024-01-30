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
    def _serialize(self, value, attr, obj, **kwargs):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        if value is None:
            return None

        assert isinstance(value, FrozenMappingStrToStrOrStrSeq)

        return {d_key: d_value if isinstance(d_value, str) else list(d_value) for d_key, d_value in value.items()}

    def _deserialize(self, value, attr, data, **kwargs):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        try:
            assert isinstance(value, dict)
            return FrozenMappingStrToStrOrStrSeq(mapping=value)
        except Exception as error:
            raise ValidationError("Invalid mapping string to (string|list[string])") from error


# TODO FIX: BI-3005 Add verbosity to error messages or totally remove
class SingleOrMultiStringField(fields.Field):
    def _serialize(self, value, attr, obj, **kwargs):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        if value is None:
            return None

        assert isinstance(value, SingleOrMultiString)

        if value.is_single:
            return value.as_single()
        return value.as_sequence()

    def _deserialize(self, value, attr, data, **kwargs):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        try:
            if isinstance(value, str):
                return SingleOrMultiString.from_string(value)
            elif isinstance(value, list):
                assert all(isinstance(s, str) for s in value)
                return SingleOrMultiString.from_sequence(value)
        except Exception as error:
            raise ValidationError("Must be a string or list of strings") from error


class FrozenStrMappingField(fields.Dict):
    def _serialize(self, value, attr, obj, **kwargs):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        if isinstance(value, FrozenStrMapping):
            value = dict(value)
        return super()._serialize(value, attr, obj, **kwargs)

    def _deserialize(self, value, attr, data, **kwargs):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        plain_dict = super()._deserialize(value, attr, data, **kwargs)
        return FrozenStrMapping(plain_dict)
