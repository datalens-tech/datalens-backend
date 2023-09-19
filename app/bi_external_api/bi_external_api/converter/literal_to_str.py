from typing import (
    ClassVar,
    Generic,
    Type,
    TypeVar,
)

from bi_external_api.converter.converter_exc import (
    ConstraintViolationError,
    NotSupportedYet,
)
from bi_external_api.domain.internal import datasets
from dl_constants.enums import BIType


_DEFAULT_VALUE_TV = TypeVar("_DEFAULT_VALUE_TV", bound=datasets.DefaultValue)
_DEFAULT_VALUE_CODEC_TV = TypeVar("_DEFAULT_VALUE_CODEC_TV", bound="DefaultValueToStringCodec")


class DefaultValueToStringCodec(Generic[_DEFAULT_VALUE_TV]):
    DEFAULT_VALUE_CLS: ClassVar[Type[_DEFAULT_VALUE_TV]]

    @classmethod
    def decode(cls, s: str) -> _DEFAULT_VALUE_TV:
        raise NotImplementedError()

    @classmethod
    def encode(cls, dp: _DEFAULT_VALUE_TV) -> str:
        raise NotImplementedError()


class DefaultValueToStringCodecString(DefaultValueToStringCodec[datasets.DefaultValueString]):
    DEFAULT_VALUE_CLS = datasets.DefaultValueString

    @classmethod
    def decode(cls, s: str) -> datasets.DefaultValueString:
        return datasets.DefaultValueString(s)

    @classmethod
    def encode(cls, dp: datasets.DefaultValueString) -> str:
        return dp.value


class DefaultValueToStringCodecInteger(DefaultValueToStringCodec[datasets.DefaultValueInteger]):
    DEFAULT_VALUE_CLS = datasets.DefaultValueInteger

    @classmethod
    def decode(cls, s: str) -> datasets.DefaultValueInteger:
        try:
            int_val = int(s)
        except Exception:
            raise ConstraintViolationError(f"not an valid integer: {s}")
        else:
            return datasets.DefaultValueInteger(int_val)

    @classmethod
    def encode(cls, dp: datasets.DefaultValueInteger) -> str:
        return str(dp.value)


class DefaultValueToStringCodecFloat(DefaultValueToStringCodec[datasets.DefaultValueFloat]):
    DEFAULT_VALUE_CLS = datasets.DefaultValueFloat

    @classmethod
    def decode(cls, s: str) -> datasets.DefaultValueFloat:
        try:
            int_val = float(s)
        except Exception:
            raise ConstraintViolationError(f"not a valid float: {s}")
        else:
            return datasets.DefaultValueFloat(int_val)

    @classmethod
    def encode(cls, dp: datasets.DefaultValueFloat) -> str:
        return str(dp.value)


class DefaultValueConverterExtStringValue:
    all_codecs: ClassVar[tuple[Type[DefaultValueToStringCodec], ...]] = (
        DefaultValueToStringCodecString,
        DefaultValueToStringCodecInteger,
        DefaultValueToStringCodecFloat,
    )
    map_bi_type_to_codec_cls: ClassVar[dict[BIType, Type[DefaultValueToStringCodec]]] = {
        codec_cls.DEFAULT_VALUE_CLS.type: codec_cls for codec_cls in all_codecs
    }

    @classmethod
    def _make_not_supported_exc(cls, bi_type: BIType) -> NotSupportedYet:
        return NotSupportedYet(f"dataset parameters with type: {bi_type.name}")

    @classmethod
    def convert_ext_to_int(cls, bi_type: BIType, ext_value: str) -> datasets.DefaultValue:
        try:
            codec_cls: Type[DefaultValueToStringCodec] = cls.map_bi_type_to_codec_cls[bi_type]
        except KeyError:
            raise cls._make_not_supported_exc(bi_type)
        else:
            decoded_default_value = codec_cls.decode(ext_value)
            assert decoded_default_value.type is bi_type
            return decoded_default_value

    @classmethod
    def convert_int_to_ext(cls, default_value: datasets.DefaultValue) -> str:
        bi_type = default_value.type

        try:
            codec_cls: Type[DefaultValueToStringCodec] = cls.map_bi_type_to_codec_cls[default_value.type]
        except KeyError:
            raise cls._make_not_supported_exc(bi_type)

        return codec_cls.encode(default_value)
