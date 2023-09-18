import abc
import enum
from typing import (
    Mapping,
    Optional,
)

import attr

from bi_external_api.attrs_model_mapper import Processor
from bi_external_api.attrs_model_mapper.field_processor import FieldMeta


@attr.s(frozen=True)
class TaggedStringAttrReplacerProcessor(Processor, metaclass=abc.ABCMeta):
    string_replacement_mapping: Mapping[str, str] = attr.ib()
    tag: enum.Enum = attr.ib()

    def _should_process(self, meta: FieldMeta) -> bool:
        attrib_descriptor = meta.attrib_descriptor
        if attrib_descriptor is not None:
            return self.tag in attrib_descriptor.tags and issubclass(meta.clz, str)
        return False

    def _process_single_object(self, obj: Optional[str], meta: FieldMeta) -> Optional[str]:
        if obj is None:
            return None
        assert isinstance(obj, str)

        if obj in self.string_replacement_mapping:
            return self.string_replacement_mapping[obj]

        return obj


@attr.s(frozen=True)
class TaggedStringAttrSetter(Processor, metaclass=abc.ABCMeta):
    tag: enum.Enum = attr.ib()
    value_to_set: str = attr.ib()

    def _should_process(self, meta: FieldMeta) -> bool:
        attrib_descriptor = meta.attrib_descriptor
        if attrib_descriptor is not None:
            return self.tag in attrib_descriptor.tags and issubclass(meta.clz, str)
        return False

    def _process_single_object(self, obj: str, meta: FieldMeta) -> Optional[str]:
        assert isinstance(obj, str)
        return self.value_to_set
