from typing import ClassVar, Mapping, Optional

import attr

from bi_external_api.attrs_model_mapper import Processor
from bi_external_api.attrs_model_mapper.field_processor import FieldMeta
from bi_external_api.domain.internal.dl_common import IntModelTags, EntryScope


@attr.s()
class IDGatheringProcessor(Processor[str]):
    id_map: dict[IntModelTags, set[str]] = attr.ib(factory=lambda: {
        IntModelTags.chart_id: set(),
        IntModelTags.dataset_id: set(),
        IntModelTags.connection_id: set(),
    })

    map_scope_id_tag: ClassVar[Mapping[EntryScope, IntModelTags]] = {
        EntryScope.widget: IntModelTags.chart_id,
        EntryScope.dataset: IntModelTags.dataset_id,
        EntryScope.connection: IntModelTags.connection_id,
    }

    def get_gathered_entry_ids(self, scope: EntryScope) -> frozenset[str]:
        return frozenset(self.id_map[self.map_scope_id_tag[scope]])

    def get_single_optional_interesting_tag(self, meta: FieldMeta) -> Optional[IntModelTags]:
        attrib_descriptor = meta.attrib_descriptor
        if attrib_descriptor is None:
            return None

        intersection = attrib_descriptor.tags & self.id_map.keys()
        if not intersection:
            return None
        assert len(intersection) == 1, f"More than one interesting tag in meta: {meta}"
        interesting_tag = next(iter(intersection))
        assert isinstance(interesting_tag, IntModelTags)

        return interesting_tag

    def _should_process(self, meta: FieldMeta) -> bool:
        may_be_interesting_tag = self.get_single_optional_interesting_tag(meta)
        return may_be_interesting_tag is not None

    def _process_single_object(self, obj: str, meta: FieldMeta) -> Optional[str]:
        tag = self.get_single_optional_interesting_tag(meta)
        assert tag is not None

        if obj is not None:
            assert isinstance(obj, str), f"Got non-string value for field with {tag}: {obj}"
            self.id_map[tag].add(obj)

        return obj
