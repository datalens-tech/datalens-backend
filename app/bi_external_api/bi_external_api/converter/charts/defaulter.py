from typing import Optional

import attr

from bi_external_api.attrs_model_mapper import Processor
from bi_external_api.attrs_model_mapper.field_processor import FieldMeta
from bi_external_api.domain import external as ext


@attr.s()
class ExtDatasetRefDefaulter(Processor[Optional[str]]):
    _default_dataset_name: str = attr.ib()

    def _should_process(self, meta: FieldMeta) -> bool:
        if not issubclass(meta.clz, str):
            return False

        attrib_descriptor = meta.attrib_descriptor
        if attrib_descriptor is not None:
            return ext.ExtModelTags.dataset_name in attrib_descriptor.tags

        return False

    def _process_single_object(self, obj: Optional[str], meta: FieldMeta) -> Optional[str]:
        if obj is None:
            return self._default_dataset_name
        return obj


@attr.s()
class ExtAggregationDefaulter(Processor[Optional[ext.Aggregation]]):
    _default_aggregation: ext.Aggregation = attr.ib()

    def _should_process(self, meta: FieldMeta) -> bool:
        return issubclass(meta.clz, ext.Aggregation)

    def _process_single_object(self, obj: Optional[ext.Aggregation], meta: FieldMeta) -> Optional[ext.Aggregation]:
        if obj is None:
            return self._default_aggregation
        return obj
