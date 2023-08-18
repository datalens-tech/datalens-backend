from typing import Sequence, Optional

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.domain.utils import ensure_tuple
from .avatars import Avatar
from .data_source import DataSource
from .errors import AllComponentErrors
from .fields import ResultSchemaFieldFull
from ..dl_common import DatasetAPIBaseModel, EntryInstance, EntryScope


@ModelDescriptor()
@attr.s(frozen=True)
class Dataset(DatasetAPIBaseModel):
    sources: Sequence[DataSource] = attr.ib(converter=ensure_tuple)
    source_avatars: Sequence[Avatar] = attr.ib(converter=ensure_tuple)
    result_schema: Sequence[ResultSchemaFieldFull] = attr.ib(converter=ensure_tuple)
    component_errors: AllComponentErrors = attr.ib(factory=AllComponentErrors)

    def get_field_by_id(self, id: str) -> ResultSchemaFieldFull:
        return next(f for f in self.result_schema if f.guid == id)

    def get_field_by_title(self, title: str) -> ResultSchemaFieldFull:
        return next(f for f in self.result_schema if f.title == title)

    ignored_keys = {
        'rls',
        'obligatory_filters',
        'source_features',
        'preview_enabled',
        'avatar_relations',
        'result_schema_aux',
        'revision_id',
    }


@attr.s()
class DatasetInstance(EntryInstance):
    SCOPE = EntryScope.dataset

    dataset: Dataset = attr.ib()
    # Field extracted to instance from dataset config
    # to keep Dataset clean of non-user controlled data
    # and to simplify comparison
    backend_local_revision_id: Optional[str] = attr.ib(default=None, hash=False, cmp=False)
