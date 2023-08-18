from typing import Sequence, Optional

import attr

from bi_external_api.domain.internal import datasets
from bi_external_api.domain.internal.dl_common import EntrySummary


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class WorkbookBackendTOC:
    connections: frozenset[datasets.BIConnectionSummary]
    datasets: frozenset[EntrySummary]
    charts: frozenset[EntrySummary]
    dashboards: frozenset[EntrySummary]


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class WorkbookInCollectionInfo:
    id: str
    title: str


@attr.s(frozen=True, auto_attribs=True)
class CollectionInfo:
    collection_id: str


@attr.s(frozen=True, auto_attribs=True)
class CollectionContentsPage:
    collections: Sequence[CollectionInfo]
    collections_next_page_token: Optional[str]
    workbooks: Sequence[WorkbookInCollectionInfo]
    workbooks_next_page_token: Optional[str]


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class WorkbookBasicInfo:
    id: str
    title: str
    project_id: str
