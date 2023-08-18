from typing import Sequence, Optional

import attr
import more_itertools

from bi_external_api.converter.worbook_list.list_gatherer import WorkbookListGatherer, \
    CollectionContentsFetch, UnpagedCollectionContentsFetch, CollectionUnpagedContentsFetchFactory
from bi_external_api.internal_api_clients.base import CollectionPagedContentsProvider
from bi_external_api.internal_api_clients.models import CollectionInfo, WorkbookInCollectionInfo, CollectionContentsPage


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class MockCollectionContentsFetch(CollectionContentsFetch):
    collection_ids: Sequence[str]
    workbook_ids: Sequence[str]

    @staticmethod
    def setup_mock_search(
            level_contents: tuple[Sequence[str], Sequence[str]]
    ) -> CollectionContentsFetch:
        collection_ids, workbook_ids = level_contents
        return MockCollectionContentsFetch(
            collection_ids=collection_ids,
            workbook_ids=workbook_ids
        )

    async def get_contents(
            self
    ) -> tuple[list[CollectionInfo], list[WorkbookInCollectionInfo]]:
        return [CollectionInfo(id) for id in self.collection_ids], \
               [WorkbookInCollectionInfo(id=id, title=id) for id in self.workbook_ids]


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class MockCollectionUnpagedContentsFetchFactory(CollectionUnpagedContentsFetchFactory):
    tree_levels: dict[Optional[str], tuple[Sequence[str], Sequence[str]]]

    def create_fetch_for_collection_id(
            self,
            collection_id: Optional[str]
    ) -> CollectionContentsFetch:
        return MockCollectionContentsFetch.setup_mock_search(self.tree_levels[collection_id])


def setup_mock_search_factory():
    collection_contents_flattened: dict[Optional[str], tuple[Sequence[str], Sequence[str]]] = {
        None: (["left", "right"], ["top_wb1", "top_wb2", "top_wb3"]),
        "left": ([], ["left_wb1"]),
        "right": (["right_left", "right_right"], ["right_wb1", "right_wb2"]),
        "right_left": ([], ["right_left_wb1"]),
        "right_right": ([], []),
    }
    return MockCollectionUnpagedContentsFetchFactory(tree_levels=collection_contents_flattened)


async def test_list_gatherer():
    gatherer = WorkbookListGatherer(fetch_factory=setup_mock_search_factory())
    workbooks = await gatherer.get_all_workbooks()

    assert workbooks == {
        WorkbookInCollectionInfo(id=wb_id, title=wb_id) for wb_id in
        {"top_wb1", "top_wb2", "top_wb3", "left_wb1", "right_wb1", "right_wb2", "right_left_wb1"}
    }


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class MockCollectionPagedContentsProvider(CollectionPagedContentsProvider):
    collection_ids: Sequence[str]
    workbook_ids: Sequence[str]

    async def get_collection_contents(
            self,
            collection_id: Optional[str],
            collections_page: Optional[str],
            workbooks_page: Optional[str],
            page_size: int
    ) -> CollectionContentsPage:
        collection_ids, collections_next_page_token = self._get_page(self.collection_ids, collections_page, page_size)
        workbook_ids, workbooks_next_page_token = self._get_page(self.workbook_ids, workbooks_page, page_size)
        return CollectionContentsPage(
            collections=[CollectionInfo(collection_id) for collection_id in collection_ids],
            collections_next_page_token=collections_next_page_token,
            workbooks=[WorkbookInCollectionInfo(id=wb_id, title=wb_id) for wb_id in workbook_ids],
            workbooks_next_page_token=workbooks_next_page_token
        )

    def _get_page_number(self, page_str: Optional[str]) -> int:
        if page_str is None:
            return 0
        return int(page_str)

    def _get_page(self, seq: Sequence[str], page_str: Optional[str], page_size: int) -> tuple[Sequence[str], str]:
        seq_chunks = list(more_itertools.chunked(seq, page_size))
        seq_page_num = self._get_page_number(page_str)
        seq_chunk = seq_chunks[seq_page_num]
        has_more_pages = len(seq_chunks) > seq_page_num + 1
        next_page = str(seq_page_num + 1) if has_more_pages else None
        return seq_chunk, next_page

    collection_ids: Sequence[str]
    workbook_ids: Sequence[str]

    @staticmethod
    def setup_mock_search(
            level_contents: tuple[Sequence[str], Sequence[str]]
    ) -> CollectionContentsFetch:
        collection_ids, workbook_ids = level_contents
        return MockCollectionContentsFetch(
            collection_ids=collection_ids,
            workbook_ids=workbook_ids
        )


async def test_unpaged_contents_fetch():
    collection_ids = ["c1"]
    workbook_ids = ["wb1", "wb2", "wb3", "wb4"]
    fetch = UnpagedCollectionContentsFetch(
        collection_paged_contents_provider=MockCollectionPagedContentsProvider(
            collection_ids=collection_ids,
            workbook_ids=workbook_ids,
        ),
        query_page_size=2,
        collection_id=None
    )
    contents = await fetch.get_contents()
    assert contents == (
        [CollectionInfo(collection_id) for collection_id in collection_ids],
        [WorkbookInCollectionInfo(id=wb_id, title=wb_id) for wb_id in workbook_ids],
    )
