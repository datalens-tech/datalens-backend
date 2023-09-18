import abc
from typing import Optional

import attr

from bi_external_api.internal_api_clients.base import CollectionPagedContentsProvider
from bi_external_api.internal_api_clients.models import (
    CollectionInfo,
    WorkbookInCollectionInfo,
)


class CollectionContentsFetch(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def get_contents(self) -> tuple[list[CollectionInfo], list[WorkbookInCollectionInfo]]:
        raise NotImplementedError


@attr.s(auto_attribs=True, kw_only=True)
class UnpagedCollectionContentsFetch(CollectionContentsFetch):
    """A stateful object to fetch paginated datalens collection contents and return them unpaged.
    To avoid state pollution a new fetch object should be created per one fetch.
    """

    collection_paged_contents_provider: CollectionPagedContentsProvider
    query_page_size: int
    collection_id: Optional[str]

    _collections_exhausted: bool = attr.ib(init=False, default=False)
    _workbooks_exhausted: bool = attr.ib(init=False, default=False)
    _collections: list[CollectionInfo] = attr.ib(init=False, factory=list)
    _workbooks: list[WorkbookInCollectionInfo] = attr.ib(init=False, factory=list)
    _collection_page_token: Optional[str] = attr.ib(init=False, default=None)
    _workbook_page_token: Optional[str] = attr.ib(init=False, default=None)

    async def get_contents(self) -> tuple[list[CollectionInfo], list[WorkbookInCollectionInfo]]:
        while not self._collections_exhausted or not self._workbooks_exhausted:
            await self._consume_next_page()
        return self._collections, self._workbooks

    async def _consume_next_page(self) -> None:
        contents = await self.collection_paged_contents_provider.get_collection_contents(
            collection_id=self.collection_id,
            collections_page=self._collection_page_token,
            workbooks_page=self._workbook_page_token,
            page_size=self.query_page_size,
        )
        if not self._collections_exhausted:
            self._collections += contents.collections
        if not self._workbooks_exhausted:
            self._workbooks += contents.workbooks
        self._collections_exhausted = contents.collections_next_page_token is None
        self._workbooks_exhausted = contents.workbooks_next_page_token is None
        self._collection_page_token = contents.collections_next_page_token
        self._workbook_page_token = contents.workbooks_next_page_token


class CollectionUnpagedContentsFetchFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def create_fetch_for_collection_id(self, collection_id: Optional[str]) -> CollectionContentsFetch:
        raise NotImplementedError


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class DefaultCollectionUnpagedContentsFetchFactory(CollectionUnpagedContentsFetchFactory):
    collection_paged_contents_provider: CollectionPagedContentsProvider
    query_page_size: int

    def create_fetch_for_collection_id(self, collection_id: Optional[str]) -> CollectionContentsFetch:
        return UnpagedCollectionContentsFetch(
            collection_paged_contents_provider=self.collection_paged_contents_provider,
            query_page_size=self.query_page_size,
            collection_id=collection_id,
        )


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class WorkbookListGatherer:
    fetch_factory: CollectionUnpagedContentsFetchFactory

    async def get_all_workbooks(self) -> set[WorkbookInCollectionInfo]:
        return set(await self._get_workbooks_in_collection(None))

    async def _get_workbooks_in_collection(self, collection_id: Optional[str]) -> list[WorkbookInCollectionInfo]:
        found_workbooks: list[WorkbookInCollectionInfo] = []
        search = self.fetch_factory.create_fetch_for_collection_id(collection_id)
        child_collections, workbooks_in_collection = await search.get_contents()
        found_workbooks += workbooks_in_collection
        for c in child_collections:
            found_workbooks += await self._get_workbooks_in_collection(c.collection_id)
        return found_workbooks
