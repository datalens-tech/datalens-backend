from typing import Sequence

import attr

from bi_external_api.converter.worbook_list.list_gatherer import WorkbookListGatherer, \
    DefaultCollectionUnpagedContentsFetchFactory
from bi_external_api.domain.external.workbook import WorkbookIndexItem
from bi_external_api.internal_api_clients.united_storage import MiniUSClient

DEFAULT_US_QUERY_PAGE_SIZE: int = 1000


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class USWorkbookListGatherer:
    us_client: MiniUSClient

    async def gather_workbooks(self) -> Sequence[WorkbookIndexItem]:
        workbooks = await WorkbookListGatherer(
            fetch_factory=DefaultCollectionUnpagedContentsFetchFactory(
                collection_paged_contents_provider=self.us_client,
                query_page_size=DEFAULT_US_QUERY_PAGE_SIZE
            )
        ).get_all_workbooks()

        return [WorkbookIndexItem(id=wb.id, title=wb.title) for wb in workbooks]
