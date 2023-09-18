import abc
from typing import Optional

from bi_external_api.internal_api_clients.models import CollectionContentsPage


class CollectionPagedContentsProvider(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def get_collection_contents(
        self,
        collection_id: Optional[str],
        collections_page: Optional[str],
        workbooks_page: Optional[str],
        page_size: int,
    ) -> CollectionContentsPage:
        raise NotImplementedError
