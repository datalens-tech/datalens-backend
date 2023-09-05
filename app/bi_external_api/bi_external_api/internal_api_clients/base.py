import abc
from typing import Optional

import attr

from bi_constants.api_constants import DLHeaders, DLHeadersCommon
from bi_api_commons_ya_cloud.models import IAMAuthData
from bi_external_api.internal_api_clients.models import CollectionContentsPage


# TODO FIX: Move to bi-core
@attr.s()
class ExternalIAMAuthData(IAMAuthData):
    def get_headers(self) -> dict[DLHeaders, str]:
        return {
            DLHeadersCommon.AUTHORIZATION_TOKEN: f"Bearer {self.iam_token}"
        }


class CollectionPagedContentsProvider(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def get_collection_contents(
            self,
            collection_id: Optional[str],
            collections_page: Optional[str],
            workbooks_page: Optional[str],
            page_size: int
    ) -> CollectionContentsPage:
        raise NotImplementedError
