from __future__ import annotations

from typing import NoReturn

from dl_core.united_storage_client import (
    USAuthContextNoAuth,
    UStorageClient,
    UStorageClientBase,
)


# TODO FIX: Remove it
class FakeUSClient(UStorageClient):
    """
    This class is used by async US manager to prevent saving/updating entities with their BaseModel.*() methods
    """

    def __init__(self) -> None:
        super().__init__(host="http://127.0.0.1:3030", auth_ctx=USAuthContextNoAuth())

    def _request(self, request_data: UStorageClientBase.RequestData) -> NoReturn:
        raise NotImplementedError("US entries created by async manager can not communicate with US directly")
