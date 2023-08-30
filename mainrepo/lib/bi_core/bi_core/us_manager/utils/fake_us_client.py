from __future__ import annotations

from bi_core.united_storage_client import UStorageClient, UStorageClientBase, USAuthContextNoAuth


# TODO FIX: Remove it
class FakeUSClient(UStorageClient):
    """
    This class is used by async US manager to prevent saving/updating entities with their BaseModel.*() methods
    """

    def __init__(self):  # type: ignore  # TODO: fix
        super().__init__(
            host="http://127.0.0.1:3030",
            auth_ctx=USAuthContextNoAuth()
        )

    def _request(self, request_data: UStorageClientBase.RequestData):  # type: ignore  # TODO: fix
        raise NotImplementedError("US entries created by async manager can not communicate with US directly")
