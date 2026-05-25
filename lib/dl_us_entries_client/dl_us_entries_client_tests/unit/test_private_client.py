import pytest
import respx

import dl_httpx
import dl_us_entries_client


@pytest.mark.asyncio
async def test_get_entry_raises_when_no_permissions_in_response(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("http://us.example.com/private/entries/dummyid").respond(
        200,
        json={
            "scope": "dash",
            "type": "",
            "key": "test/key",
            "entryId": "dummyid",
        },
    )
    client = dl_us_entries_client.USEntriesPrivateAsyncClient.from_dependencies(
        dl_httpx.HttpxClientDependencies(
            base_url="http://us.example.com",
        ),
    )
    with pytest.raises(dl_us_entries_client.UsEntriesClientException):
        await client.get_entry(
            dl_us_entries_client.PrivateEntryGetRequest(
                entry_id="dummyid",
                include_permissions_info=True,
            )
        )
