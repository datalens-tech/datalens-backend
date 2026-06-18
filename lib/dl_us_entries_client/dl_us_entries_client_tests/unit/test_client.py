import pytest
import respx

import dl_us_entries_client


@pytest.mark.asyncio
async def test_get_entry_raises_when_no_permissions_in_response(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("http://us.example.com/v1/entries/dummyid").respond(
        200,
        json={
            "scope": "dash",
            "type": "",
            "key": "test/key",
            "entryId": "dummyid",
            "revId": "revision",
        },
    )
    client = dl_us_entries_client.USEntriesAsyncClient.from_dependencies(
        dl_us_entries_client.USEntriesClientDependencies(
            base_url="http://us.example.com",
        ),
    )
    with pytest.raises(dl_us_entries_client.UsEntriesClientError):
        await client.get_entry(
            dl_us_entries_client.EntryGetRequest(
                entry_id="dummyid",
                include_permissions_info=True,
            )
        )
