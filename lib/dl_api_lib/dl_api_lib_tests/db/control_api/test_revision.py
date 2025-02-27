from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestControlApiRevisionHistory(DefaultApiTestBase):
    def test_entry_revisions(self, saved_dataset, sync_us_manager):
        usm = sync_us_manager
        us_client = usm._us_client

        resp = us_client.get_entry_revisions(saved_dataset.id)
        assert len(resp) == 1
        assert isinstance(resp[0]["revId"], str)
