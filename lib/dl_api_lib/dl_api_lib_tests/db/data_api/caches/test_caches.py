from __future__ import annotations

from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.helpers.data_source import data_source_settings_from_table
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_core_testing.database import make_table


class TestDataCaches(DefaultApiTestBase):
    data_caches_enabled = True

    def test_cache_by_deleting_table(self, db, control_api, data_api, saved_connection_id):
        db_table = make_table(db)
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id, **data_source_settings_from_table(db_table)
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds.result_schema["measure"] = ds.field(formula="SUM([int_value])")
        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(ds).dataset

        def get_data():
            result_resp = data_api.get_result(
                dataset=ds,
                fields=[
                    ds.find_field(title="int_value"),
                    ds.find_field(title="measure"),
                ],
            )
            assert result_resp.status_code == 200, result_resp.response_errors
            data = get_data_rows(response=result_resp)
            return data

        data_rows = get_data()

        # Now delete the table.
        # This will make real DB queries impossible,
        # however the cache should still return the same data
        db_table.db.drop_table(db_table.table)

        data_rows_after_deletion = get_data()
        assert data_rows_after_deletion == data_rows
