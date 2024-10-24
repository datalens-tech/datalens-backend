import pytest

from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_testing.api_base import DefaultApiTestBase


class TestDatasetJoinOptimization(DefaultApiTestBase):
    @pytest.fixture(scope="function")
    def dataset_for_join_optimization_tests(self, control_api, saved_connection_id, dataset_params):
        ds = Dataset()

        # Create 3 identical sources & corresponding avatars
        for source_idx in (1, 2, 3):
            ds.sources[f"source_{source_idx}"] = ds.source(
                connection_id=saved_connection_id,
                **dataset_params,
            )
            ds.source_avatars[f"avatar_{source_idx}"] = ds.sources[f"source_{source_idx}"].avatar()

        # Create relations
        ds.avatar_relations["relation_1"] = (
            ds.source_avatars["avatar_1"]
            .join(ds.source_avatars["avatar_2"])
            .on(
                ds.col("customer_id") == ds.col("customer_id"),
            )
        )
        ds.avatar_relations["relation_2"] = (
            ds.source_avatars["avatar_1"]
            .join(ds.source_avatars["avatar_3"])
            .on(
                ds.col("customer_id") == ds.col("customer_id"),
            )
        )

        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset

        # Resulting avatar tree:
        # avatar_1 ── avatar_2
        # │
        # └── avatar_3
        # Fields associated with avatar_2 have a suffix " (1)", same for avatar_3, no suffix for avatar_1

        yield ds

        control_api.delete_dataset(ds.id)

    def test_join_optimization_no_required(self, data_api, dataset_for_join_optimization_tests):
        """Selecting from a dataset without any required relations and making sure there are no JOINs"""

        ds = dataset_for_join_optimization_tests

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="customer_id (1)"),  # Requesting a field associated with avatar_2
            ],
        )
        assert result_resp.status_code == 200, result_resp.json
        sent_query: str = result_resp.json["blocks"][0]["query"]
        joins_count = sent_query.count("JOIN")
        assert joins_count == 0, sent_query  # 1 source is used

    def test_join_optimization_implicit_required(self, data_api, dataset_for_join_optimization_tests):
        """Selecting from 2 sources that are not connected by a direct relation and making sure all necessary JOINs are performed"""

        ds = dataset_for_join_optimization_tests

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="customer_id (1)"),  # Requesting a field associated with avatar_2
                ds.find_field(title="customer_id (2)"),  # Requesting a field associated with avatar_3
            ],
        )
        assert result_resp.status_code == 200, result_resp.json
        sent_query: str = result_resp.json["blocks"][0]["query"]
        joins_count = sent_query.count("JOIN")
        assert joins_count == 2, sent_query  # all 3 sources are used

    def test_join_optimization_required_neighbour(
        self,
        control_api,
        data_api,
        dataset_for_join_optimization_tests,
    ):
        """Selecting from a source that has one required relation and making sure this required JOIN is performed"""

        ds = dataset_for_join_optimization_tests
        ds.avatar_relations["relation_2"].require()

        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset

        # Resulting avatar tree (the relation 1 -> 3 is required)
        # avatar_1 ─── avatar_2
        # ┃
        # ┗━━ avatar_3

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="customer_id"),  # Requesting a field associated with avatar_1
            ],
        )
        assert result_resp.status_code == 200, result_resp.json
        sent_query: str = result_resp.json["blocks"][0]["query"]
        joins_count = sent_query.count("JOIN")
        assert joins_count == 1, sent_query  # 2 sources are used

    def test_join_optimization_required_other(
        self,
        control_api,
        data_api,
        dataset_for_join_optimization_tests,
    ):
        """Selecting from a source that has no required relations and making sure all required JOINs are performed"""

        ds = dataset_for_join_optimization_tests
        ds.avatar_relations["relation_2"].require()

        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset

        # Resulting avatar tree (the relation 1 -> 3 is required)
        # avatar_1 ─── avatar_2
        # ┃
        # ┗━━ avatar_3

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="customer_id (1)"),  # Requesting a field associated with avatar_2
            ],
        )
        assert result_resp.status_code == 200, result_resp.json
        sent_query: str = result_resp.json["blocks"][0]["query"]
        joins_count = sent_query.count("JOIN")
        assert joins_count == 2, sent_query  # all 3 sources are used
