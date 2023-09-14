from bi_core.components.dependencies.factory import ComponentDependencyManagerFactory
from bi_core_tests.db.base import DefaultCoreTestClass


class TestAvatarTreeResolution(DefaultCoreTestClass):
    def test_resolve_req_avatars_with_gaps(
        self,
        empty_saved_dataset,
        sync_us_manager,
        conn_default_service_registry,
        dataset_builder_factory,
    ):
        dataset = empty_saved_dataset
        dataset_builder = dataset_builder_factory.get_dataset_builder(dataset=dataset)

        dsrc_1_proxy = dataset_builder.add_data_source()
        dsrc_2_proxy = dataset_builder.add_data_source()
        dsrc_3_proxy = dataset_builder.add_data_source()

        avatar_1_proxy = dsrc_1_proxy.add_avatar()
        avatar_2_proxy = dsrc_2_proxy.add_avatar()
        avatar_3_proxy = dsrc_3_proxy.add_avatar()

        relation_1_2_proxy = avatar_1_proxy.add_relation_simple_eq(
            right=avatar_2_proxy, left_col_name="int_value", right_col_name="int_value"
        )
        relation_2_3_proxy = avatar_2_proxy.add_relation_simple_eq(
            right=avatar_3_proxy, left_col_name="int_value", right_col_name="int_value"
        )

        dep_mgr_factory = ComponentDependencyManagerFactory(dataset=dataset)
        tree_resolver = dep_mgr_factory.get_avatar_tree_resolver()
        root_avatar, required_avatar_ids, required_relation_ids = tree_resolver.expand_required_avatar_ids(
            required_avatar_ids={
                avatar_1_proxy.avatar_id,
                avatar_3_proxy.avatar_id,
            },  # leave a gap in place of avatar_2_id
        )

        assert root_avatar == avatar_1_proxy.avatar_id
        assert required_avatar_ids == {avatar_1_proxy.avatar_id, avatar_2_proxy.avatar_id, avatar_3_proxy.avatar_id}
        assert required_relation_ids == {relation_1_2_proxy.relation_id, relation_2_3_proxy.relation_id}
