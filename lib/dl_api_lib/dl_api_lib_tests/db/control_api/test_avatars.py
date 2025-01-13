from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestAvatars(DefaultApiTestBase):
    def test_delete_avatar_with_formula_fields(self, control_api, saved_dataset):
        ds = saved_dataset
        field_count = len(saved_dataset.result_schema)
        fields = {f"{field.title}_copy": ds.field(formula=f"[{field.title}]") for field in saved_dataset.result_schema}
        for title, field in fields.items():
            ds.result_schema[title] = field
        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(dataset=ds).dataset
        assert len(ds.result_schema) == 2 * field_count

        resp = control_api.apply_updates(
            ds,
            updates=[
                ds.source_avatars["avatar_1"].delete(),
            ],
            fail_ok=True,
        )
        assert resp.status_code == 400, resp.json  # unknown fields in formulas

        # check that only copies are left
        ds = resp.dataset
        assert len(ds.result_schema) == field_count
        assert all(field.title.endswith("_copy") for field in ds.result_schema)
