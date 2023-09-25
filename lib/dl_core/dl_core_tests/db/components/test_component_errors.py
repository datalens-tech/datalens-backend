import uuid

from dl_constants.enums import ComponentType
from dl_core_tests.db.base import DefaultCoreTestClass


class TestComponentErrors(DefaultCoreTestClass):
    def test_component_errors(self, sync_us_manager, saved_dataset):
        dataset = saved_dataset
        source_id = dataset.get_single_data_source_id()
        erreg = dataset.error_registry
        erreg.add_error(id=source_id, type=ComponentType.data_source, message="This is an error", code=["ERR", "1"])
        assert len(erreg.get_pack(id=source_id).errors) == 1
        erreg.add_error(id=source_id, type=ComponentType.data_source, message="This is the end", code=["ERR", "2"])
        assert len(erreg.get_pack(id=source_id).errors) == 2

        field_id = "111111"
        erreg.add_error(
            id=field_id,
            type=ComponentType.field,
            message="This field needs ploughing",
            code=["ERR", "3"],
            details=dict(row=10, column=8),
        )
        erreg.add_error(
            id=field_id,
            type=ComponentType.field,
            message="Just an error",
            code=["ERR", "3", "1"],
            details=dict(row=100, column=80),
        )

        obligatory_filter_id = str(uuid.uuid4())
        erreg.add_error(
            id=obligatory_filter_id,
            type=ComponentType.obligatory_filter,
            message="obligatory_filter test err",
            code=["ERR", "OBLIG_FILT", "TEST"],
        )

        sync_us_manager.save(dataset)
        dataset = sync_us_manager.get_by_id(entry_id=dataset.uuid)

        erreg = dataset.error_registry
        field_errors = erreg.get_pack(id=field_id).errors
        assert len(field_errors) == 2
        assert field_errors[0].code == ["ERR", "3"]
        assert field_errors[0].details["row"] == 10
        assert field_errors[0].details["column"] == 8

        oblig_filter_errors = erreg.for_type(type=ComponentType.obligatory_filter)
        assert len(oblig_filter_errors) == 1
        assert oblig_filter_errors[0].id == obligatory_filter_id
        assert len(oblig_filter_errors[0].errors) == 1
        assert oblig_filter_errors[0].errors[0].message == "obligatory_filter test err"

        erreg.remove_errors(id=field_id, code=["ERR", "3"])

        sync_us_manager.save(dataset)
        dataset = sync_us_manager.get_by_id(entry_id=dataset.uuid)

        erreg = dataset.error_registry
        assert len(erreg.get_pack(id=field_id).errors) == 1

        erreg.remove_errors(id=field_id, code_prefix=["ERR", "3"])

        sync_us_manager.save(dataset)
        dataset = sync_us_manager.get_by_id(entry_id=dataset.uuid)

        erreg = dataset.error_registry
        assert erreg.get_pack(id=field_id) is None
