import pytest

from dl_constants.enums import (
    ExtractMode,
    ExtractStatus,
    OrderDirection,
    WhereClauseOperation,
)
from dl_core.base_models import DefaultWhereClause
from dl_core.components.editor import DatasetComponentEditor
from dl_core.fields import (
    FilterField,
    OrderField,
)
from dl_core.us_dataset import Dataset as Dataset
from dl_core.us_extract import ExtractProperties
from dl_core.us_manager.us_manager_sync import SyncUSManager
from dl_core.us_manager.us_manager_sync_mock import MockedSyncUSManager
from dl_core_tests.db.base import DefaultCoreTestClass


class TestExtractValidationMocked(DefaultCoreTestClass):
    @pytest.fixture(scope="function")
    def sync_us_manager(self) -> MockedSyncUSManager:
        return MockedSyncUSManager()

    def test_load_of_old_dataset_without_extract_settings(
        self,
        empty_saved_dataset: Dataset,
        sync_us_manager: MockedSyncUSManager,
    ):
        sync_us_manager.save(empty_saved_dataset)

        # Simulate dataset without extract properties
        del sync_us_manager._us_client._saved_entries[empty_saved_dataset.uuid]["data"]["extract"]
        del sync_us_manager._us_client._saved_entries[empty_saved_dataset.uuid]["unversionedData"]["extract"]

        loaded_dataset = sync_us_manager.get_by_id(empty_saved_dataset.uuid, Dataset)

        assert loaded_dataset.data.extract == ExtractProperties(
            mode=ExtractMode.disabled,
            status=ExtractStatus.disabled,
            filters=[],
            sorting=[],
            errors=[],
            last_update=0,
        )


class TestExtractValidation(DefaultCoreTestClass):
    def test_extract_settings_persistence_in_us(
        self,
        saved_dataset: Dataset,
        sync_us_manager: SyncUSManager,
    ):
        ds_editor = DatasetComponentEditor(dataset=saved_dataset)

        # Fill dataset with extract config
        ds_editor.set_extract_filters(
            extract_filters=[
                FilterField(
                    id="id_1",
                    default_filters=[
                        DefaultWhereClause(
                            operation=WhereClauseOperation.ENDSWITH,
                            values=[
                                "hehe",
                            ],
                        ),
                    ],
                    valid=True,
                    guid="guid_1",
                ),
                FilterField(
                    id="id_2",
                    default_filters=[
                        DefaultWhereClause(
                            operation=WhereClauseOperation.EQ,
                            values=[
                                "not hehe",
                            ],
                        ),
                    ],
                    valid=False,
                    guid="guid_2",
                ),
            ],
        )
        ds_editor.set_extract_sorting(
            extract_sorting=[
                OrderField(
                    id="id_3",
                    valid=True,
                    guid="guid_3",
                    order=OrderDirection.desc,
                ),
                OrderField(
                    id="id_4",
                    valid=False,
                    guid="guid_4",
                    order=OrderDirection.asc,
                ),
            ],
        )
        ds_editor.set_extract_mode(
            extract_mode=ExtractMode.automatic,
        )
        ds_editor.set_extract_errors(
            extract_errors=["potato"],
        )
        ds_editor.set_extract_last_update(
            extract_last_update=1234567,
        )
        ds_editor.set_extract_status(
            extract_status=ExtractStatus.empty,
        )

        sync_us_manager.save(saved_dataset)

        # Check that config persisted
        loaded_dataset = sync_us_manager.get_by_id(saved_dataset.uuid, Dataset)

        # Check that config persisted correctly
        assert loaded_dataset.data.extract == ExtractProperties(
            mode=ExtractMode.automatic,
            status=ExtractStatus.empty,
            filters=[
                FilterField(
                    id="id_1",
                    guid="guid_1",
                    default_filters=[
                        DefaultWhereClause(
                            operation=WhereClauseOperation.ENDSWITH,
                            values=["hehe"],
                        ),
                    ],
                    valid=True,
                ),
                FilterField(
                    id="id_2",
                    guid="guid_2",
                    default_filters=[
                        DefaultWhereClause(
                            operation=WhereClauseOperation.EQ,
                            values=["not hehe"],
                        ),
                    ],
                    valid=False,
                ),
            ],
            sorting=[
                OrderField(
                    id="id_3",
                    guid="guid_3",
                    order=OrderDirection.desc,
                    valid=True,
                ),
                OrderField(
                    id="id_4",
                    guid="guid_4",
                    order=OrderDirection.asc,
                    valid=False,
                ),
            ],
            errors=["potato"],
            last_update=1234567,
        )
