import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib.enums import DatasetSettingName
from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestSettings(DefaultApiTestBase):
    @pytest.mark.parametrize(
        "setting_name",
        DatasetSettingName,
    )
    @pytest.mark.parametrize("setting_value", [True, False])
    def test_change_settings(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        setting_name: DatasetSettingName,
        setting_value: bool,
    ):
        saved_dataset = control_api.update_setting(saved_dataset, setting_name.value, setting_value).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        assert getattr(saved_dataset, setting_name.value) == setting_value, "Setting should be updated"
