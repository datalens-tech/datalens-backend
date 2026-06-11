import attr

from dl_core.base_models import BaseAttrsDataModel
from dl_core.connection_executors.secret_extraction import data_model_secret_fields
from dl_utils.utils import DataKey


@attr.s
class _ExampleDataModel(BaseAttrsDataModel):
    visible: str = attr.ib(default="")
    credentials: str = attr.ib(default="")

    @classmethod
    def get_secret_keys(cls) -> set[DataKey]:
        return {DataKey(parts=("credentials",))}


@attr.s
class _NotADataModel:
    anything: str = attr.ib(default="")


class TestDataModelSecretFields:
    def test_returns_get_secret_keys_field_names(self) -> None:
        assert data_model_secret_fields(_ExampleDataModel) == frozenset({"credentials"})

    def test_returns_empty_for_non_data_model(self) -> None:
        assert data_model_secret_fields(_NotADataModel) == frozenset()
