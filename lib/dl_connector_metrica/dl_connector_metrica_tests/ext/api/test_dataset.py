import abc
from copy import deepcopy
from typing import ClassVar

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_testing.connector.dataset_suite import DefaultConnectorDatasetTestSuite
from dl_constants.enums import (
    AggregationFunction,
    FieldType,
    UserDataType,
)
from dl_sqlalchemy_metrica_api.api_info.appmetrica.installs import installs_fields
from dl_sqlalchemy_metrica_api.api_info.metrika.hits import hits_fields

from dl_connector_metrica_tests.ext.api.base import (
    AppMetricaDatasetTestBase,
    MetricaDatasetTestBase,
)


class MetricaDatasetChecker(DefaultConnectorDatasetTestSuite, metaclass=abc.ABCMeta):
    expected_fields: ClassVar[dict]

    def check_basic_dataset(self, ds: Dataset, annotation: dict) -> None:
        assert ds.id
        assert len(ds.result_schema)
        assert all(field.aggregation == AggregationFunction.none for field in ds.result_schema)

        fields = [
            {
                "is_dim": field.type == FieldType.DIMENSION,
                "name": field.source,
                "title": field.title,
                "type": (
                    UserDataType.datetime.name
                    if field.initial_data_type == UserDataType.genericdatetime
                    else field.initial_data_type.name
                ),
            }
            for field in ds.result_schema
        ]

        expected = deepcopy(self.expected_fields)
        for elem in expected:
            elem.pop("description")
            elem.pop("src_key", None)
        assert fields == expected

        assert ds.annotation == annotation


class TestMetricaDataset(MetricaDatasetTestBase, MetricaDatasetChecker):
    expected_fields = hits_fields

    def test_add_field_to_dataset(
        self,
        saved_dataset: Dataset,
        control_api: SyncHttpDatasetApiV1,
    ) -> None:
        ds = saved_dataset

        ds.result_schema["new field"] = ds.field(
            avatar_id=ds.source_avatars[0].id,
            source="ym:pv:pageviewsPerMinute",
            aggregation=AggregationFunction.none,
            title="new field",
        )
        ds_resp = control_api.apply_updates(dataset=ds)
        resp_result_schema = ds_resp.json["dataset"]["result_schema"]
        assert resp_result_schema[0]["title"] == "new field"
        assert resp_result_schema[0]["type"] == FieldType.MEASURE.name

    def test_concat_validation(
        self,
        saved_dataset: Dataset,
        control_api: SyncHttpDatasetApiV1,
    ) -> None:
        ds = saved_dataset

        ds.result_schema["Test"] = ds.field(formula='CONCAT("TEST1 ", "test2")')
        ds_resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert ds_resp.status_code == 400, ds_resp.json
        assert ds_resp.json["code"] == "ERR.DS_API.VALIDATION.ERROR"


class TestAppMetricaDataset(AppMetricaDatasetTestBase, MetricaDatasetChecker):
    expected_fields = installs_fields
