import itertools
import json

import pytest

from dl_api_connector.form_config.models.base import ConnectionFormMode
from dl_api_lib.connection_forms.registry import CONN_FORM_FACTORY_BY_TYPE
from dl_api_lib.enums import BI_TYPE_AGGREGATIONS
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_constants.enums import (
    AggregationFunction,
    ConnectionType,
    UserDataType,
)

from dl_connector_clickhouse.core.clickhouse.settings import ClickHouseConnectorSettings
from dl_connector_clickhouse.core.clickhouse.us_connection import ConnectionClickhouse
from dl_connector_clickhouse.core.clickhouse_base.constants import CONNECTION_TYPE_CLICKHOUSE
from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from dl_connector_postgresql.core.postgresql.settings import PostgreSQLConnectorSettings


class TestInfo(DefaultApiTestBase):
    @pytest.fixture(scope="class")
    def connectors_settings(self) -> dict[ConnectionType, ConnectorSettingsBase]:
        return {
            CONNECTION_TYPE_CLICKHOUSE: ClickHouseConnectorSettings(),
            CONNECTION_TYPE_POSTGRES: PostgreSQLConnectorSettings(),
        }

    def test_get_field_types_info(self, client):
        resp = client.get("/api/v1/info/field_types")
        assert resp.status_code == 200, resp.json
        resp_data = resp.json

        names = set(item["name"] for item in resp_data["types"])
        expected = set(
            item.name
            for item in BI_TYPE_AGGREGATIONS
            if item
            not in (
                UserDataType.uuid,
                UserDataType.markup,
                UserDataType.datetimetz,
                UserDataType.datetime,
                UserDataType.unsupported,
            )
        )
        assert names == expected

        for api_record in resp_data["types"]:
            aggs = set(AggregationFunction[x] for x in api_record["aggregations"])
            expected = set(BI_TYPE_AGGREGATIONS[UserDataType[api_record["name"]]])
            assert aggs == expected

    def test_get_connectors(self, client):
        resp = client.get("/api/v1/info/connectors")
        assert resp.status_code == 200, resp.json
        connector_info = resp.json

        uncategorized = connector_info.get("uncategorized", [])
        assert uncategorized
        sections = connector_info.get("sections", [])
        assert sections
        assert all(
            isinstance(section["title"], str) and not section["title"].startswith("section_title-")
            for section in sections
        )

        all_connectors = itertools.chain(
            uncategorized,
            itertools.chain.from_iterable(
                connector["includes"] if "includes" in connector else (connector,)  # type: ignore
                for connector in itertools.chain.from_iterable(section["connectors"] for section in sections)
            ),
        )

        assert all(
            conn_info["backend_driven_form"] == (ConnectionType(conn_info["conn_type"]) in CONN_FORM_FACTORY_BY_TYPE)
            for conn_info in all_connectors
        )

    @pytest.mark.parametrize("mode_name", [mode.name for mode in ConnectionFormMode])
    @pytest.mark.parametrize("conn_type_name", [conn_type.name for conn_type in ConnectionType])
    def test_get_connector_form(self, client, conn_type_name, mode_name):
        """There is not much we can test, other than the fact that all forms can be built successfully"""

        form_resp = client.get(f"/api/v1/info/connectors/forms/{conn_type_name}/{mode_name}")
        assert form_resp.status_code == 200
        assert "form" in form_resp.json

    def test_get_connector_form_bad_request(self, client):
        form_resp = client.get(f"/api/v1/info/connectors/forms/bad_conn_type/{ConnectionFormMode.create.name}")
        assert form_resp.status_code == 400, form_resp.json

        form_resp = client.get(f"/api/v1/info/connectors/forms/{CONNECTION_TYPE_CLICKHOUSE.name}/bad_form_mode")
        assert form_resp.status_code == 400, form_resp.json

    def test_get_connector_icons(self, client):
        icons_resp = client.get("/api/v1/info/connectors/icons")
        assert icons_resp.status_code == 200, icons_resp.json

        resp_data = icons_resp.json
        assert resp_data["icons"]
        assert len(resp_data["icons"]) > 0
        for icon in resp_data["icons"]:
            assert "type" in icon
            assert icon["type"] == "data"
            assert "data" in icon
            assert len(icon["data"].get("standard", "")) > 0, icon
            assert len(icon["data"].get("nav", "")) > 0, icon

    @pytest.mark.parametrize(
        "conn_type_name",
        [conn_type.name for conn_type in ConnectionType if conn_type != ConnectionType.unknown],
    )
    def test_get_connector_icon(self, client, conn_type_name):
        icons_resp = client.get(f"/api/v1/info/connectors/icons/{conn_type_name}")
        assert icons_resp.status_code == 200, icons_resp.json

        resp_data = icons_resp.json
        assert "icon" in resp_data
        icon_data = resp_data["icon"]
        assert "type" in icon_data
        assert icon_data["type"] == "data"
        assert "data" in icon_data
        assert len(icon_data["data"].get("standard", "")) > 0, icon_data
        assert len(icon_data["data"].get("nav", "")) > 0, icon_data

    def test_get_connector_icon_not_found(self, client):
        icons_resp = client.get("/api/v1/info/connectors/icons/unknown_conn_type")
        assert icons_resp.status_code == 404, icons_resp.json

    def test_public_usage_checker(self, client, saved_dataset, saved_connection_id):
        data = dict(datasets=[saved_dataset.id])
        response = client.post(
            "/api/v1/info/datasets_publicity_checker",
            content_type="application/json",
            data=json.dumps(data),
        )
        expected_resp = [
            dict(
                reason=None,
                dataset_id=saved_dataset.id,
                allowed=True,
            )
        ]

        assert response.status_code == 200
        assert response.json["result"] == expected_resp

        data = dict(connections=[saved_connection_id])
        response = client.post(
            "/api/v1/info/connections_publicity_checker",
            content_type="application/json",
            data=json.dumps(data),
        )
        expected_resp = [
            dict(
                reason=None,
                connection_id=saved_connection_id,
                allowed=True,
            )
        ]
        assert response.status_code == 200
        assert response.json["result"] == expected_resp

    def test_datasets_publicity_checker_connection_not_allowed(
        self, client, monkeypatch, saved_dataset, saved_connection_id
    ):
        monkeypatch.setattr(ConnectionClickhouse, "allow_public_usage", False)
        data = dict(datasets=[saved_dataset.id])
        response = client.post(
            "/api/v1/info/datasets_publicity_checker",
            content_type="application/json",
            data=json.dumps(data),
        )
        expected_resp = [
            dict(
                reason=f"The publication of this object or some of its dependencies is not allowed. "
                f"Connections of type clickhouse are not available for publication (connection ID: {saved_connection_id})",
                dataset_id=saved_dataset.id,
                allowed=False,
            )
        ]

        assert response.status_code == 200
        assert response.json["result"] == expected_resp

    def test_dataset_publicity_checker(self, client, dataset_id):
        resp = client.post(
            "/api/v1/info/datasets_publicity_checker",
            content_type="application/json",
            data=json.dumps(dict(datasets=[dataset_id])),
        )
        assert resp.status_code == 200
        resp_data = resp.json
        assert "result" in resp_data
        assert len(resp_data["result"]) == 1 and resp_data["result"][0]["allowed"] == True, resp_data
