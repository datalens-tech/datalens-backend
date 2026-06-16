from dl_api_connector.api_schema.source_base import DataSourceTemplateResponseField
from dl_constants import DataSourceType
from dl_core.us_connection_base import DataSourceTemplate

SOURCE_TYPE = DataSourceType.declare("test_source_base_disabled_text")


def test_response_field_serializes_disabled_text():
    field = DataSourceTemplateResponseField()
    template = {
        "title": "SQL",
        "source_type": SOURCE_TYPE,
        "connection_id": "conn-1",
        "disabled": True,
        "disabled_text": {
            "title": "Adding sources is unavailable",
            "description": "Manual data source creation is disabled in the connection settings.",
        },
        "parameters": {},
        "some_internal_key": "should-be-dropped",
    }

    result = field._serialize(template, None, None)

    assert result is not None
    assert result["disabled"] is True
    assert result["disabled_text"] == {
        "title": "Adding sources is unavailable",
        "description": "Manual data source creation is disabled in the connection settings.",
    }
    assert result["source_type"] == SOURCE_TYPE.name
    assert "some_internal_key" not in result


def test_response_field_serializes_none_disabled_text():
    # A template built directly (e.g. a listed db-table source), not via the freeform
    # helpers, has no availability message. Mirror the real serialization path: the dict is
    # produced by DataSourceTemplate._asdict(), which includes disabled_text=None, and the
    # serializer passes it through as null (consistent with the optional tab_title / form).
    field = DataSourceTemplateResponseField()
    template = DataSourceTemplate(
        title="SQL",
        group=[],
        source_type=SOURCE_TYPE,
        connection_id="conn-1",
        parameters={},
        disabled=False,
    )

    result = field._serialize(dict(template._asdict()), None, None)

    assert result is not None
    assert result["disabled"] is False
    assert result["disabled_text"] is None
