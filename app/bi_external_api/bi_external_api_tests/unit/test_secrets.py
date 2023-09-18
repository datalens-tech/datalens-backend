from bi_external_api.domain import external as ext

# TODO FIX: BI-3005 test schema creation (projections test)
from bi_external_api.domain.external import get_external_model_mapper
from bi_external_api.enums import ExtAPIType


def test_secret_hide_direct():
    schema = get_external_model_mapper(ExtAPIType.CORE).get_schema_for_attrs_class(ext.Secret)()

    secret = ext.PlainSecret("the_secret_value")

    assert schema.dump(secret) == {"kind": secret.kind.name}


def test_secret_hide_in_wb_create_req():
    schema = get_external_model_mapper(ExtAPIType.CORE).get_schema_for_attrs_class(ext.FakeWorkbookCreateRequest)()

    req = ext.FakeWorkbookCreateRequest(
        workbook_id="fake_id",
        connection_secrets=(ext.ConnectionSecret(conn_name="cn", secret=ext.PlainSecret("the_secret_value")),),
    )

    assert schema.dump(req) == dict(
        workbook_id="fake_id", workbook=None, connection_secrets=[dict(conn_name="cn", secret=dict(kind="plain"))]
    )
