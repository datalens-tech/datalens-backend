import os

from apispec import APISpec

from bi_external_api.docs.main_dc import DoubleCloudDocsBuilder
from bi_external_api.docs.main_ya_team import YaTeamDocsBuilder
from bi_external_api.domain.external import get_external_model_mapper
from dl_testing.utils import skip_outside_devhost


def test_ext_domain_model_api_spec_generation_posibility(bi_ext_api_types_all):
    spec = APISpec(
        title="DataLens external API",
        version="0.0.0",
        openapi_version="3.0.2",
    )
    amm_schema_registry = get_external_model_mapper(bi_ext_api_types_all).get_amm_schema_registry()

    for comp_id, schema_dict in amm_schema_registry.dump_open_api_schemas().items():
        spec.components.schema(comp_id, schema_dict)

    open_api_schema_dict = spec.to_dict()
    assert open_api_schema_dict is not None


@skip_outside_devhost
def test_md_doc_generation():
    docs = YaTeamDocsBuilder().build()
    dir_path = "/data/docs/external_api_md/"
    os.makedirs(dir_path, exist_ok=True)
    docs.render(dir_path, locale="ru")


@skip_outside_devhost
def test_md_doc_generation_en():
    docs = DoubleCloudDocsBuilder().build()
    dir_path = "/data/docs/external_api_dc_md"
    os.makedirs(dir_path, exist_ok=True)
    docs.render(dir_path, locale="en")
