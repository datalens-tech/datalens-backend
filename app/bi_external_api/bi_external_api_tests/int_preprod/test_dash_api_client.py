import pytest

from bi_external_api.attrs_model_mapper import pretty_repr
from bi_external_api.domain.internal import dashboards
from bi_testing.utils import skip_outside_devhost


@skip_outside_devhost
@pytest.mark.asyncio
async def test_get_dash(
        bi_ext_api_int_preprod_dash_api_client,
):
    dash_cli = bi_ext_api_int_preprod_dash_api_client
    dash_inst = await dash_cli.get_dashboard("jl54guud894ab")
    print(pretty_repr(dash_inst.dash, preferred_cls_name_prefixes=[dashboards]))
