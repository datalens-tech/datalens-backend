import os

import pytest

from bi_external_api.attrs_model_mapper import pretty_repr
from bi_external_api.domain import external as ext
from bi_external_api.workbook_ops.public_exceptions import WorkbookOperationException


def get_comma_separated_list_from_env(var_name: str) -> list[str]:
    str_val = os.environ.get(var_name)
    if not str_val:
        return []
    return [
        val.strip() for val in str_val.split(",")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("dash_id", get_comma_separated_list_from_env("EA_ARGS_DASH_ID_LIST"))
async def test_load_dash(bi_ext_api_int_prod_su_wb_ops_facade, dash_id):
    ops_facade = bi_ext_api_int_prod_su_wb_ops_facade
    try:
        rs = await ops_facade.clusterize_workbook(ext.WorkbookClusterizeRequest(dash_id_list=[dash_id]))
    except WorkbookOperationException as wb_op_exc:
        entry_errors = wb_op_exc.data.entry_errors
        common_errors = wb_op_exc.data.common_errors
        print(f"\n\nGot errors during dash {dash_id!r} handling:\n\n")

        for err in entry_errors:
            print(pretty_repr(err))

        for err in common_errors:
            print(pretty_repr(err))
