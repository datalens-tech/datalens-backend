from typing import NoReturn

import pytest

from bi_external_api.domain.internal import datasets
from bi_external_api.testings import SingleAvatarDatasetBuilder
from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from dl_constants.enums import (
    BIType,
    FieldType,
)


@pytest.fixture(scope="session")
def bi_ext_api_docker_compose_test_env() -> NoReturn:
    raise AssertionError("Local united storage must not be used in unit tests")


WB_ID = "unit_tests_wb"

PG_CONN = datasets.ConnectionInstance.create_for_type(
    CONNECTION_TYPE_POSTGRES, id="pg_conn_1_id", name="pg_conn_1", wb_id=WB_ID
)

PG_1_DS = (
    SingleAvatarDatasetBuilder()
    .summary(id="pg_conn_1_ds_ID", name="pg_conn_1_ds_NAME", wb_id=WB_ID)
    .fake_pg_table(PG_CONN.summary.id)
    .df(id="id", data_type=BIType.string)
    .df(id="date", data_type=BIType.date)
    .df(id="customer", data_type=BIType.string)
    .df(id="amount", data_type=BIType.float)
    .df(id="position", data_type=BIType.string)
    .ff(id="amount_sum", formula="SUM([amount])", data_type=BIType.float, f_type=FieldType.MEASURE)
    .build_dataset_instance()
)

# TODO FIX: Validate that matches
PG_1_DS_SQL = """
SELECT t.*
FROM (VALUES ('hYfUnzmXobmYyeZh99h68n', date('11-01-2001'), 'Vasya', 10.1, 'Cat food'),
             ('NPJSJSHLBBcU9yLgsGpoaS', date('11-01-2001'), 'Petya', 100.3, 'Bitcoin'),
             ('CZG3CReCiVABHrS74GC8xe', date('11-01-2001'), 'Kolya', 14.4, 'Book'),

             ('X8wcofcarDC7hvfWtiziCg', date('12-01-2001'), 'Vasya', 10.1, 'Cat food'),
             ('iN6o2yp33kQggAHzGJg4yU', date('12-01-2001'), 'Vasya', 14.1, 'Medical mask'),

             ('YLWuixDCp9LDpQah8oNu9t', date('13-01-2001'), 'Kolya', 10.1, 'Medical mask'),
             ('AdD2fB8X5hF9c77ZiQRv6z', date('13-01-2001'), 'Petya', 10.1, 'Medical mask'),

             ('SZRPSPFBdLeWdSFoeLz7xH', date('14-01-2001'), 'Vasya', 10.1, 'Vodka'),
             ('NiUVgETf5hE2YTjyTriGPX', date('14-01-2001'), 'Vasya', 10.1, 'Cat food'),
             ('3qWGZ74cniezDJbAvVJAem', date('14-01-2001'), 'Kolya', 210.1, 'Bitcoin'),
             ('n2EaqgPtVdryfwto6csLsx', date('14-01-2001'), 'Petya', 10.1, 'Vodka')
     ) AS t (id, date, customer, amount, position)
"""
