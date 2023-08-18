import json
import tempfile

import pytest

from bi_connector_clickhouse.formula.constants import ClickHouseDialect
from bi_connector_postgresql.formula.constants import PostgreSQLDialect

from bi_formula_ref.config import DOC_GEN_CONFIG_DEFAULT
from bi_formula_ref.generator import ReferenceDocGenerator, ConfigVersion


@pytest.fixture(scope='function')
def example_db_conf_patch(monkeypatch, all_db_configurations, all_dbe):
    db_conf_data = {
        'CLICKHOUSE_21_8': all_db_configurations[ClickHouseDialect.CLICKHOUSE_21_8],
        'COMPENG': all_db_configurations[PostgreSQLDialect.COMPENG],
    }
    with tempfile.NamedTemporaryFile() as db_conf_f:
        # Prepare DB config
        with open(db_conf_f.name, 'w') as db_conf_f_w:
            json.dump(db_conf_data, db_conf_f_w)

        monkeypatch.setattr(DOC_GEN_CONFIG_DEFAULT, 'db_config_file', db_conf_f.name)
        yield


@pytest.fixture(scope='function')
def example_data_file_patch(monkeypatch):
    with tempfile.NamedTemporaryFile() as ex_data_f:
        monkeypatch.setattr(DOC_GEN_CONFIG_DEFAULT, 'example_data_file', ex_data_f.name)
        yield


def test_prepare_example_data(example_db_conf_patch, example_data_file_patch):
    gen = ReferenceDocGenerator(config_version=ConfigVersion.default, locale='en_US')
    gen.generate_example_data()

    with open(DOC_GEN_CONFIG_DEFAULT.example_data_file, 'rb') as ex_data_f:
        data: list = json.load(ex_data_f)
        keys = {item[0] for item in data}
        assert 'ABS.Example' in keys
