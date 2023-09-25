import json
import tempfile

import pytest

from dl_connector_clickhouse.formula.constants import ClickHouseDialect
from dl_formula_ref.config import DOC_GEN_CONFIG_DEFAULT
from dl_formula_ref.generator import (
    ConfigVersion,
    ReferenceDocGenerator,
)


@pytest.fixture(scope="function")
def example_db_conf_patch(monkeypatch, all_db_configurations, dbe):
    db_conf_data = {
        "CLICKHOUSE_22_10": all_db_configurations[ClickHouseDialect.CLICKHOUSE_22_10],
    }
    with tempfile.NamedTemporaryFile() as db_conf_f:
        # Prepare DB config
        with open(db_conf_f.name, "w") as db_conf_f_w:
            json.dump(db_conf_data, db_conf_f_w)

        monkeypatch.setattr(DOC_GEN_CONFIG_DEFAULT, "db_config_file", db_conf_f.name)
        monkeypatch.setattr(DOC_GEN_CONFIG_DEFAULT, "default_example_dialect", ClickHouseDialect.CLICKHOUSE_22_10)
        yield


@pytest.fixture(scope="function")
def example_data_file_patch(monkeypatch):
    with tempfile.NamedTemporaryFile() as ex_data_f:
        monkeypatch.setattr(DOC_GEN_CONFIG_DEFAULT, "example_data_file", ex_data_f.name)
        yield


def test_prepare_example_data(example_db_conf_patch, example_data_file_patch):
    gen = ReferenceDocGenerator(config_version=ConfigVersion.default, locale="en")
    gen.generate_example_data()

    with open(DOC_GEN_CONFIG_DEFAULT.example_data_file, "rb") as ex_data_f:
        data: list = json.load(ex_data_f)
        keys = {item[0] for item in data}
        assert "ABS.Example" in keys
