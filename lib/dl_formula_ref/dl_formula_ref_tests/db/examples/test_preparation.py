import json
import tempfile

import pytest

from dl_formula_ref.config import DOC_GEN_CONFIG_DEFAULT
from dl_formula_ref.generator import (
    ConfigVersion,
    ReferenceDocGenerator,
)

from dl_connector_clickhouse.formula.constants import ClickHouseDialect


@pytest.fixture(scope="function")
def example_db_config_file(all_db_configurations, dbe):
    db_conf_data = {
        "CLICKHOUSE_22_10": all_db_configurations[ClickHouseDialect.CLICKHOUSE_22_10],
    }
    with tempfile.NamedTemporaryFile() as db_conf_f:
        # Prepare DB config
        with open(db_conf_f.name, "w") as db_conf_f_w:
            json.dump(db_conf_data, db_conf_f_w)

        yield db_conf_f.name


@pytest.fixture(scope="function")
def config_example_dialect_patch(monkeypatch):
    monkeypatch.setattr(DOC_GEN_CONFIG_DEFAULT, "default_example_dialect", ClickHouseDialect.CLICKHOUSE_22_10)


@pytest.fixture(scope="function")
def example_data_file(monkeypatch):
    with tempfile.NamedTemporaryFile() as ex_data_f:
        yield ex_data_f.name


def test_prepare_example_data(example_db_config_file, example_data_file, config_example_dialect_patch):
    gen = ReferenceDocGenerator(config_version=ConfigVersion.default, locale="en")
    gen.generate_example_data(
        output_path=example_data_file,
        db_config_path=example_db_config_file,
        default_dialect=ClickHouseDialect.CLICKHOUSE_22_10,
    )

    with open(example_data_file, "rb") as ex_data_f:
        data: list = json.load(ex_data_f)
        keys = {item[0] for item in data}
        assert "ABS.Example" in keys
