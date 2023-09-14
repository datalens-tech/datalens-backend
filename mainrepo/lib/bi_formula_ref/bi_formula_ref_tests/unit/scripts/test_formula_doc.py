from __future__ import annotations

import os
import pkgutil
import shutil
import tempfile

import pytest

import bi_formula_ref
from bi_formula_ref.config import (
    _CONFIGS_BY_VERSION,
    ConfigVersion,
)
from bi_formula_ref.scripts.formula_doc import (
    FormulaDocTool,
    parser,
)
from bi_formula_testing.tool_runner import ToolRunner

from bi_connector_clickhouse.formula.constants import ClickHouseDialect


@pytest.fixture(scope="module")
def tool():
    return ToolRunner(parser=parser, tool_cls=FormulaDocTool)


@pytest.fixture(scope="function")
def example_data_patch(monkeypatch):
    # TODO: Maybe this should be the standard way to access this file
    # Workaround for accessing data file from the monobinary in CI tests
    with tempfile.NamedTemporaryFile() as ex_file:
        ex_filename = ex_file.name

        data = pkgutil.get_data(bi_formula_ref.__name__, os.path.join("example_data.json"))
        with open(ex_filename, "wb") as ex_file_w:
            ex_file_w.write(data)

        tested_version = ConfigVersion.default
        original_config = _CONFIGS_BY_VERSION[tested_version]
        config_patched = original_config.clone(
            example_data_file=ex_filename,
            supported_dialects=frozenset({ClickHouseDialect.CLICKHOUSE_22_10}),
        )
        _CONFIGS_BY_VERSION[tested_version] = config_patched
        try:
            yield
        finally:
            _CONFIGS_BY_VERSION[tested_version] = original_config


def test_locales(tool):
    stdout, stderr = tool.run(
        [
            "locales",
            "--config-version",
            "default",
        ]
    )
    assert stderr == ""
    assert "en" in stdout


def test_versions(tool):
    stdout, stderr = tool.run(
        [
            "config-versions",
        ]
    )
    assert stderr == ""
    assert "default" in stdout


def test_doc_full_dir(tool, example_data_patch):
    tmpdir = tempfile.mkdtemp()
    stdout, stderr = tool.run(["generate", tmpdir])
    assert stderr == ""
    assert os.path.exists(os.path.join(tmpdir, "function-ref", "CONCAT.md"))
    assert os.path.exists(os.path.join(tmpdir, "function-ref", "all.md"))
    assert os.path.exists(os.path.join(tmpdir, "function-ref", "availability.md"))
    assert os.path.exists(os.path.join(tmpdir, "toc.yaml"))
    shutil.rmtree(tmpdir)
