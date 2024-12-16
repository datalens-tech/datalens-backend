import typing

import pytest

import dl_settings_tests.utils as test_utils


@pytest.fixture(name="tmp_configs")
def fixture_tmp_configs() -> typing.Generator[test_utils.TmpConfigs, None, None]:
    with test_utils.TmpConfigs() as tmp_configs:
        yield tmp_configs
