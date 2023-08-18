import os

import pytest

from bi_testing.env_params.generic import GenericEnvParamGetter
from bi_testing.files import get_file_loader


@pytest.fixture(scope='session')
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), 'params.yml')
    filepath = get_file_loader().resolve_path(filepath)
    return GenericEnvParamGetter.from_yaml_file(filepath)
