from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def resource_dir():
    conftest_dir = Path(__file__).resolve().parent
    resource_dir = conftest_dir / "resources"
    assert resource_dir.is_dir(), f"Resource directory not found at {resource_dir}"
    return resource_dir


@pytest.fixture(scope="session")
def sample_pkg_toml(resource_dir):
    return resource_dir / "sample.toml"


@pytest.fixture(scope="session")
def sample_compose_src(resource_dir):
    return resource_dir / "docker-compose.src.yml"


@pytest.fixture(scope="session")
def sample_compose_expected(resource_dir):
    return resource_dir / "docker-compose.expected.yml"
