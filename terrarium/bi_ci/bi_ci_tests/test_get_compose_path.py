from pathlib import Path

from bi_ci.get_compose_path import get_compose_path


def test_get_compose_path(resource_dir):
    prj_root = resource_dir.parent
    rel_path = resource_dir.name
    section = "db"

    result = get_compose_path(prj_root, f"{rel_path}:{section}")
    expected = str(resource_dir / "alt_docker-compose.yml")
    assert result == expected

    result = get_compose_path(prj_root, f"{rel_path}:{section}", True)
    expected = str(resource_dir / "alt_docker-compose.local.yml")
    assert result == expected

    section = "snowflake"
    result = get_compose_path(prj_root, f"{rel_path}:{section}")
    expected = str(resource_dir / "docker-compose.yml")
    assert result == expected

    section = "--undefined--"
    result = get_compose_path(prj_root, f"{rel_path}:{section}")
    expected = str(resource_dir / "docker-compose.yml")
    assert result == expected



