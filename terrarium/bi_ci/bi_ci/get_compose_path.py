from functools import partial
from pathlib import Path

import clize
import tomlkit


def get_compose_path(prj_root: Path, target: str) -> str:
    rel_path, section_name = target.split(":")

    ref: dict
    try:
        ref = tomlkit.load(open(prj_root / rel_path / "pyproject.toml"))
    except FileNotFoundError:
        ref = {}

    section = ref.get("datalens", {}).get("pytest", {}).get(section_name, {})

    base_path = prj_root / rel_path
    base_name = "docker-compose"
    if section and (base := section.get("compose_file_base")):
        base_name = base

    return str(Path(base_path / f"{base_name}.yml").resolve())


cmd = partial(clize.run, get_compose_path)
