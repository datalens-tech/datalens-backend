from functools import partial
from pathlib import Path

import clize
import tomlkit


def get_compose_path(prj_root: Path, target: str, with_local_suffix: str = '') -> str:
    rel_path, section = target.split(":")

    try:
        ref = tomlkit.load(open(prj_root / rel_path / "pyproject.toml"))
    except FileNotFoundError:
        ref = {}

    section = ref.get("datalens", {}).get("pytest", {}).get(section)

    base_path = prj_root / rel_path
    base_name = "docker-compose"
    if section and (base := section.get("compose_file_base")):
        base_name = base

    # todo remove local suffixed yml's after migration from arc
    if with_local_suffix:
        return str(Path(base_path / f"{base_name}.local.yml").resolve())
    else:
        return str(Path(base_path / f"{base_name}.yml").resolve())


cmd = partial(clize.run, get_compose_path)
