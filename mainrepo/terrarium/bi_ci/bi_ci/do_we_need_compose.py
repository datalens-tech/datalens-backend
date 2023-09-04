from functools import partial
from pathlib import Path

import clize
import tomlkit


def do_we_need_compose(prj_root: Path, target: str) -> str:
    rel_path, section_name = target.split(":")
    if not (prj_root / rel_path / "docker-compose.yml").exists():
        return "0"

    ref = tomlkit.load(open(prj_root / rel_path / "pyproject.toml"))
    section = ref.get("datalens", {}).get("pytest", {}).get(section_name)

    if section and section.get("skip_compose") == "true":
        return "0"
    return "1"


cmd = partial(clize.run, do_we_need_compose)
