from functools import partial
from pathlib import Path

import clize
import tomlkit


def do_we_need_compose(prj_root: Path, target: str) -> str:
    prj_root = prj_root.resolve()
    rel_path, section_name = target.split(":")
    mb_compose = prj_root / rel_path / "docker-compose.yml"

    ref = tomlkit.load(open(prj_root / rel_path / "pyproject.toml"))
    section = ref.get("datalens", {}).get("pytest", {}).get(section_name, {})

    if section.get("skip_compose") == "true":
        return "0"

    if name := section.get("compose_file_base"):
        mb_compose = prj_root / rel_path / f"{name}.yml"

    return "1" if mb_compose.exists() else "0"


cmd = partial(clize.run, do_we_need_compose)
