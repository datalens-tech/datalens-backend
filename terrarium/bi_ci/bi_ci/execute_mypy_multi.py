from functools import partial
import json
from pathlib import Path
import subprocess
import sys
from typing import Iterable

import clize
import tomlkit
from tomlkit.exceptions import NonExistentKey


PYPROJECT_TOML = "pyproject.toml"


def get_mypy_targets(pkg_dir: Path) -> list[str]:
    try:
        with open(pkg_dir / PYPROJECT_TOML) as fh:
            meta = tomlkit.load(fh)
            return meta["datalens"]["meta"]["mypy"]["targets"]  # type: ignore
    except NonExistentKey:
        pass

    # fallback to the same name defaults
    dirname = pkg_dir.name or ""
    return [dirname]


def get_targets(root: Path) -> Iterable[str]:
    for path in root.rglob(f"*{PYPROJECT_TOML}"):
        yield str(path.parent).replace(PYPROJECT_TOML, "")


def main(root: Path, targets_file: Path = None) -> None:  # type: ignore
    # clize can't recognize type annotation "Optional"
    paths: Iterable[str]
    if targets_file is not None:
        paths = json.load(open(targets_file))
    else:
        paths = get_targets(root)
    failed_list: list[str] = []
    mypy_cache_dir = Path("/tmp/mypy_cache")
    mypy_cache_dir.mkdir(exist_ok=True)
    for path in paths:
        pkg_dir = root / path
        run_args = ["mypy", f"--cache-dir={mypy_cache_dir}"]
        targets = get_mypy_targets(pkg_dir)
        print(f"Cmd: {run_args}; cwd={pkg_dir}")
        if len(targets) > 0:
            run_args.extend(targets)
            run_exit_code = subprocess.run(" ".join(run_args), shell=True, cwd=str(pkg_dir)).returncode
            if run_exit_code != 0:
                failed_list.append(path)
        else:
            print(f"mypy config not found in {pkg_dir}/pyproject.toml, skipped")

    if failed_list:
        print("Mypy failed for targets:")
        print("\n".join(sorted(failed_list)))

    sys.exit(0 if len(failed_list) == 0 else 1)


cmd = partial(clize.run, main)

if __name__ == "__main__":
    cmd()
