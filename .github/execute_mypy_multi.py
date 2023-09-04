import json
import subprocess
import sys
from pathlib import Path

import tomlkit
from tomlkit.exceptions import NonExistentKey

SRC_ROOT = Path("/data/")
PYTHON = "/venv/bin/python"


def get_mypy_targets(pkg_dir: Path) -> list[str]:
    try:
        with open(pkg_dir / "pyproject.toml") as fh:
            meta = tomlkit.load(fh)
            return meta["datalens"]["meta"]["mypy"]["targets"]
    except NonExistentKey:
        pass

    # fallback to the same name defaults
    return [pkg_dir.name]


def main(targets: list[str]):
    failed_list: list[str] = []
    mypy_cache_dir = Path("/tmp/mypy_cache")
    mypy_cache_dir.mkdir(exist_ok=True)
    for target in targets:
        pkg_dir = SRC_ROOT / target
        run_args = [
            PYTHON,
            "-m",
            "mypy",
            f"--cache-dir={mypy_cache_dir}"
        ]
        targets = get_mypy_targets(pkg_dir)
        print(f"Cmd: {run_args}; cwd={pkg_dir}")
        if len(targets) > 0:
            run_args.extend(targets)
            run_exit_code = subprocess.run(" ".join(run_args), shell=True, cwd=str(pkg_dir)).returncode
            if run_exit_code != 0:
                failed_list.append(target)
        else:
            print(f"mypy config not found in {pkg_dir}/pyproject.toml, skipped")

    if failed_list:
        print("Mypy failed for targets:")
        print("\n".join(sorted(failed_list)))

    sys.exit(0 if len(failed_list) == 0 else 1)


if __name__ == "__main__":
    main(json.load(open(sys.argv[1])))
