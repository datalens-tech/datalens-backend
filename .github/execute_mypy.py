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


def main(target: str):
    pkg_dir = SRC_ROOT / target
    if (pkg_dir / "mypy.ini").exists():
        run_args = [

            PYTHON,
            "-m",
            "mypy",
        ]
        targets = get_mypy_targets(pkg_dir)
        run_args.extend(targets)
        sys.exit(
            subprocess.run(" ".join(run_args), shell=True, cwd=str(pkg_dir)).returncode
        )
    else:
        print(f"mypy.ini not found in {pkg_dir}, skipped")


if __name__ == "__main__":
    main(sys.argv[1])
