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
            if not meta.get("tool", {}).get("mypy", None):
                return []
            return [
                x["include"] for x in
                meta["tool"]["poetry"]["packages"]
            ]

    except NonExistentKey:
        pass
    return []


def main(target: str):
    pkg_dir = SRC_ROOT / target
    run_args = [
        PYTHON,
        "-m",
        "mypy",
    ]
    targets = get_mypy_targets(pkg_dir)
    if len(targets) > 0:
        run_args.extend(targets)
        sys.exit(
            subprocess.run(" ".join(run_args), shell=True, cwd=str(pkg_dir)).returncode
        )
    else:
        print(f"mypy.ini not found in {pkg_dir}, skipped")


if __name__ == "__main__":
    main(sys.argv[1])
