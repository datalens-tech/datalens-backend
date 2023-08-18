import json
import os

import clize
from pathlib import Path
from tomlkit import parse

"""
example test spec:

[datalens.pytest.unit]
root_dir = "bi_core_tests/"
target_path = "unit"

[datalens.pytest.db_part_1]
root_dir = "bi_core_tests/db/"
target_path = "aio caches capabilities common compeng"
labels = ["fat"]
"""

SECTIONS_TO_SKIP = ["ext"]


def format_output(name: str, sections: list[tuple[str, str]]) -> str:
    data = [
        f"{path}:{target}" for path, target in sections
        if target not in SECTIONS_TO_SKIP
    ]
    return f"{name}={json.dumps(data)}"


def split_tests(mode: str) -> None:
    """
    :param mode: Either "base" for test targets without specific labels, or "fat" for test target to be put into
     the "fat" runners
    :side-effect: prints to stdout variable assignment for github output
    """
    targets_file = os.environ.get("TEST_TARGETS")
    raw = open(targets_file).read().strip().replace("'", '"')
    paths = json.loads(raw)

    base = []
    fat = []

    for short_path in paths:
        path = Path(__file__).resolve().parent.parent / short_path.strip() / "pyproject.toml"

        if not path.is_file():
            continue

        with open(path, "r") as file:
            toml_data = parse(file.read())

        pytest_targets = toml_data.get("datalens", {}).get("pytest", {})
        if pytest_targets:
            for section in pytest_targets.keys():
                spec = toml_data["datalens"]["pytest"][section]
                if "fat" in spec.get("labels", []):
                    fat.append((short_path, section))
                else:
                    base.append((short_path, section))
        else:
            base.append((short_path, "__default__"))

    out_split = format_output("split", base)
    out_split_fat = format_output("split_fat", fat)

    if mode == "base":
        print(out_split)
    elif mode == "fat":
        print(out_split_fat)
    else:
        raise RuntimeError("provide either base or fat mode for the script")


if __name__ == "__main__":
    clize.run(split_tests)
