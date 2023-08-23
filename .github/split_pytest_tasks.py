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

SECTIONS_TO_SKIP = []


def format_output(name: str, sections: list[tuple[str, str]]) -> str:
    data = [
        f"{path}:{target}" for path, target in sections
        if target not in SECTIONS_TO_SKIP
    ]
    return f"{name}={json.dumps(data)}"


def split_tests(mode: str) -> None:
    """
    :param mode: One of: "base" for test targets without specific labels, "fat" for test target to be put into
     the "fat" runners, "ext" to pass secrets into env
    :side-effect: prints to stdout variable assignment for github output
    """
    targets_file = os.environ.get("TEST_TARGETS")
    raw = open(targets_file).read().strip().replace("'", '"')
    paths = json.loads(raw)

    split_result: dict[str, list[tuple]] = dict(
        fat=[],
        ext=[],
    )
    split_base = []

    if mode not in (modes := list(split_result.keys()) + ["base"]):
        raise RuntimeError(f'Unknown mode "{mode}", expected one of: {modes}')

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
                section_labels = spec.get("labels", [])
                for category in split_result.keys():  # find a proper category based on given labels or fallback to base
                    if category in section_labels:
                        split_result[category].append((short_path, section))
                        break
                else:
                    split_base.append((short_path, section))
        else:
            split_base.append((short_path, "__default__"))

    if mode == "base":
        print(format_output("split", split_base))
    else:
        print(format_output(f"split_{mode}", split_result[mode]))


if __name__ == "__main__":
    clize.run(split_tests)
