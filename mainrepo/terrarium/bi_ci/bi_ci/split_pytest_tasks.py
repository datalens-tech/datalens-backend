from collections import defaultdict
from functools import partial
import json
from pathlib import Path

import clize
from tomlkit import parse

"""
example test spec:

[datalens.pytest.unit]
root_dir = "bi_core_tests/"
target_path = "unit"

[datalens.pytest.db_part_1]
root_dir = "bi_core_tests/db/"
target_path = "aio caches capabilities common"
labels = ["fat"]
"""


def format_output(name: str, sections: list[tuple[str, str]]) -> str:
    data = [f"{path}:{target}" for path, target in sections]
    return f"{name}={json.dumps(data)}"


def split_tests(
    requested_mode: str,
    root_dir: Path,
    test_targets_json_path: Path,
) -> None:
    # targets_file = os.environ.get("TEST_TARGETS")
    raw = open(test_targets_json_path).read().strip().replace("'", '"')
    paths = json.loads(raw)

    # print(paths)
    split_result: dict[str, list[tuple]] = defaultdict(list)

    # ipdb.set_trace()
    for package_path in paths:
        path = root_dir / package_path.strip() / "pyproject.toml"
        # print(path)
        if not path.is_file():
            continue

        with open(path, "r") as file:
            toml_data = parse(file.read())

        pytest_targets = toml_data.get("datalens", {}).get("pytest", {})

        if pytest_targets:
            for section in pytest_targets.keys():
                spec = toml_data["datalens"]["pytest"][section]
                # supporting only a single label for now
                for category in spec.get("labels", []):
                    split_result[category].append((package_path, section))
                    break
                else:
                    split_result["base"].append((package_path, section))
        else:
            split_result["base"].append((package_path, "__default__"))

    print(format_output(f"split_{requested_mode}", split_result[requested_mode]))
    # note: I would expect following to be usable, but no idea how to make it with GHA :(
    # for mode, result in sorted(split_result.items()):
    #     print(format_output(f"split_{mode}", result), end=" ")


cmd = partial(clize.run, split_tests)

if __name__ == "__main__":
    cmd()
