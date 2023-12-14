import functools
import json
import pathlib
import typing

import clize
from tomlkit import parse


"""
example test spec:

[datalens.pytest.unit]
root_dir = "dl_core_tests/"
target_path = "unit"

[datalens.pytest.db_part_1]
root_dir = "dl_core_tests/db/"
target_path = "aio caches capabilities common"
labels = ["fat"]
"""

DEFAULT_MODE = "base"


def format_output(name: str, sections: list[tuple[str, str]]) -> str:
    data = [f"{path}:{target}" for path, target in sections]

    return f"{name}={json.dumps(data)}"


def read_package_paths(targets_path: pathlib.Path) -> typing.Generator[str, None, None]:
    with open(targets_path, "r") as file:
        data = file.read()
        data = data.strip()
        data = data.replace("'", '"')

    for item in json.loads(data):
        yield item.strip()


def read_pytest_targets(path: pathlib.Path) -> typing.Optional[dict[str, typing.Any]]:
    if not path.is_file():
        print(f"File {path} not found")
        raise FileNotFoundError(f"File {path} not found")

    with open(path, "r") as file:
        toml_data = parse(file.read())

    return toml_data.get("datalens", {}).get("pytest", {})


def get_package_tests(
    root_path: pathlib.Path,
    package_path: str,
    requested_mode: str,
) -> typing.Generator[tuple[str, str], None, None]:
    pytest_targets: dict | None = {}
    try:
        pytest_targets = read_pytest_targets(root_path / package_path / "pyproject.toml")
    except FileNotFoundError:
        return

    if pytest_targets is None:
        return

    for section in pytest_targets.keys():
        spec = pytest_targets.get(section, {})
        labels = spec.get("labels", [])

        if requested_mode in labels:
            yield package_path, section


def get_default_package_tests(
    root_path: pathlib.Path,
    package_path: str,
) -> typing.Generator[tuple[str, str], None, None]:
    pytest_targets: dict | None = {}
    try:
        pytest_targets = read_pytest_targets(root_path / package_path / "pyproject.toml")
    except FileNotFoundError:
        return

    if not pytest_targets or len(pytest_targets) == 0:
        yield package_path, "__default__"
        return

    for section, spec in pytest_targets.items():
        labels = spec.get("labels", [])

        if len(labels) == 0:
            yield package_path, section


def get_tests(
    requested_mode: str,
    root_dir: pathlib.Path,
    test_targets_json_path: pathlib.Path,
) -> typing.Generator[tuple[str, str], None, None]:
    for package_path in read_package_paths(test_targets_json_path):
        if requested_mode == DEFAULT_MODE:
            yield from get_default_package_tests(root_dir, package_path)
        else:
            yield from get_package_tests(root_dir, package_path, requested_mode)


def split_tests(
    requested_mode: str,
    root_dir: pathlib.Path,
    test_targets_json_path: pathlib.Path,
) -> None:
    result = list(get_tests(requested_mode, root_dir, test_targets_json_path))
    formatted_output = format_output(f"split_{requested_mode}", result)
    print(formatted_output)


cmd = functools.partial(clize.run, split_tests)

if __name__ == "__main__":
    cmd()
