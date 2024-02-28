import dataclasses
import functools
import json
import pathlib
import sys
import typing

import clize
import tomlkit
import typing_extensions


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
DEFAULT_LABELS = frozenset([DEFAULT_MODE])


@dataclasses.dataclass
class TestTarget:
    package_path: str
    section: str
    requested_mode: str


@dataclasses.dataclass
class PyprojectPytestTarget:
    labels: frozenset[str] = dataclasses.field(default=DEFAULT_LABELS)

    @classmethod
    def from_dict(cls, data: dict[str, typing.Any]) -> typing_extensions.Self:
        return cls(
            labels=frozenset(data["labels"]) if "labels" in data else DEFAULT_LABELS,
        )


def format_output(name: str, targets: typing.Iterable[TestTarget]) -> str:
    data = [f"{target.package_path}:{target.section}" for target in targets]

    return f"{name}={json.dumps(data)}"


def print_output(requested_modes: set[str], test_targets: typing.Iterable[TestTarget]) -> None:
    targets_by_mode: dict[str, list] = {}
    for target in test_targets:
        targets_by_mode.setdefault(target.requested_mode, []).append(target)

    for requested_mode in requested_modes:
        targets = targets_by_mode.get(requested_mode, [])
        formatted_output = format_output(f"split_{requested_mode}", targets)
        print(formatted_output)


def print_error(message: str) -> None:
    print(message, file=sys.stderr)


def read_package_paths(targets_path: pathlib.Path) -> typing.Generator[str, None, None]:
    with open(targets_path, "r") as file:
        data = file.read()
        data = data.strip()
        data = data.replace("'", '"')

    for item in json.loads(data):
        yield item.strip()


def read_pytest_targets(path: pathlib.Path) -> dict[str, PyprojectPytestTarget]:
    if not path.is_file():
        print_error(f"File {path.absolute()} not found")
        raise FileNotFoundError(f"File {path.absolute()} not found")

    with open(path, "r") as file:
        toml_data = tomlkit.parse(file.read())

    raw_targets = toml_data.get("datalens", {}).get("pytest", {})

    if len(raw_targets) == 0:
        return {"__default__": PyprojectPytestTarget()}

    return {name: PyprojectPytestTarget.from_dict(data) for name, data in raw_targets.items()}


def get_package_tests(
    root_path: pathlib.Path,
    package_path: str,
    requested_modes: set[str],
) -> typing.Generator[TestTarget, None, None]:
    try:
        pytest_targets = read_pytest_targets(root_path / package_path / "pyproject.toml")
    except FileNotFoundError:
        return

    for name, target in pytest_targets.items():
        requested_labels = requested_modes & target.labels

        for requested_mode in requested_labels:
            yield TestTarget(package_path=package_path, section=name, requested_mode=requested_mode)


def get_tests(
    requested_modes: set[str],
    root_dir: pathlib.Path,
    test_targets_json_path: pathlib.Path,
) -> typing.Generator[TestTarget, None, None]:
    for package_path in read_package_paths(test_targets_json_path):
        yield from get_package_tests(
            root_path=root_dir,
            package_path=package_path,
            requested_modes=requested_modes,
        )


def split_tests(
    requested_modes: str,
    root_dir: str,
    test_targets_json_path: str,
) -> None:
    modes = set(requested_modes.split(","))

    result = get_tests(
        requested_modes=modes,
        root_dir=pathlib.Path(root_dir),
        test_targets_json_path=pathlib.Path(test_targets_json_path),
    )
    print_output(modes, result)


cmd = functools.partial(clize.run, split_tests)

if __name__ == "__main__":
    cmd()
