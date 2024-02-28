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
DEFAULT_TARGET_NAME = "__default__"
DEFAULT_TARGET_PATHS = frozenset(["."])


@dataclasses.dataclass
class TestTarget:
    package_path: pathlib.Path
    section: str
    requested_mode: str


@dataclasses.dataclass
class PyprojectPytestTarget:
    root_dir: str
    target_paths: frozenset[str] = dataclasses.field(default=DEFAULT_TARGET_PATHS)
    name: str = DEFAULT_TARGET_NAME
    labels: frozenset[str] = dataclasses.field(default=DEFAULT_LABELS)

    @classmethod
    def from_raw(cls, name: str, data: dict[str, typing.Any]) -> typing_extensions.Self:
        if "target_path" in data:
            target_paths = frozenset(str(data["target_path"]).split(" "))
        else:
            target_paths = DEFAULT_TARGET_PATHS

        if "labels" in data:
            labels = frozenset(data["labels"])
        else:
            labels = DEFAULT_LABELS

        return cls(
            name=name,
            root_dir=data["root_dir"],
            target_paths=target_paths,
            labels=labels,
        )


def format_output(name: str, targets: typing.Iterable[TestTarget]) -> str:
    data = [f"{target.package_path.name}:{target.section}" for target in targets]

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


def read_package_paths(
    root_dir: pathlib.Path,
    targets_path: pathlib.Path,
) -> typing.Generator[pathlib.Path, None, None]:
    with open(targets_path, "r") as file:
        data = file.read()
        data = data.strip()
        data = data.replace("'", '"')

    for item in json.loads(data):
        package_relative_path = item.strip()
        yield root_dir / package_relative_path


def read_pytest_targets(package_path: pathlib.Path) -> list[PyprojectPytestTarget]:
    pyproject_path = package_path / "pyproject.toml"

    if not pyproject_path.is_file():
        error_message = f"pyproject.toml not found in {package_path}"
        print_error(error_message)
        raise FileNotFoundError(error_message)

    with open(pyproject_path, "r") as file:
        toml_data = tomlkit.parse(file.read())

    raw_targets = toml_data.get("datalens", {}).get("pytest", {})

    if len(raw_targets) == 0:
        return [PyprojectPytestTarget(root_dir=f"{package_path.name}_tests/")]

    return [PyprojectPytestTarget.from_raw(name=name, data=data) for name, data in raw_targets.items()]


def get_target_tests(
    package_path: pathlib.Path,
    requested_labels: set[str],
    target: PyprojectPytestTarget,
    raise_on_unused_label: bool = False,
) -> typing.Generator[TestTarget, None, None]:
    requested_labels = requested_labels & target.labels
    unused_labels = target.labels - requested_labels

    if len(unused_labels) > 0:
        error_message = f"Unused labels in {package_path.name}:{target.name}: {' '.join(unused_labels)}"
        print_error(error_message)

        if raise_on_unused_label:
            raise ValueError(error_message)

    for requested_mode in requested_labels:
        yield TestTarget(package_path=package_path, section=target.name, requested_mode=requested_mode)


def get_package_tests(
    package_path: pathlib.Path,
    requested_labels: set[str],
    raise_on_unused_label: bool = False,
) -> typing.Generator[TestTarget, None, None]:
    try:
        pytest_targets = read_pytest_targets(package_path)
    except FileNotFoundError:
        return

    for target in pytest_targets:
        yield from get_target_tests(
            package_path=package_path,
            requested_labels=requested_labels,
            target=target,
            raise_on_unused_label=raise_on_unused_label,
        )


def get_tests(
    requested_labels: set[str],
    root_dir: pathlib.Path,
    test_targets_json_path: pathlib.Path,
    raise_on_unused_label: bool = False,
) -> typing.Generator[TestTarget, None, None]:
    for package_path in read_package_paths(root_dir=root_dir, targets_path=test_targets_json_path):
        yield from get_package_tests(
            package_path=package_path,
            requested_labels=requested_labels,
            raise_on_unused_label=raise_on_unused_label,
        )


def split_tests(
    requested_labels: str,
    root_dir: str,
    test_targets_json_path: str,
    raise_on_unused_label: bool = False,
) -> None:
    labels = set(requested_labels.split(","))

    result = get_tests(
        requested_labels=labels,
        root_dir=pathlib.Path(root_dir),
        test_targets_json_path=pathlib.Path(test_targets_json_path),
        raise_on_unused_label=raise_on_unused_label,
    )
    print_output(labels, result)


cmd = functools.partial(clize.run, split_tests)

if __name__ == "__main__":
    cmd()
