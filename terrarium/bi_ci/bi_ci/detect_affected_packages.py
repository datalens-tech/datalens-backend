import argparse
from collections import (
    defaultdict,
    deque,
)
import json
import os
from pathlib import Path
from typing import Iterator

import attr

from .pkg_ref import PkgRef


@attr.s
class Config:
    root_dir: Path = attr.ib()
    roots: list[str] = attr.ib()
    changed_paths: list[str] = attr.ib()


def collect_affected_packages(
    refs: list[PkgRef],
    paths: list[str],
) -> list[PkgRef]:
    # path relative to project root
    seen = set()
    pkg_by_path = {str(p.partial_parent_path): p for p in refs}

    # slow, but should work ...
    for p in paths:
        for pkg_path in pkg_by_path:
            if pkg_path in p:
                seen.add(pkg_path)
                continue

    return [pkg_by_path[k] for k in seen]


def gen_pkg_dirs(cfg: Config) -> Iterator[Path]:
    # todo: maybe relay of dl_repmanager to walk sub projects
    for root in cfg.roots:
        root_path = (Path(cfg.root_dir) / root).resolve()
        if not root_path.exists():
            continue
        for item in root_path.iterdir():
            if item.is_dir() and (item / "pyproject.toml").exists():
                yield item


def get_reverse_dependencies(direct_dependency: dict[str, list[str]]) -> dict[str, list[str]]:
    reverse_ref = defaultdict(list)
    for pkg in direct_dependency:
        for dep in direct_dependency[pkg]:
            reverse_ref[dep].append(pkg)
    return reverse_ref


def get_leafs(dependencies: dict[str, list[str]]) -> set[str]:
    all_values = []
    for deps in dependencies.values():
        all_values.extend(deps)
    leafs = set(all_values) - set([k for k in dependencies.keys() if len(dependencies[k]) > 0])
    return leafs


def get_deep_deps(dependencies: dict[str, list[str]]) -> dict[str, set[str]]:
    """
    @param dependencies: dict with pkg names direct dependencies
    """
    aff_dict = get_reverse_dependencies(dependencies)
    leafs = get_leafs(dependencies)

    result = defaultdict(set)

    seen = set()
    ws = deque(leafs)
    while len(ws) > 0:
        node = ws.popleft()
        seen.add(node)
        for child in dependencies.get(node, []):
            result[node].add(child)
            result[node] |= result.get(child, set())
        for parent in aff_dict.get(node, []):
            if parent not in seen:
                ws.append(parent)

    return result


def get_deep_affection_map(dependencies: dict[str, list[str]]) -> dict[str, set[str]]:
    """
    Returns a mapping with pkg pointing to set of pkgs which is affected by changes in it
    """
    deep_deps = get_deep_deps(dependencies)

    affected = defaultdict(set)
    for pkg, dep_list in deep_deps.items():
        for dep in dep_list:
            affected[dep].add(pkg)
    return affected


def process(
    cfg: Config,
    get_all: bool = False,
) -> list[PkgRef]:
    affected = list(cfg.changed_paths)

    depends_on = defaultdict(list)
    pkg_by_ref = {}

    all_refs = []
    for item in gen_pkg_dirs(cfg):
        ref = PkgRef(root=cfg.root_dir, full_path=item)
        all_refs.append(ref)
        if ref.self_pkg_name is None:
            continue
        pkg_by_ref[ref.self_pkg_name] = ref
        for req in ref.extract_local_requirements(include_groups=["tests"]):
            depends_on[ref.self_pkg_name].append(req)

    if get_all:
        to_test = set(pkg_by_ref.keys())
    else:
        affection_map = get_deep_affection_map(depends_on)
        if len(affected) == 0:
            return all_refs

        direct_affected = collect_affected_packages(all_refs, affected)
        to_test = set()

        for pkg in direct_affected:
            if pkg.self_pkg_name:
                to_test.add(pkg.self_pkg_name)
                to_test.update(affection_map.get(pkg.self_pkg_name, {}))

    return [pkg_by_ref[k] for k in to_test]


def main() -> None:
    if override := os.environ.get("TEST_TARGET_OVERRIDE", ""):
        if override != "__ALL__":
            overrides = override.strip().split(",")
            print(f"affected={json.dumps(overrides)}")
            return

    parser = argparse.ArgumentParser()
    parser.add_argument("--repo")
    parser.add_argument(
        "--root_pkgs",
        help="comma separated list of dirs to walk inside to find test targets",
    )
    parser.add_argument("--changes_file")
    args = parser.parse_args()

    with open(Path(args.changes_file)) as fh:
        changed_paths = fh.read().strip().split(" ")

    cfg = Config(
        root_dir=Path(args.repo),
        roots=(args.root_pkgs or "").split(","),
        changed_paths=changed_paths,
    )

    if override == "__ALL__":
        cfg.changed_paths = []
        to_check = process(cfg, get_all=True)
    else:
        to_check = process(cfg, False)

    result = set()
    skipped = set()

    for sub in to_check:
        if sub.skip_test:
            skipped.add(str(sub.partial_parent_path))
            continue
        result.add(str(sub.partial_parent_path))
    print(f"affected={json.dumps(list(result))}")
    print(f"all_affected_with_skips={json.dumps(list(result | skipped))}")


if __name__ == "__main__":
    main()
