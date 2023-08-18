import argparse
import json
import os
from collections import (
    defaultdict,
    deque,
)
from pathlib import Path
from typing import Iterator, Iterable

from .pkg_ref import PkgRef
from .pkg_ref import SECTIONS, PREFIX

SECTIONS_WO_CI = set([x for x in SECTIONS if x != "ci"])


def collect_affected_packages(repo_root: Path, paths: Iterable[Path]) -> list[PkgRef]:
    # path relative to project root
    seen = set()
    for path in paths:
        parts = path.parts
        if len(parts) >= 2 and parts[0] in SECTIONS_WO_CI and (key := (parts[0:2])) not in seen:
            seen.add(key)
    refs = [PkgRef(path=repo_root / Path("/".join(key))) for key in seen]
    return [ref for ref in refs if ref.self_pkg_name is not None]


def gen_pkg_dirs(repo_path: Path) -> Iterator[Path]:
    roots = ("lib", "app")
    for root in roots:
        root_path = (Path(repo_path) / root).resolve()
        if not root_path.exists():
            continue
        for item in root_path.iterdir():
            if item.is_dir() and (item / "pyproject.toml").exists():
                yield item


def get_reverse_dependencies(direct_dependency: dict[str, list[str]]):
    reverse_ref = defaultdict(list)
    for pkg in direct_dependency:
        for dep in direct_dependency[pkg]:
            reverse_ref[dep].append(pkg)
    return reverse_ref


def get_leafs(dependencies):
    all_values = []
    for deps in dependencies.values():
        all_values.extend(deps)
    all_values = set(all_values)
    leafs = set(all_values) - set([k for k in dependencies.keys() if len(dependencies[k]) > 0])
    return leafs


def get_deep_deps(dependencies: dict[str, list[str]]) -> dict[str, set[str]]:
    """
    @param dependencies: dict with pkg names direct dependencies
    """

    aff_dict = get_reverse_dependencies(dependencies)
    leafs = get_leafs(dependencies)

    result = defaultdict(set)

    ws = deque(leafs)
    while len(ws) > 0:
        node = ws.popleft()
        for child in dependencies.get(node, []):
            result[node].add(child)
            result[node] |= result.get(child, set())
        for parent in aff_dict.get(node, []):
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


def process(repo_root: Path, affected: Iterable[Path]) -> list[PkgRef]:
    affected = list(affected)

    depends_on = defaultdict(list)
    pkg_by_ref = {}

    all_refs = []
    for item in gen_pkg_dirs(repo_root):
        ref = PkgRef(item)
        all_refs.append(ref)
        if ref.self_pkg_name is None:
            continue
        pkg_by_ref[ref.self_pkg_name] = ref
        for req in ref.extract_requirements():
            if req.name.startswith(PREFIX):
                depends_on[ref.self_pkg_name].append(req.name)

    affection_map = get_deep_affection_map(depends_on)

    if len(affected) == 0:
        return all_refs

    direct_affected = collect_affected_packages(repo_root, affected)
    to_test = set()

    for pkg in direct_affected:
        to_test.add(pkg.self_pkg_name)
        to_test.update(affection_map.get(pkg.self_pkg_name, {}))

    return [pkg_by_ref[k] for k in to_test]


def main() -> None:
    if override := os.environ.get("TEST_TARGET_OVERRIDE", ""):
        overrides = override.strip().split(",")
        print(f"affected={json.dumps(overrides)}")
        return

    parser = argparse.ArgumentParser()
    parser.add_argument("--repo")
    parser.add_argument("paths", nargs="*")
    # list on affected files in commit, starting from the repo root

    args = parser.parse_args()
    repo_root = args.repo

    to_check = process(Path(repo_root), [Path(p) for p in args.paths if not (p.startswith("ops/") or "ya.make" in p)])
    to_check.append(PkgRef(Path(repo_root) / "terrarium" / "bi_ci"))
    result = []
    for sub in to_check:
        if sub.skip_test:
            continue
        if (sub.path / "tests").exists():
            # skip generic named tests dir, use prefixed ones
            continue

        result.append(sub.rel_path)
    # result = [x.rel_path for x in to_check if not x.skip_test]
    print(f"affected={json.dumps(result)}")


if __name__ == "__main__":
    main()
