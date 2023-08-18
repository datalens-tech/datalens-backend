import itertools
import json
import os
import subprocess
import argparse
from typing import Any

HERE = os.path.realpath(os.path.dirname(__file__))


def get_bake_config_for_target(target_name: str) -> dict[str, Any]:
    return json.loads(subprocess.run(
        [os.path.join(HERE, "run-project-bake"), "--print", "--progress=plain", target_name],
        stderr=None,
        stdout=subprocess.PIPE,
        check=True
    ).stdout)


def get_all_dependent_targets(target_name: str) -> list[str]:
    cfg = get_bake_config_for_target(target_name)
    return [target_name for target_name, _ in cfg["target"].items()]


def get_all_dependent_targets_cache_from_list(target_name: str) -> list[str]:
    cfg = get_bake_config_for_target(target_name)
    raw_list_of_lists = [target_value.get("cache-from", ()) for _, target_value in cfg["target"].items()]

    return list(sorted({
        raw_value.removeprefix("type=registry,ref=")
        for raw_value in itertools.chain(*raw_list_of_lists)
        if isinstance(raw_value, str) and raw_value.startswith("type=registry,ref=")
    }))


def pre_load_cache(target_name: str) -> None:
    related_cache_img_list = get_all_dependent_targets_cache_from_list(target_name)
    for img in related_cache_img_list:
        subprocess.run(["docker", "pull", img], check=False)


def wrap_bake(target_name: str, do_load_cache: bool) -> None:
    if do_load_cache:
        pre_load_cache(target_name)

    cmd = [os.path.join(HERE, "run-project-bake"), "--progress=plain", *get_all_dependent_targets(target_name)]
    print(cmd)

    subprocess.run(
        cmd,
        check=True
    )


def main():
    parser = argparse.ArgumentParser(description="Docker bake digger")

    subparsers = parser.add_subparsers(title="Commands", dest="command")

    parser_expand_targets = subparsers.add_parser("expand_targets", help="Add two numbers")
    parser_expand_targets.add_argument("target", type=str, help="Target to expand")

    parser_pull_cache = subparsers.add_parser("pull_cache", help="Pull cache for target and it's dependencies")
    parser_pull_cache.add_argument("target", type=str, help="Target to expand")

    parser_list_cache_img = subparsers.add_parser("list_cache", help="List cache for target and it's dependencies")
    parser_list_cache_img.add_argument("target", type=str, help="Target to expand")

    parser_bake_wrapped = subparsers.add_parser("wrap_bake", help="Run bake for targets with cache loading/pushing")
    parser_bake_wrapped.add_argument("target", type=str, help="Target to expand")
    parser_bake_wrapped.add_argument("--no-load-cache", default=False, action='store_true', help="Target to expand")

    # Parse the arguments
    args = parser.parse_args()

    # Perform actions based on the selected command
    if args.command == "expand_targets":
        print(get_all_dependent_targets(args.target))
    elif args.command == "pull_cache":
        pre_load_cache(args.target)
    elif args.command == "list_cache":
        for img in get_all_dependent_targets_cache_from_list(args.target):
            print(img)
    elif args.command == "wrap_bake":
        wrap_bake(args.target, do_load_cache=not args.no_load_cache)


if __name__ == '__main__':
    main()
