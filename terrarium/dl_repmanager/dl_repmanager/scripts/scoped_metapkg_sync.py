import argparse
from pathlib import Path

from dl_repmanager.logging import setup_basic_logging
from dl_repmanager.metapkg_scoped_sync import sync_scoped_metapkg


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="DL Package Meta CLI")
    parser.add_argument("--orig-metapkg-path", required=True)
    parser.add_argument("--scoped-metapkg-path", required=True)
    parser.add_argument("--lib-no-prune", action="append", default=[])
    parser.add_argument("--remove-private-pypi", action="store_true", default=False)
    parser.add_argument("--use-target-lock", action="store_true", default=False)
    parser.add_argument(
        "lib_paths_to_include",
        nargs="+",
        help="Path of libraries to include relative to scoped metapkg",
    )

    return parser


def main() -> None:
    setup_basic_logging()
    parser = make_parser()

    args = parser.parse_args()

    sync_scoped_metapkg(
        original_metapkg_path=Path(args.orig_metapkg_path).absolute(),
        scoped_metapkg_path=Path(args.scoped_metapkg_path).absolute(),
        use_target_lock=args.use_target_lock,
        prevent_prune_for_deps=args.lib_no_prune,
        package_dirs_to_include=[Path(path_str) for path_str in args.lib_paths_to_include],
        remove_private_pypi=args.remove_private_pypi,
    )


if __name__ == "__main__":
    main()
