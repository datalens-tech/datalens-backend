import datetime
import itertools
import os
import subprocess

from datalens_local_dev.constants import (
    EXTRA_PATH_LIST,
    SUBMODULE_DEFAULT_EXCLUDES,
)
from datalens_local_dev.get_submodules import rel_path_list_for_pycham_upload
from datalens_local_dev.project_root import get_project_root


def main():
    dev_machine = os.environ["DEV_MACHINE"]
    dev_machine_mirror_path = os.environ["DEV_MACHINE_PKG_ROOT"]
    debug_mode_str = os.environ.get("DEV_MACHINE_UPLOAD_DEBUG", "0")
    debug_mode = bool(int(debug_mode_str))

    assert dev_machine, "Dev machine address is not specified"

    submodule_list = rel_path_list_for_pycham_upload() + EXTRA_PATH_LIST

    from pprint import pprint
    pprint(submodule_list)

    print(f"\n\n--- Uploading project to {dev_machine=} ---\n")

    dev_machine_escaped = dev_machine
    if ":" in dev_machine_escaped:
        dev_machine_escaped = f"[{dev_machine_escaped}]"

    root = get_project_root()
    args = [
        "rsync",
        "-rIc",
        *(["-v"] if debug_mode else []),
        "--include=*/",
        *list(
            itertools.chain(
                *list(
                    itertools.chain(
                        *[
                            [
                                # Exclude files named as in SUBMODULE_DEFAULT_EXCLUDES
                                f"--exclude={submodule}/{exclude}",
                                f"--exclude={submodule}/**/{exclude}",
                                # Exclude folders named as in SUBMODULE_DEFAULT_EXCLUDES
                                f"--exclude={submodule}/{exclude}/**",
                                f"--exclude={submodule}/**/{exclude}/**",
                            ]
                            for exclude in SUBMODULE_DEFAULT_EXCLUDES
                        ],
                        [f"--include={submodule}/**"],
                    )
                    for submodule in submodule_list
                )
            )
        ),
        "--exclude=*",
        "--prune-empty-dirs",
        f"{root}/",
        f"{dev_machine_escaped}:{dev_machine_mirror_path}",
    ]

    if debug_mode:
        print("Going to run rsync with following args:")
        for arg in args:
            print(arg)

    print("\nRunning rsync")
    start = datetime.datetime.now()
    subprocess.run(args)
    end = datetime.datetime.now()
    print(f"\nSync finished: {end - start}")


if __name__ == "__main__":
    main()
