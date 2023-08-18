import datetime
import itertools
import os
import subprocess

from pycharm.common import SUBMODULE_DEFAULT_EXCLUDES, get_submodules


def main():
    dev_machine = os.environ["DEV_MACHINE"]
    dev_machine_mirror_path = os.environ["DEV_MACHINE_PKG_ROOT"]
    debug_mode_str = os.environ.get("DEV_MACHINE_UPLOAD_DEBUG", "0")
    debug_mode = bool(int(debug_mode_str))

    assert dev_machine, "Dev machine address is not specified"

    submodule_list = get_submodules() + [
        "ops/ci",
        "terrarium"
    ]

    print(f"\n\n--- Uploading project to {dev_machine=} ---\n")

    dev_machine_escaped = dev_machine
    if ':' in dev_machine_escaped:
        dev_machine_escaped = f'[{dev_machine_escaped}]'

    root = os.path.abspath(os.path.join(__file__, "../../../.."))
    args = [
        "rsync",
        "-rIc",
        *(["-v"] if debug_mode else []),
        "--include=*/",
        *list(itertools.chain(
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
        )),
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


if __name__ == '__main__':
    main()
