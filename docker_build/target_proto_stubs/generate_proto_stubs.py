import argparse
import glob
import itertools
import os.path
import subprocess


def main():
    parser = argparse.ArgumentParser(description="Build stubs for proto specs")
    parser.add_argument("targets", type=str, help="Whitespace split targets. Globbing is supported.")
    parser.add_argument("--proto_path", type=str, required=True)
    parser.add_argument("--stubs_dir", type=str, required=True)
    args = parser.parse_args()

    expanded_targets = itertools.chain(
        *[glob.glob(os.path.join(args.proto_path, target), recursive=True) for target in args.targets.split()]
    )

    protoc_arg_list = [
        "python3",
        "-m",
        "grpc_tools.protoc",
        f"--proto_path={args.proto_path}",
        f"--python_out={args.stubs_dir}",
        f"--grpc_python_out={args.stubs_dir}",
        *expanded_targets,
    ]

    print("Going to execute command to build GRPC stubs: ")
    for single_arg in protoc_arg_list:
        print(single_arg)

    print("\nExecuting...")
    subprocess.run(protoc_arg_list, check=True)

    # Creating `__init__.py` in each directory
    for root, _, _ in os.walk(args.stubs_dir):
        if os.path.realpath(root) == os.path.realpath(args.stubs_dir):
            continue
        with open(os.path.join(root, "__init__.py"), mode="a"):
            pass


if __name__ == "__main__":
    main()
