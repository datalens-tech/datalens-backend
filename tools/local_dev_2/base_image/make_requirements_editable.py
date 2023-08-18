from pathlib import Path

from clize import run


def main(src: Path, dst: Path):
    with open(dst, "w") as fh:
        for line in open(src).readlines():
            if " @ file:///" in line:
                target = line.split(" @ file://")[1]
                fh.write(f"-e {target}")


if __name__ == "__main__":
    run(main)
